"""Discord Tools Panel"""

import asyncio
import aiohttp
import collections
import json
import logging
import os
import queue
import threading
import time
import uuid
from pathlib import Path

import discord
import requests
from discord.ext import commands
from flask import Flask, jsonify, render_template, request, Response, stream_with_context
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

app = Flask(__name__)
BASE_DIR = Path(__file__).parent

# ── Suppress poll routes from werkzeug log ────────────────────────────────────
class _F(logging.Filter):
    def filter(self, r): return "/api/jobs" not in r.getMessage() and "/api/logs" not in r.getMessage()
logging.getLogger("werkzeug").addFilter(_F())

# ── Log system ────────────────────────────────────────────────────────────────
_hist: collections.deque = collections.deque(maxlen=2000)
_listeners: list = []
_llock = threading.Lock()

def log(job_id: str, source: str, msg: str):
    e = {"job_id": job_id, "source": source, "msg": msg, "t": time.strftime("%H:%M:%S")}
    with _llock:
        _hist.append(e)
        for q in list(_listeners):
            try: q.put_nowait(e)
            except queue.Full: pass

# ── Paths ─────────────────────────────────────────────────────────────────────
def resolve(s: str) -> Path:
    p = Path(s)
    return p if p.is_absolute() else BASE_DIR / p

def ensure(s: str) -> Path:
    p = resolve(s)
    p.mkdir(parents=True, exist_ok=True)
    return p

def safe_name(s: str) -> str:
    for c in r'\/:*?"<>|': s = s.replace(c, "_")
    return s.strip(". ") or "folder"

# ── Config ────────────────────────────────────────────────────────────────────
_CFG = BASE_DIR / "config.json"

def load_cfg() -> dict:
    if _CFG.exists():
        with open(_CFG, encoding="utf-8") as f: return json.load(f)
    return {}

def save_cfg(d: dict):
    with open(_CFG, "w", encoding="utf-8") as f: json.dump(d, f, indent=2)

# ── Media types ───────────────────────────────────────────────────────────────
EXTS = {
    "image": {".jpg", ".jpeg", ".png", ".webp", ".bmp"},
    "gif":   {".gif"},
    "video": {".mp4", ".mov", ".mkv", ".webm", ".avi", ".m4v"},
}
ALL_EXTS = set().union(*EXTS.values())

def t2e(types) -> set:
    out = set()
    for t in (types or list(EXTS)): out |= EXTS.get(t, set())
    return out

# ── Job ───────────────────────────────────────────────────────────────────────
class Job:
    def __init__(self, jtype: str, label: str, cfg: dict):
        self.id       = uuid.uuid4().hex[:8]
        self.type     = jtype
        self.label    = label
        self.cfg      = cfg
        self.stop     = threading.Event()
        self.thread   = None
        self.status   = "running"
        self.progress = ""
        self.error    = ""

    def alive(self) -> bool:
        return self.thread is not None and self.thread.is_alive()

    def d(self) -> dict:
        return {"id": self.id, "type": self.type, "label": self.label,
                "status": self.status, "alive": self.alive(),
                "progress": self.progress, "error": self.error}

_jobs: dict = {}
_jlock = threading.Lock()

# ════════════════════════════════════════════════════════════════════════════
#  MODULE 1 — Webhook Uploader
# ════════════════════════════════════════════════════════════════════════════

def _wh_send(url: str, path: str, job: Job) -> bool:
    for _ in range(5):
        try:
            with open(path, "rb") as f:
                r = requests.post(url, files={"file": (os.path.basename(path), f)}, timeout=120)
        except Exception as e:
            log(job.id, "uploader", f"Request error: {e}"); return False
        if r.status_code in (200, 204): return True
        if r.status_code == 429:
            w = 2.0
            try: w = float(r.json().get("retry_after", 2))
            except Exception: pass
            log(job.id, "uploader", f"Rate limited — {w:.1f}s")
            time.sleep(w); continue
        log(job.id, "uploader", f"HTTP {r.status_code}"); return False
    return False

def run_uploader(job: Job):
    c = job.cfg
    wh    = c.get("webhook_url", "").strip()
    src   = c.get("source_folder", "").strip()
    types = c.get("media_types", list(EXTS))
    maxmb = float(c.get("max_file_mb", 25))

    if not wh:
        log(job.id, "uploader", "No webhook URL."); job.status = "error"; return
    if not src:
        log(job.id, "uploader", "No source folder."); job.status = "error"; return

    folder = resolve(src)
    if not folder.exists():
        log(job.id, "uploader", f"Folder not found: {folder}"); job.status = "error"; return

    exts  = t2e(types)
    files = sorted(f for f in folder.iterdir() if f.is_file() and f.suffix.lower() in exts)
    total = len(files)
    log(job.id, "uploader", f"{total} file(s) queued  →  {wh[-40:]}")

    sent = failed = skipped = 0
    for i, fp in enumerate(files, 1):
        if job.stop.is_set(): log(job.id, "uploader", "Stopped."); break
        mb = fp.stat().st_size / 1e6
        job.progress = f"{i}/{total}  sent:{sent}"
        if mb > maxmb:
            log(job.id, "uploader", f"[skip] {fp.name}  {mb:.1f} MB"); skipped += 1; continue
        if _wh_send(wh, str(fp), job):
            sent += 1
            log(job.id, "uploader", f"[✓] {fp.name}  {mb:.1f} MB  [{i}/{total}]")
        else:
            failed += 1
            log(job.id, "uploader", f"[✗] {fp.name}")

    log(job.id, "uploader", f"Done — sent:{sent}  failed:{failed}  skipped:{skipped}")
    if not job.stop.is_set():
        job.status   = "done"
        job.progress = f"done  {sent}/{total}"

# ════════════════════════════════════════════════════════════════════════════
#  MODULE 2 — Channel Downloader
# ════════════════════════════════════════════════════════════════════════════

def run_downloader(job: Job):
    c    = job.cfg
    tok  = c.get("token", "").strip()
    chid = str(c.get("channel_id", "")).strip()
    save = c.get("save_folder", "2_downloader").strip()
    sep  = bool(c.get("separate_folders", False))
    types = c.get("media_types", list(EXTS))

    if not tok:  log(job.id, "downloader", "No token.");      job.status = "error"; return
    if not chid: log(job.id, "downloader", "No channel ID."); job.status = "error"; return

    exts = t2e(types)

    async def _run():
        hdrs = {"Authorization": tok,
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}

        ch_name = chid
        try:
            async with aiohttp.ClientSession(headers=hdrs) as s:
                async with s.get(f"https://discord.com/api/v10/channels/{chid}",
                                  timeout=aiohttp.ClientTimeout(total=10)) as r:
                    if r.status == 200:
                        ch_name = (await r.json()).get("name", chid)
        except Exception: pass

        base = ensure(save) / safe_name(ch_name)
        base.mkdir(parents=True, exist_ok=True)
        dirs = {}
        if sep:
            for k, name in [("image","images"),("gif","gifs"),("video","videos")]:
                dirs[k] = base / name
                dirs[k].mkdir(exist_ok=True)
        log(job.id, "downloader", f"#{ch_name}  →  {base}")

        saved = scanned = 0
        before = None
        conn = aiohttp.TCPConnector(limit=10)
        to   = aiohttp.ClientTimeout(total=None, connect=30, sock_read=60)

        async with aiohttp.ClientSession(headers=hdrs, connector=conn, timeout=to) as ses:
            while not job.stop.is_set():
                url = f"https://discord.com/api/v10/channels/{chid}/messages?limit=100"
                if before: url += f"&before={before}"
                try:
                    async with ses.get(url) as r:
                        if r.status == 429:
                            w = float((await r.json()).get("retry_after", 5))
                            log(job.id, "downloader", f"Rate limit — {w:.0f}s")
                            await asyncio.sleep(w); continue
                        if r.status == 403:
                            log(job.id, "downloader", "403 — no access to this channel."); break
                        if r.status != 200:
                            log(job.id, "downloader", f"HTTP {r.status}"); break
                        msgs = await r.json()
                except Exception as e:
                    log(job.id, "downloader", f"Error: {e}"); break

                if not msgs: break

                for msg in msgs:
                    if job.stop.is_set(): break
                    scanned += 1
                    for att in msg.get("attachments", []):
                        aurl = att.get("url") or att.get("proxy_url")
                        fn   = att.get("filename", "file")
                        ext  = Path(fn).suffix.lower()
                        if ext not in exts or not aurl: continue
                        if sep:
                            if   ext in EXTS["gif"]:   d = dirs["gif"]
                            elif ext in EXTS["video"]: d = dirs["video"]
                            else:                       d = dirs["image"]
                        else:
                            d = base
                        dest = d / f"{msg['id']}_{fn}"
                        if dest.exists(): continue
                        try:
                            async with ses.get(aurl) as fr:
                                if fr.status == 200:
                                    dest.write_bytes(await fr.read())
                                    saved += 1
                                    if saved % 50 == 0:
                                        log(job.id, "downloader", f"[{saved}] {fn}")
                        except Exception: pass

                job.progress = f"{scanned:,} msgs · {saved:,} saved"
                before = msgs[-1]["id"]
                if len(msgs) < 100: break

        log(job.id, "downloader", f"Done — {scanned:,} msgs, {saved:,} files")
        if not job.stop.is_set():
            job.status   = "done"
            job.progress = f"done  {saved:,} files"

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try: loop.run_until_complete(_run())
    except Exception as e:
        job.status = "error"; job.error = str(e)
        log(job.id, "downloader", f"Fatal: {e}")
    finally: loop.close()

# ════════════════════════════════════════════════════════════════════════════
#  MODULE 3 — Selfbot Uploader (timer-based, single job)
# ════════════════════════════════════════════════════════════════════════════

def run_selfbot(job: Job):
    c    = job.cfg
    tok  = c.get("token", "").strip()
    chid = str(c.get("channel_id", "")).strip()
    dmin = float(c.get("delay_minutes", 1))
    maxmb = float(c.get("max_file_mb", 25))
    wdir  = ensure(c.get("watch_folder", "3_self_uploader/media"))

    if not tok or not chid:
        log(job.id, "selfbot", "Missing token or channel ID."); job.status = "error"; return

    ch_id = int(chid)
    dsec  = dmin * 60

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    bot = commands.Bot(command_prefix="!", self_bot=True, help_command=None)
    q: asyncio.Queue = asyncio.Queue()

    async def _stopper():
        while not job.stop.is_set(): await asyncio.sleep(0.5)
        await bot.close()

    async def _worker():
        ch = None
        while True:
            try: path = await asyncio.wait_for(q.get(), 1.0)
            except asyncio.TimeoutError:
                if job.stop.is_set(): break
                continue
            if ch is None:
                try:
                    ch = await bot.fetch_channel(ch_id)
                    log(job.id, "selfbot", f"Channel: #{getattr(ch,'name',chid)}")
                except Exception as e:
                    log(job.id, "selfbot", f"Cannot load channel: {e}")
                    q.task_done(); continue
            if not path.exists(): q.task_done(); continue
            mb = path.stat().st_size / 1e6
            if mb > maxmb:
                log(job.id, "selfbot", f"[skip] {path.name}  {mb:.1f} MB")
                q.task_done(); continue
            try:
                await ch.send(file=discord.File(str(path), path.name))
                log(job.id, "selfbot", f"[sent] {path.name}  —  next in {dmin:.0f}m")
                job.progress = f"last: {path.name}"
                w = 0.0
                while w < dsec and not job.stop.is_set():
                    await asyncio.sleep(0.5); w += 0.5
            except Exception as e:
                log(job.id, "selfbot", f"[error] {path.name}: {e}")
                await asyncio.sleep(5)
            q.task_done()

    class _W(FileSystemEventHandler):
        def on_created(self, ev):
            if ev.is_directory: return
            p = Path(ev.src_path)
            if p.suffix.lower() not in ALL_EXTS: return
            time.sleep(2)
            log(job.id, "selfbot", f"Queued: {p.name}")
            asyncio.run_coroutine_threadsafe(q.put(p), loop)

    @bot.event
    async def on_ready():
        log(job.id, "selfbot", f"Logged in: {bot.user}")
        # Queue existing files now that the loop is running
        existing = sorted(f for f in wdir.iterdir() if f.is_file() and f.suffix.lower() in ALL_EXTS)
        for f in existing:
            await q.put(f)
        if existing:
            log(job.id, "selfbot", f"Queued {len(existing)} existing file(s)")
        asyncio.create_task(_stopper())
        asyncio.create_task(_worker())

    obs = Observer()
    obs.schedule(_W(), str(wdir), recursive=False)
    obs.start()
    try:
        loop.run_until_complete(bot.start(tok))
    except Exception as e:
        job.status = "error"; job.error = str(e)
        log(job.id, "selfbot", f"Error: {e}")
    finally:
        obs.stop(); obs.join(); loop.close()
        log(job.id, "selfbot", "Selfbot stopped.")

# ── Runners + launch ──────────────────────────────────────────────────────────
_RUNNERS = {"uploader": run_uploader, "downloader": run_downloader, "selfbot": run_selfbot}

def _run_job(job: Job):
    try: _RUNNERS[job.type](job)
    except Exception as e:
        job.status = "error"; job.error = str(e)
        log(job.id, "system", f"Crash: {e}")
    finally:
        if job.status == "running": job.status = "done"

def launch(job: Job):
    job.thread = threading.Thread(target=_run_job, args=(job,), daemon=True, name=f"j-{job.id}")
    job.thread.start()

# ── Flask routes ──────────────────────────────────────────────────────────────

@app.route("/")
def index(): return render_template("index.html")

@app.route("/api/jobs")
def api_jobs():
    with _jlock: return jsonify([j.d() for j in _jobs.values()])

@app.route("/api/jobs", methods=["POST"])
def api_new_job():
    d = request.get_json(force=True) or {}
    jtype = d.get("type", "")
    if jtype not in _RUNNERS: return jsonify({"error": "Unknown type"}), 400
    if jtype == "selfbot":
        with _jlock:
            for j in _jobs.values():
                if j.type == "selfbot" and j.alive():
                    return jsonify({"error": "Selfbot already running"}), 400
    job = Job(jtype, d.get("label", jtype), d.get("cfg", {}))
    with _jlock: _jobs[job.id] = job
    launch(job)
    log(job.id, "system", f"Started: {job.label}")
    return jsonify({"ok": True, "id": job.id})

@app.route("/api/jobs/<jid>/stop", methods=["POST"])
def api_stop(jid):
    with _jlock: job = _jobs.get(jid)
    if not job: return jsonify({"error": "Not found"}), 404
    job.stop.set(); job.status = "stopped"
    log(jid, "system", f"Stopped: {job.label}")
    return jsonify({"ok": True})

@app.route("/api/jobs/<jid>", methods=["DELETE"])
def api_del_job(jid):
    with _jlock:
        job = _jobs.get(jid)
        if not job: return jsonify({"error": "Not found"}), 404
        if job.alive(): return jsonify({"error": "Still running — stop first"}), 400
        del _jobs[jid]
    return jsonify({"ok": True})

@app.route("/api/logs")
def api_logs():
    q2: queue.Queue = queue.Queue(maxsize=2000)
    with _llock:
        for e in list(_hist):
            try: q2.put_nowait(e)
            except queue.Full: break
        _listeners.append(q2)
    def gen():
        try:
            while True:
                try: yield f"data: {json.dumps(q2.get(timeout=15))}\n\n"
                except queue.Empty: yield 'data: {"ping":true}\n\n'
        finally:
            with _llock:
                if q2 in _listeners: _listeners.remove(q2)
    return Response(stream_with_context(gen()), mimetype="text/event-stream",
                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})

# Webhooks
def _murl(u):
    p = u.split("/"); return "/".join(p[:6]) + "/***" if len(p) >= 7 else "***"

@app.route("/api/webhooks")
def api_whs():
    raw = load_cfg().get("webhooks", [])
    return jsonify([{"idx": i, "label": w.get("label", f"WH {i+1}"),
                     "url": w.get("url", ""), "masked": _murl(w.get("url", ""))}
                    for i, w in enumerate(raw)])

@app.route("/api/webhooks", methods=["POST"])
def api_add_wh():
    d = request.get_json(force=True) or {}
    u = (d.get("url") or "").strip()
    if not u.startswith("https://discord.com/api/webhooks/"):
        return jsonify({"error": "Invalid webhook URL"}), 400
    cfg = load_cfg(); whs = cfg.setdefault("webhooks", [])
    whs.append({"label": (d.get("label") or f"WH #{len(whs)+1}").strip(), "url": u})
    save_cfg(cfg); return jsonify({"ok": True})

@app.route("/api/webhooks/<int:i>", methods=["DELETE"])
def api_del_wh(i):
    cfg = load_cfg(); whs = cfg.get("webhooks", [])
    if not (0 <= i < len(whs)): return jsonify({"error": "Not found"}), 404
    whs.pop(i); save_cfg(cfg); return jsonify({"ok": True})

# Tokens
def _mtok(t): return (t[:6] + "…" + t[-4:]) if len(t) > 12 else "***"

@app.route("/api/tokens")
def api_toks():
    raw = load_cfg().get("tokens", [])
    return jsonify([{"idx": i, "label": t.get("label", f"Tok {i+1}"),
                     "token": t.get("token", ""), "masked": _mtok(t.get("token", ""))}
                    for i, t in enumerate(raw)])

@app.route("/api/tokens", methods=["POST"])
def api_add_tok():
    d = request.get_json(force=True) or {}
    t = (d.get("token") or "").strip()
    if not t: return jsonify({"error": "Token required"}), 400
    cfg = load_cfg(); toks = cfg.setdefault("tokens", [])
    toks.append({"label": (d.get("label") or f"Tok #{len(toks)+1}").strip(), "token": t})
    save_cfg(cfg); return jsonify({"ok": True})

@app.route("/api/tokens/<int:i>", methods=["DELETE"])
def api_del_tok(i):
    cfg = load_cfg(); toks = cfg.get("tokens", [])
    if not (0 <= i < len(toks)): return jsonify({"error": "Not found"}), 404
    toks.pop(i); save_cfg(cfg); return jsonify({"ok": True})

# Selfbot saved config
@app.route("/api/selfbot")
def api_sb(): return jsonify(load_cfg().get("selfbot", {}))

@app.route("/api/selfbot", methods=["POST"])
def api_sb_set():
    cfg = load_cfg()
    cfg["selfbot"] = request.get_json(force=True) or {}
    save_cfg(cfg); return jsonify({"ok": True})

# Quick drop upload
@app.route("/api/drop-upload", methods=["POST"])
def api_drop_upload():
    wh = request.form.get("webhook_url", "").strip()
    if not wh.startswith("https://discord.com/api/webhooks/"):
        return jsonify({"error": "Invalid webhook URL"}), 400
    results = []
    for f in request.files.getlist("files"):
        name = f.filename or "file"
        data = f.read()
        sent = False
        err  = ""
        for _ in range(5):
            try:
                r = requests.post(wh, files={"file": (name, data)}, timeout=120)
                if r.status_code in (200, 204):
                    sent = True; break
                if r.status_code == 429:
                    w = 2.0
                    try: w = float(r.json().get("retry_after", 2))
                    except Exception: pass
                    time.sleep(w); continue
                err = f"HTTP {r.status_code}"; break
            except Exception as e:
                err = str(e); break
        results.append({"name": name, "ok": sent, "error": err})
    return jsonify({"results": results})


# Selfbot drop — save files to watch folder so the running bot picks them up
@app.route("/api/selfbot/drop", methods=["POST"])
def api_selfbot_drop():
    folder = request.form.get("folder", "").strip()
    if not folder:
        folder = load_cfg().get("selfbot", {}).get("watch_folder", "") or "3_self_uploader/media"
    dest = ensure(folder)
    saved = []
    for f in request.files.getlist("files"):
        name = f.filename or "file"
        target = dest / name
        if target.exists():
            stem, ext = os.path.splitext(name)
            i = 1
            while (dest / f"{stem}_{i}{ext}").exists(): i += 1
            target = dest / f"{stem}_{i}{ext}"
        f.save(str(target))
        saved.append(target.name)
    return jsonify({"ok": True, "saved": saved, "folder": str(dest)})


# Folder browser
@app.route("/api/browse")
def api_browse():
    rel = request.args.get("path", "").strip().replace("\\", "/")
    base = BASE_DIR.resolve()
    try:
        target = (base / rel).resolve()
        target.relative_to(base)  # safety — no traversal outside panel dir
    except Exception:
        target = base
    folders = []
    try:
        for item in sorted(target.iterdir()):
            if item.is_dir() and not item.name.startswith(".") and not item.name.startswith("__"):
                folders.append(item.name)
    except Exception:
        pass
    try:
        rel_path = str(target.relative_to(base)).replace("\\", "/")
        if rel_path == ".": rel_path = ""
    except Exception:
        rel_path = ""
    return jsonify({"path": rel_path, "folders": folders, "can_up": bool(rel_path)})

@app.route("/api/browse/mkdir", methods=["POST"])
def api_browse_mkdir():
    d = request.get_json(force=True) or {}
    rel  = d.get("path", "").strip()
    name = d.get("name", "").strip()
    if not name or any(c in name for c in r'\/:*?"<>|'):
        return jsonify({"error": "Invalid name"}), 400
    try:
        target = (BASE_DIR.resolve() / rel / name).resolve()
        target.relative_to(BASE_DIR.resolve())
        target.mkdir(parents=True, exist_ok=True)
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 400


if __name__ == "__main__":
    for _d in ("1_uploader/INBOX", "2_downloader", "3_self_uploader/media"):
        ensure(_d)
    log("system", "system", "Discord Tools Panel → http://localhost:5000")
    app.run(host="0.0.0.0", port=5000, debug=False, threaded=True)
