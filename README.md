# media-vacuum

A local web panel for Discord media tools — webhook uploader, channel downloader, and selfbot file sender. Runs entirely on your machine at `http://localhost:5000`.

---

## Features

### Webhook Uploader
- Upload images, GIFs, and videos from a local folder to any Discord webhook
- Supports batch uploads with automatic rate-limit handling
- Configurable file size limit and media type filters

### Channel Downloader
- Download all media attachments from a Discord channel using a user token
- Optionally organizes files into separate `images/`, `gifs/`, `videos/` subfolders
- Resumes automatically — skips already-downloaded files

### Selfbot Uploader
- Watches a local folder and sends new files to a Discord channel on a timer
- Queues existing files on startup
- Configurable delay between sends

### Quick Drop
- Drag-and-drop files directly in the browser UI to instantly send via webhook

### Token & Webhook Manager
- Save and manage multiple tokens and webhooks in the UI
- Tokens and webhook URLs are masked in the interface

---

## Setup

**Requirements:** Python 3.9+

### Windows (easy)
Double-click `start.bat` — it installs dependencies and launches the panel automatically.

### Manual
```bash
pip install -r requirements.txt
python app.py
```

Then open [http://localhost:5000](http://localhost:5000) in your browser.

---

## Configuration

On first run, a `config.json` is created automatically to store your saved tokens, webhooks, and selfbot settings. This file is gitignored — your credentials never leave your machine.

Copy `config.example.json` as a reference for the structure:
```json
{
  "webhooks": [],
  "tokens":   [],
  "selfbot":  {}
}
```

---

## Folder Structure

| Folder | Purpose |
|---|---|
| `1_uploader/INBOX/` | Source folder for the webhook uploader |
| `2_downloader/` | Output folder for the channel downloader |
| `3_self_uploader/media/` | Watch folder for the selfbot uploader |

All folders are created automatically on startup if they don't exist.

---

## Notes

- The channel downloader and selfbot require a **user token** (not a bot token)
- Uses `discord.py-self` for selfbot functionality — do **not** install the official `discord.py` alongside it
- All jobs run in the background and stream live logs to the UI
