@echo off
title Discord Tools Panel
cd /d "%~dp0"

echo Checking Python...
python --version >nul 2>&1
if errorlevel 1 (
    echo Python not found. Install from https://python.org
    pause & exit /b 1
)

echo Installing dependencies...
pip install -r requirements.txt --quiet

echo.
echo Starting Discord Tools Panel...
echo Open http://localhost:5000 in your browser.
echo Press Ctrl+C to stop.
echo.

python app.py
pause
