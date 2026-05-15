# 📨 Telegram Auto-Forwarder Bot

A powerful, multi-user Telegram bot that automatically forwards messages from source groups to destination groups — including protected/restricted channels — using a **Hybrid Forwarding Engine**.

---

## ✨ Features

- **Multi-user support** — multiple users can run independent tasks simultaneously, each with their own session
- **Hybrid Forwarding Engine** — auto-detects if native forwarding is allowed; falls back to Copy/Send for protected groups
- **Smart file handling** — memory upload for small files (<10MB), streamed disk upload for large files with real-time progress
- **Custom thumbnails** — set a custom thumbnail for videos; falls back to the original video thumbnail automatically
- **Caption control** — keep original captions, remove them, or apply a custom Markdown template with dynamic variables
- **FFmpeg integration** — processes videos with `-movflags +faststart` for instant Telegram streaming (optional)
- **Parallel pipeline** — producer/consumer architecture that fetches the next message while uploading the current one
- **Back navigation** — wizard-style setup with `◀️ Back` buttons on every step
- **FloodWait handling** — automatically waits and retries when Telegram rate-limits
- **Auto temp cleanup** — background task deletes downloaded files older than 30 minutes

---

## 🚀 Quick Start

### 1. Prerequisites

- Python 3.10+
- A Telegram account (used as the userbot)
- Telegram API credentials from [my.telegram.org](https://my.telegram.org)
- A Bot Token from [@BotFather](https://t.me/BotFather)

### 2. Install dependencies

```bash
pip install -r requirements.txt
```

### 3. Configure `.env`

Create a `.env` file in the project root:

```env
BOT_TOKEN=your_bot_token_here
API_ID=your_api_id_here
API_HASH=your_api_hash_here
```

### 4. (Optional) Install FFmpeg for streaming videos

Download from [ffmpeg.org](https://ffmpeg.org/download.html) and add to your system `PATH`. Without FFmpeg, videos still upload correctly but may require full download before playback.

### 5. Run

```bash
python main.py
```

---

## 🤖 Bot Commands

| Command | Description |
|---------|-------------|
| `/start` | Login and configure a forwarding task |
| `/status` | Check current task and login status |
| `/stop` | Stop the active forwarding task (stay logged in) |
| `/logout` | Stop task and permanently delete your session |

---

## 📋 Setup Wizard Flow

```
/start
  ├─ Phone Number
  ├─ OTP (with spaces: 1 2 3 4 5)
  ├─ 2FA Password (if enabled)
  ├─ Starting Message Link  ◀️ Back supported from here
  ├─ Ending Message Link
  ├─ Destination Group ID
  ├─ Custom Thumbnail? [Yes / No]
  ├─ (Upload photo if Yes)
  └─ Caption Mode [Keep / Remove / Custom]
       └─ (Type template if Custom)
            → Task Starts!
```

---

## 🎨 Caption Template Variables

When using **Custom Caption**, you can use these variables:

| Variable | Description |
|----------|-------------|
| `{filename}` | Original file name (e.g. `lecture.mp4`) |
| `{size}` | Human-readable size (e.g. `367.05 MB`) |
| `{date}` | Upload date (e.g. `2026-05-16 02:00:00`) |
| `{original_caption}` | The original message text |

**Example template:**
```
{original_caption}

📁 File: {filename}
📦 Size: {size}
🕒 Date: {date}
```

---

## 📁 Project Structure

```
msg forward tele bot/
├── main.py            # Main bot logic
├── requirements.txt   # Python dependencies
├── .env               # Secrets (never commit this)
├── .gitignore
└── temp_media/        # Temporary download folder (auto-cleaned)
```

> ⚠️ **Security Note:** `.env` and `*.session` files are excluded from git. Never share them — they contain your account credentials.

---

## 📦 Dependencies

- [`telethon`](https://github.com/LonamiWebs/Telethon) — MTProto client
- [`python-dotenv`](https://github.com/theskumar/python-dotenv) — env var loader
- `ffmpeg` (system binary, optional) — video fast-start processing
