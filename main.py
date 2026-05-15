import os
import io
import asyncio
import logging
import time
import re
import shutil
import subprocess
from dotenv import load_dotenv
from telethon import TelegramClient, events, errors, Button
from telethon.tl.types import MessageMediaWebPage, MessageMediaUnsupported, MessageEmpty
from telethon import functions, types

# Configure logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger("TelegramForwarder")

# Constants
BOT_SESSION = "bot_controller.session"
TEMP_DIR = "temp_media"
FFMPEG_AVAILABLE = bool(shutil.which("ffmpeg"))

if FFMPEG_AVAILABLE:
    logger.info("FFmpeg detected. Streamable video processing is ENABLED.")
else:
    logger.warning(
        "FFmpeg not found in PATH. Videos will be uploaded without fast-start processing. "
        "Install FFmpeg (https://ffmpeg.org/download.html) and add it to PATH to enable streaming."
    )

# State Machine for the Bot Conversation
user_states = {}

# Active tasks and clients
active_userbots = {}
forwarding_tasks = {}

def get_user_session_name(user_id):
    return f"user_session_{user_id}.session"

def parse_telegram_link(link):
    match = re.search(r't\.me/c/(\d+)/(\d+)', link)
    if match:
        chat_id = int("-100" + match.group(1))
        msg_id = int(match.group(2))
        return chat_id, msg_id
    
    match2 = re.search(r't\.me/([^/]+)/(\d+)', link)
    if match2:
        chat_id = match2.group(1)
        msg_id = int(match2.group(2))
        if chat_id.isdigit():
            chat_id = int(chat_id)
        return chat_id, msg_id
    return None, None

def format_size(size_bytes):
    if not size_bytes:
        return "Unknown"
    for unit in ['B', 'KB', 'MB', 'GB']:
        if size_bytes < 1024.0:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.2f} TB"

def format_caption(template, message):
    if not template or template == 'keep':
        return message.text
    if template == 'remove':
        return ""
        
    filename = "Unknown"
    size = "Unknown"
    
    if message.file:
        filename = message.file.name or "Unknown"
        size = format_size(message.file.size)
        
    date_str = message.date.strftime("%Y-%m-%d %H:%M:%S") if message.date else "Unknown"
    original_caption = message.text or ""
    
    formatted = template.replace("{filename}", filename)
    formatted = formatted.replace("{size}", size)
    formatted = formatted.replace("{date}", date_str)
    formatted = formatted.replace("{original_caption}", original_caption)
    
    return formatted

def is_video_file(path):
    """Check if a file path points to a video format."""
    video_exts = {'.mp4', '.mkv', '.avi', '.mov', '.webm', '.flv', '.m4v', '.ts'}
    return os.path.splitext(path)[1].lower() in video_exts

def is_image_file(path):
    """Check if a file path points to an image format."""
    image_exts = {'.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp'}
    return os.path.splitext(path)[1].lower() in image_exts

MIME_TO_EXT = {
    'image/jpeg': '.jpg', 'image/png': '.png', 'image/gif': '.gif',
    'image/webp': '.webp', 'image/bmp': '.bmp',
    'video/mp4': '.mp4', 'video/x-matroska': '.mkv', 'video/avi': '.avi',
    'video/quicktime': '.mov', 'video/webm': '.webm',
    'audio/mpeg': '.mp3', 'audio/mp4': '.m4a', 'audio/ogg': '.ogg',
    'application/pdf': '.pdf', 'text/plain': '.txt',
    'application/zip': '.zip', 'application/x-rar-compressed': '.rar',
}

def get_proper_filename(message):
    """Return a proper filename, falling back to MIME-inferred extension instead of .bin."""
    if message.file and message.file.name:
        return message.file.name
    mime = message.file.mime_type if message.file else None
    ext = MIME_TO_EXT.get(mime, '.bin') if mime else '.bin'
    return f"file_{message.id}{ext}"

async def get_original_thumb(client, message):
    """
    Extract the original thumbnail from a Telegram message (for videos).
    Returns a BytesIO object or None.
    """
    try:
        from telethon.tl.types import MessageMediaDocument, DocumentAttributeVideo
        if not isinstance(message.media, MessageMediaDocument):
            return None
        doc = message.media.document
        # Check if it's a video
        is_video = any(isinstance(a, DocumentAttributeVideo) for a in doc.attributes)
        if not is_video:
            return None
        # Download thumbnail into memory
        thumb_bytes = await client.download_media(message, file=bytes, thumb=-1)
        if thumb_bytes:
            bio = io.BytesIO(thumb_bytes)
            bio.name = 'thumb.jpg'
            return bio
    except Exception as e:
        logger.warning(f"Could not extract original thumbnail: {e}")
    return None

def process_video_for_streaming(input_path):
    """
    Run FFmpeg to:
    1. Move the moov atom to the beginning (fast-start) so Telegram can stream it.
    2. Edit metadata: clear old title/artist/author, optionally set new ones.
    Returns the output path or None if FFmpeg failed/unavailable.
    """
    if not FFMPEG_AVAILABLE:
        return None
    
    base, ext = os.path.splitext(input_path)
    output_path = base + "_streamable" + (ext if ext else ".mp4")
    
    cmd = [
        "ffmpeg", "-y",
        "-i", input_path,
        "-c", "copy",                    # Re-mux only — no re-encoding (fast!)
        "-movflags", "+faststart",        # Move moov atom to front (streamable)
        "-metadata", "title=",           # Clear title
        "-metadata", "artist=",          # Clear artist
        "-metadata", "author=",          # Clear author
        "-metadata", "comment=",         # Clear comment
        output_path
    ]
    
    try:
        result = subprocess.run(
            cmd,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            timeout=120,
        )
        if result.returncode == 0 and os.path.exists(output_path):
            logger.info(f"FFmpeg processing complete: {output_path}")
            return output_path
        else:
            err = result.stderr.decode('utf-8', errors='ignore')[-500:]
            logger.error(f"FFmpeg processing failed: {err}")
            return None
    except subprocess.TimeoutExpired:
        logger.error("FFmpeg timed out.")
        return None
    except Exception as e:
        logger.error(f"FFmpeg exception: {e}")
        return None

async def bot_respond(event, text):
    """
    Safe respond helper — always sends a NEW message, never tries to edit.
    This avoids MessageIdInvalidError when event is a user text message.
    """
    await event.respond(text)

async def safe_bot_edit(bot_client, chat_id, msg_id, text):
    """
    Safe edit wrapper — catches common edit errors (e.g., message not modified).
    """
    try:
        await bot_client.edit_message(chat_id, msg_id, text)
    except errors.rpcerrorlist.MessageNotModifiedError:
        pass
    except Exception as e:
        logger.warning(f"Could not edit progress message: {e}")

async def forward_message(client, message, dest_group, thumb_path, caption_rule,
                          bot_client=None, admin_id=None, status_msg_id=None, progress_ctx=None):
    needs_copy = False

    if thumb_path or (caption_rule not in ('keep', None)):
        needs_copy = True

    if not needs_copy:
        try:
            await client.forward_messages(dest_group, message)
            logger.info(f"Message {message.id} forwarded successfully (Native).")
            return 'direct'
        except errors.rpcerrorlist.ChatForwardsRestrictedError:
            logger.warning(f"Message {message.id}: Chat protected. Falling back to copy...")
            needs_copy = True
        except Exception as e:
            logger.error(f"Error natively forwarding message {message.id}: {e}")
            return False

    if needs_copy:
        path = None
        streamable_path = None
        try:
            if message.media and not isinstance(message.media, (MessageMediaWebPage, MessageMediaUnsupported)):
                file_size = message.file.size if message.file else 0
                # Use MIME-inferred extension instead of blindly using 'file.bin'
                file_name = get_proper_filename(message)
                final_caption = format_caption(caption_rule, message)
                is_custom_cap = caption_rule and caption_rule not in ('keep', 'remove', None)
                # For 'keep' mode: use original entities if present.
                # If entities are missing but text has Markdown syntax (**bold**), auto-parse.
                if caption_rule == 'keep' or not caption_rule:
                    if message.entities:
                        final_entities = message.entities
                        final_parse_mode = None
                    elif final_caption and ('**' in (final_caption or '') or '__' in (final_caption or '')):
                        final_entities = None
                        final_parse_mode = 'md'
                    else:
                        final_entities = None
                        final_parse_mode = None
                else:
                    final_entities = None
                    final_parse_mode = 'md' if is_custom_cap else None

                # --- Download progress callback for large files ---
                _last_dl_update = [0.0]
                async def dl_progress(current, total):
                    now = time.time()
                    if now - _last_dl_update[0] > 4 and bot_client and admin_id and status_msg_id and progress_ctx:
                        _last_dl_update[0] = now
                        pct = (current / total * 100) if total else 0
                        ctx = progress_ctx.copy()
                        txt = (
                            f"⏳ **Hybrid Forwarding Engine**\n\n"
                            f"📥 Downloading Msg `{message.id}`... {pct:.1f}%\n"
                            f"   ({format_size(current)} / {format_size(total)})\n\n"
                            f"Processed: {ctx.get('processed',0)}/{ctx.get('total',0)}\n"
                            f"Successful: {ctx.get('success',0)}"
                        )
                        await safe_bot_edit(bot_client, admin_id, status_msg_id, txt)

                # Hybrid memory/disk upload
                # Determine unique download path per user + message to avoid conflicts
                uid = admin_id or 'shared'
                safe_name = re.sub(r'[^\w.]', '_', file_name)
                unique_prefix = f"{uid}_{message.id}_"

                if file_size and file_size < 10 * 1024 * 1024:  # < 10 MB: download to memory
                    logger.info(f"Message {message.id}: Small file ({format_size(file_size)}), downloading to memory...")
                    mem_bytes = await client.download_media(message, file=bytes)
                    if mem_bytes:
                        bio = io.BytesIO(mem_bytes)
                        bio.name = file_name
                        is_vid = file_name and is_video_file(file_name)
                        is_img = file_name and is_image_file(file_name)
                        # Smart thumb: custom thumb only for videos, not images
                        effective_thumb = None
                        if is_vid:
                            if thumb_path:
                                effective_thumb = thumb_path
                            else:
                                effective_thumb = await get_original_thumb(client, message)
                        # images get no thumb override
                        await client.send_file(
                            dest_group, file=bio,
                            caption=final_caption,
                            formatting_entities=final_entities,
                            parse_mode=final_parse_mode,
                            thumb=effective_thumb,
                            supports_streaming=bool(is_vid)
                        )
                    else:
                        await client.send_message(dest_group, final_caption or "", formatting_entities=final_entities)

                else:  # Large file: stream to disk with progress + unique name
                    logger.info(f"Message {message.id}: Large file ({format_size(file_size)}), streaming to disk ({uid})...")
                    os.makedirs(TEMP_DIR, exist_ok=True)
                    # Use unique filename so multiple users don't overwrite each other
                    dl_target = os.path.join(TEMP_DIR, unique_prefix + safe_name)
                    path = await client.download_media(message, file=dl_target, progress_callback=dl_progress)

                    if path:
                        upload_path = path
                        is_vid = is_video_file(path)

                        # Apply FFmpeg fast-start for video files (makes them streamable)
                        if is_vid and FFMPEG_AVAILABLE:
                            logger.info(f"Message {message.id}: Applying FFmpeg fast-start...")
                            if bot_client and admin_id and status_msg_id:
                                await safe_bot_edit(bot_client, admin_id, status_msg_id,
                                    f"⚙️ Processing video {message.id} with FFmpeg (fast-start)...")
                            streamable_path = process_video_for_streaming(path)
                            if streamable_path:
                                upload_path = streamable_path

                        # Smart thumb: custom for videos, original for videos with no custom, none for images
                        effective_thumb = None
                        if is_vid:
                            if thumb_path:
                                effective_thumb = thumb_path
                            else:
                                effective_thumb = await get_original_thumb(client, message)

                        # Upload progress callback
                        _last_ul_update = [0.0]
                        async def ul_progress(current, total):
                            now = time.time()
                            if now - _last_ul_update[0] > 4 and bot_client and admin_id and status_msg_id and progress_ctx:
                                _last_ul_update[0] = now
                                pct = (current / total * 100) if total else 0
                                ctx = progress_ctx.copy()
                                txt = (
                                    f"⏳ **Hybrid Forwarding Engine**\n\n"
                                    f"📤 Uploading Msg `{message.id}`... {pct:.1f}%\n"
                                    f"   ({format_size(current)} / {format_size(total)})\n\n"
                                    f"Processed: {ctx.get('processed',0)}/{ctx.get('total',0)}\n"
                                    f"Successful: {ctx.get('success',0)}"
                                )
                                await safe_bot_edit(bot_client, admin_id, status_msg_id, txt)

                        await client.send_file(
                            dest_group, upload_path,
                            caption=final_caption,
                            formatting_entities=final_entities,
                            parse_mode=final_parse_mode,
                            thumb=effective_thumb,
                            supports_streaming=bool(is_vid),
                            progress_callback=ul_progress
                        )
                    else:
                        await client.send_message(dest_group, final_caption or "", formatting_entities=final_entities)
            else:
                # Text-only message
                final_caption = format_caption(caption_rule, message)
                is_custom = caption_rule and caption_rule not in ('keep', 'remove', None)
                if final_caption:
                    if is_custom:
                        await client.send_message(dest_group, final_caption, parse_mode='md')
                    elif message.entities:
                        await client.send_message(dest_group, final_caption, formatting_entities=message.entities)
                    elif '**' in final_caption or '__' in final_caption:
                        # Original text has raw markdown but no entities - parse it
                        await client.send_message(dest_group, final_caption, parse_mode='md')
                    else:
                        await client.send_message(dest_group, final_caption)
                    
            logger.info(f"Message {message.id} copied/sent successfully.")
            return 'copy'

        except Exception as copy_e:
            logger.error(f"Error copying message {message.id}: {copy_e}")
            return False
        finally:
            # Guarantee cleanup of ALL temp files
            for p in [path, streamable_path]:
                if p and os.path.exists(p):
                    try:
                        os.remove(p)
                        logger.info(f"Deleted temp file: {p}")
                    except Exception as e:
                        logger.error(f"Failed to delete temp file {p}: {e}")
            
    return False

async def start_forwarding_task(user_client, chat_id, start_id, end_id, dest_group, bot_client, admin_id, thumb_path, caption_rule):
    status_msg = None
    try:
        status_msg = await bot_client.send_message(admin_id, "🚀 Preparing message forwarding task...\nResolving Chat Entity...")
        
        # Resolve entity to avoid silent fetch failures for private groups
        try:
            entity = await user_client.get_entity(chat_id)
            logger.info(f"[User {admin_id}] Successfully resolved source group entity: {entity.id}")
            await safe_bot_edit(bot_client, admin_id, status_msg.id, "✅ Chat resolved! Starting to fetch messages...")
        except Exception as e:
            err_msg = (
                f"❌ Failed to resolve source group: {e}\n"
                "Ensure the account you logged in with has joined the group."
            )
            logger.error(f"[User {admin_id}] {err_msg}")
            await safe_bot_edit(bot_client, admin_id, status_msg.id, err_msg)
            return

        total_ids = end_id - start_id + 1
        processed = 0
        skipped = 0
        success = 0
        last_update_time = 0
        current_msg_id = start_id
        progress_ctx = {'processed': 0, 'total': total_ids, 'success': 0}

        # --- Detect if forwarding is allowed ONCE by probing the first real message ---
        group_forward_allowed = None  # None = not yet detected
        logger.info(f"[User {admin_id}] Probing source group for forward restrictions...")
        for probe_id in range(start_id, end_id + 1):
            probe_msg = await user_client.get_messages(entity, ids=probe_id)
            if probe_msg and not isinstance(probe_msg, MessageEmpty):
                try:
                    await user_client.forward_messages(dest_group, probe_msg)
                    group_forward_allowed = True
                    success += 1
                    processed += 1
                    progress_ctx.update({'processed': processed, 'success': success})
                    logger.info(f"[User {admin_id}] ✅ Forwarding ALLOWED. Using native forward for all messages.")
                except errors.rpcerrorlist.ChatForwardsRestrictedError:
                    group_forward_allowed = False
                    processed += 1
                    logger.info(f"[User {admin_id}] 🔒 Forwarding RESTRICTED. Using Copy/Send for all messages.")
                except Exception as e:
                    group_forward_allowed = False
                    processed += 1
                    logger.warning(f"[User {admin_id}] Probe failed ({e}), defaulting to Copy/Send.")
                start_id = probe_id + 1  # Continue from next message
                break
            else:
                skipped += 1
                processed += 1

        if group_forward_allowed is None:
            await bot_client.send_message(admin_id, "⚠️ Could not find any messages in the given range.")
            return

        ffmpeg_status = "✅ On" if FFMPEG_AVAILABLE else "❌ Off"
        mode_label = "Native Forward" if group_forward_allowed else "Copy/Send (Protected)"
        await safe_bot_edit(bot_client, admin_id, status_msg.id,
            f"🔍 Detection complete!\nMode: **{mode_label}**\nFFmpeg: {ffmpeg_status}\n\nStarting...")

        # --- Pipeline: asyncio Queue for parallel download→upload ---
        upload_queue = asyncio.Queue(maxsize=2)  # bounded: max 2 items buffered
        SENTINEL = object()

        async def producer():
            """Fetch messages and enqueue them for processing."""
            nonlocal skipped
            for msg_id in range(start_id, end_id + 1):
                logger.info(f"[User {admin_id}] Fetching message {msg_id}...")
                try:
                    message = await user_client.get_messages(entity, ids=msg_id)
                    if message and not isinstance(message, MessageEmpty):
                        await upload_queue.put(message)
                    else:
                        skipped += 1
                        logger.info(f"[User {admin_id}] Message {msg_id} empty/deleted. Skipping.")
                except Exception as e:
                    logger.error(f"[User {admin_id}] Fetch error msg {msg_id}: {e}")
                    skipped += 1
            await upload_queue.put(SENTINEL)

        async def consumer():
            """Process queued messages: forward or copy/send."""
            nonlocal processed, success
            while True:
                item = await upload_queue.get()
                if item is SENTINEL:
                    upload_queue.task_done()
                    break
                message = item
                try:
                    if group_forward_allowed:
                        await user_client.forward_messages(dest_group, message)
                        logger.info(f"Message {message.id} forwarded (Native).")
                        success += 1
                    else:
                        res = await forward_message(
                            user_client, message, dest_group, thumb_path, caption_rule,
                            bot_client=bot_client, admin_id=admin_id,
                            status_msg_id=status_msg.id, progress_ctx=progress_ctx
                        )
                        if res:
                            success += 1
                    await asyncio.sleep(1.5)
                except errors.FloodWaitError as fw:
                    logger.warning(f"FloodWait {fw.seconds}s for msg {message.id}. Waiting...")
                    await bot_client.send_message(admin_id, f"⏳ FloodWait: pausing {fw.seconds}s...")
                    await asyncio.sleep(fw.seconds + 2)
                    # Retry
                    try:
                        if group_forward_allowed:
                            await user_client.forward_messages(dest_group, message)
                            success += 1
                        else:
                            res = await forward_message(
                                user_client, message, dest_group, thumb_path, caption_rule,
                                bot_client=bot_client, admin_id=admin_id,
                                status_msg_id=status_msg.id, progress_ctx=progress_ctx
                            )
                            if res:
                                success += 1
                    except Exception as retry_e:
                        logger.error(f"Retry failed for msg {message.id}: {retry_e}")
                except Exception as e:
                    logger.error(f"[User {admin_id}] Error processing msg {message.id}: {e}")
                finally:
                    processed += 1
                    progress_ctx.update({'processed': processed, 'success': success})
                    upload_queue.task_done()

                    now = time.time()
                    nonlocal last_update_time, current_msg_id
                    current_msg_id = message.id
                    if now - last_update_time > 3:
                        last_update_time = now
                        progress_text = (
                            f"⏳ **Hybrid Forwarding Engine**\n\n"
                            f"📌 Msg ID: `{current_msg_id}`\n"
                            f"📊 Progress: {processed}/{total_ids}\n"
                            f"✅ Sent: {success}  |  ⏭️ Skipped: {skipped}\n"
                            f"🔧 Mode: {mode_label}\n"
                            f"🎬 FFmpeg: {ffmpeg_status}"
                        )
                        await safe_bot_edit(bot_client, admin_id, status_msg.id, progress_text)

        # Run producer and consumer concurrently (pipeline)
        await asyncio.gather(producer(), consumer())

        await bot_client.send_message(
            admin_id,
            f"✅ **Forwarding Complete!**\n"
            f"Sent: {success} | Skipped: {skipped} | Total: {total_ids}\n"
            f"Mode: {mode_label}"
        )
        
    except asyncio.CancelledError:
        logger.info(f"[User {admin_id}] Forwarding task cancelled.")
        if status_msg:
            await bot_client.send_message(admin_id, "🛑 Forwarding task was cancelled.")
    except Exception as e:
        logger.error(f"[User {admin_id}] Fatal error in forwarding task: {e}", exc_info=True)
        if status_msg:
            await bot_client.send_message(admin_id, f"⚠️ Fatal Error during forwarding: {e}")
    finally:
        if admin_id in forwarding_tasks:
            del forwarding_tasks[admin_id]
        if thumb_path and os.path.exists(thumb_path):
            try:
                os.remove(thumb_path)
            except:
                pass

async def cancel_task_only(admin_id):
    """Cancel the forwarding task but keep the session alive."""
    if admin_id in forwarding_tasks:
        forwarding_tasks[admin_id].cancel()
        del forwarding_tasks[admin_id]
        return True
    return False

async def logout_user(admin_id):
    """Cancel task, disconnect the userbot, and delete session file."""
    await cancel_task_only(admin_id)
    if admin_id in active_userbots:
        try:
            await active_userbots[admin_id].disconnect()
        except Exception:
            pass
        del active_userbots[admin_id]
    session_file = get_user_session_name(admin_id)
    if os.path.exists(session_file):
        os.remove(session_file)

# --- Navigation: which step to return to when Back is pressed ---
STEP_BACK = {
    'end_link':      'start_link',
    'dest_group':    'end_link',
    'thumb_option':  'dest_group',
    'wait_thumbnail':'thumb_option',
    'caption_option':'thumb_option',
    'wait_caption':  'caption_option',
}

BACK_BTN = [[Button.inline("◀️ Back", data=b"back")]]

TEMP_FILE_TTL_MINUTES = 30  # Delete temp files older than this

async def cleanup_temp_files():
    """Background task: delete files in temp_media older than TTL."""
    while True:
        await asyncio.sleep(10 * 60)  # Run every 10 minutes
        if not os.path.exists(TEMP_DIR):
            continue
        now = time.time()
        cutoff = TEMP_FILE_TTL_MINUTES * 60
        deleted = 0
        for fname in os.listdir(TEMP_DIR):
            fpath = os.path.join(TEMP_DIR, fname)
            try:
                if os.path.isfile(fpath) and (now - os.path.getmtime(fpath)) > cutoff:
                    os.remove(fpath)
                    deleted += 1
            except Exception as e:
                logger.warning(f"Cleanup: could not delete {fpath}: {e}")
        if deleted:
            logger.info(f"Cleanup: deleted {deleted} temp file(s) older than {TEMP_FILE_TTL_MINUTES} min.")

async def show_step_prompt(step, event_or_respond, state):
    """Re-show the prompt for a given step (used when Back is pressed)."""
    async def r(text, buttons=None):
        if hasattr(event_or_respond, 'edit'):
            await event_or_respond.edit(text, buttons=buttons)
        else:
            await event_or_respond.respond(text, buttons=buttons)

    if step == 'start_link':
        await r("📎 Please send the **Starting Message Link** (e.g. https://t.me/c/123456789/100):")
    elif step == 'end_link':
        sid = state.get('start_id', '?')
        await r(f"✅ Start ID: `{sid}`\n\nNow, send the **Ending Message Link** (same group):", BACK_BTN)
    elif step == 'dest_group':
        eid = state.get('end_id', '?')
        await r(f"✅ End ID: `{eid}`\n\nNow, send the **Destination Group ID** (e.g. `-100123456789`):", BACK_BTN)
    elif step == 'thumb_option':
        ffmpeg_note = "\n✅ FFmpeg On — videos will be streamable." if FFMPEG_AVAILABLE else "\n⚠️ FFmpeg Off — install FFmpeg for streaming."
        buttons = [
            [Button.inline("🖼️ Yes, Custom Thumbnail", data=b"thumb_yes"),
             Button.inline("❌ No Thumbnail", data=b"thumb_no")],
            BACK_BTN[0]
        ]
        await r(f"Do you want a **custom thumbnail** for videos?{ffmpeg_note}", buttons)
    elif step == 'wait_thumbnail':
        await r("🖼️ Send the **photo** to use as thumbnail for videos.", BACK_BTN)
    elif step == 'caption_option':
        buttons = [
            [Button.inline("📝 Keep Original", data=b"cap_keep")],
            [Button.inline("🗑️ Remove All", data=b"cap_remove")],
            [Button.inline("✏️ Custom Caption", data=b"cap_custom")],
            BACK_BTN[0]
        ]
        await r("How do you want to handle **captions**?", buttons)
    elif step == 'wait_caption':
        await r(
            "✏️ Send your **custom caption template**.\n\n"
            "Variables: `{filename}` `{size}` `{date}` `{original_caption}`\n\n"
            "Example: `{original_caption}\n📁 {filename} | 📦 {size}`",
            BACK_BTN
        )

async def main():
    load_dotenv()
    
    bot_token = os.getenv("BOT_TOKEN")
    api_id = os.getenv("API_ID")
    api_hash = os.getenv("API_HASH")

    if not bot_token or not api_id or not api_hash:
        logger.error("Missing BOT_TOKEN, API_ID, or API_HASH in .env file.")
        return
        
    try:
        api_id = int(api_id)
    except ValueError:
        logger.error("API_ID must be a number.")
        return

    bot_client = TelegramClient(BOT_SESSION, api_id, api_hash)
    await bot_client.start(bot_token=bot_token)  # type: ignore
    
    # Set GUI Commands
    try:
        await bot_client(functions.bots.SetBotCommandsRequest(
            scope=types.BotCommandScopeDefault(),
            lang_code='en',
            commands=[
                types.BotCommand(command="start", description="Login & start a forwarding task"),
                types.BotCommand(command="status", description="Show current task status"),
                types.BotCommand(command="stop", description="Stop forwarding (stay logged in)"),
                types.BotCommand(command="logout", description="Stop task & delete session"),
            ]
        ))
    except Exception as e:
        logger.warning(f"Failed to set bot commands: {e}")
    
    logger.info("Controller Bot started successfully! Waiting for commands in Telegram...")

    # Start background temp-file cleanup task
    asyncio.create_task(cleanup_temp_files())
    logger.info(f"Temp file cleanup task started (TTL={TEMP_FILE_TTL_MINUTES} min).")

    async def start_task_from_state(event, state, sender_id):
        """Launch the forwarding task and always respond (never edit a user message)."""
        user_client = state['user_client']
        chat_id = state['chat_id']
        start_id = state['start_id']
        end_id = state['end_id']
        dest_group = state['dest_group']
        thumb_path = state.get('thumb_path')
        caption_rule = state.get('caption_rule')
        
        if sender_id in forwarding_tasks:
            await event.respond("🛑 Stopping previous forwarding task before starting a new one...")
            forwarding_tasks[sender_id].cancel()
        
        task = asyncio.create_task(start_forwarding_task(
            user_client, chat_id, start_id, end_id, dest_group,
            bot_client, sender_id, thumb_path, caption_rule
        ))
        forwarding_tasks[sender_id] = task
        
        await event.respond(
            "🚀 **Configuration Complete!**\n"
            "Hybrid Forwarding Engine started. I will automatically determine the best upload mode.\n"
            "Progress updates will appear below shortly..."
        )
        del user_states[sender_id]

    @bot_client.on(events.NewMessage(pattern='/start'))
    async def start_command(event):
        sender_id = event.sender_id

        # Already logged in — skip auth, go straight to task config
        if sender_id in active_userbots:
            user_states[sender_id] = {'step': 'start_link', 'user_client': active_userbots[sender_id]}
            await event.respond(
                "✅ You are already logged in!\n\n"
                "📎 Please send the **Starting Message Link** to begin a new task:"
            )
            return

        session_file = get_user_session_name(sender_id)
        if os.path.exists(session_file):
            try:
                user_client = TelegramClient(session_file, api_id, api_hash)
                await user_client.connect()
                if await user_client.is_user_authorized():
                    active_userbots[sender_id] = user_client
                    user_states[sender_id] = {'step': 'start_link', 'user_client': user_client}
                    await event.respond(
                        "✅ Found an active session! No need to login again.\n\n"
                        "📎 Please send the **Starting Message Link**:"
                    )
                    return
                else:
                    await user_client.disconnect()
                    os.remove(session_file)
            except Exception as e:
                logger.error(f"Failed to check existing login: {e}")

        user_states[sender_id] = {'step': 'phone'}
        await event.respond(
            "👋 Welcome to the Auto-Forwarder!\n\n"
            "Please send me your **Phone Number** with country code (e.g. +1234567890)."
        )

    @bot_client.on(events.NewMessage(pattern='/stop'))
    async def stop_command(event):
        sender_id = event.sender_id
        cancelled = await cancel_task_only(sender_id)
        if cancelled:
            await event.respond(
                "🛑 Forwarding task stopped.\n"
                "✅ You are still logged in. Use /start to begin a new task."
            )
        else:
            await event.respond("ℹ️ No active forwarding task found.")

    @bot_client.on(events.NewMessage(pattern='/logout'))
    async def logout_command(event):
        sender_id = event.sender_id
        await logout_user(sender_id)
        await event.respond(
            "🚪 **Logged out successfully.**\n"
            "Your forwarding task (if any) has been stopped and your session has been permanently deleted.\n"
            "Use /start to login again."
        )
        if sender_id in user_states:
            del user_states[sender_id]

    @bot_client.on(events.NewMessage(pattern='/status'))
    async def status_command(event):
        sender_id = event.sender_id
        if sender_id in forwarding_tasks:
            await event.respond(
                "🔄 **A forwarding task is currently running.**\n"
                "Use /stop to cancel it (you'll stay logged in), or /logout to stop and delete your session."
            )
        elif sender_id in active_userbots:
            await event.respond(
                "✅ **Logged in** — No active forwarding task.\n"
                "Use /start to begin a new task or /logout to sign out."
            )
        else:
            await event.respond("❌ You are not logged in. Use /start to begin.")

    @bot_client.on(events.CallbackQuery)
    async def callback_handler(event):
        sender_id = event.sender_id
        if sender_id not in user_states:
            await event.answer("Session expired. Please use /start again.")
            return

        state = user_states[sender_id]
        step = state.get('step')
        data = event.data.decode('utf-8')

        # --- Back navigation ---
        if data == 'back':
            prev_step = STEP_BACK.get(step)
            if not prev_step:
                await event.answer("Nothing to go back to.")
                return
            state['step'] = prev_step
            await event.answer("Going back...")
            await show_step_prompt(prev_step, event, state)
            return

        if step == 'thumb_option':
            if data == 'thumb_yes':
                state['step'] = 'wait_thumbnail'
                await event.edit(
                    "🖼️ Please send me the **photo** to use as the thumbnail for videos.",
                    buttons=BACK_BTN
                )
            else:
                state['thumb_path'] = None
                state['step'] = 'caption_option'
                buttons = [
                    [Button.inline("📝 Keep Original", data=b"cap_keep")],
                    [Button.inline("🗑️ Remove All", data=b"cap_remove")],
                    [Button.inline("✏️ Custom Caption", data=b"cap_custom")],
                    BACK_BTN[0]
                ]
                await event.edit("How do you want to handle **captions**?", buttons=buttons)

        elif step == 'caption_option':
            if data == 'cap_keep':
                state['caption_rule'] = 'keep'
                await event.edit("✅ Will keep original captions.")
                await start_task_from_state(event, state, sender_id)
            elif data == 'cap_remove':
                state['caption_rule'] = 'remove'
                await event.edit("✅ Will remove all captions.")
                await start_task_from_state(event, state, sender_id)
            elif data == 'cap_custom':
                state['step'] = 'wait_caption'
                await event.edit(
                    "✏️ Send your **custom caption template**.\n\n"
                    "Variables:\n"
                    "`{filename}` `{size}` `{date}` `{original_caption}`\n\n"
                    "Example: `{original_caption}\n📁 {filename} | 📦 {size}`",
                    buttons=BACK_BTN
                )

    @bot_client.on(events.NewMessage)
    async def handle_conversation(event):
        if event.message.text and event.message.text.startswith('/'):
            return
            
        sender_id = event.sender_id
        if sender_id not in user_states:
            return
            
        state = user_states[sender_id]
        step = state.get('step')
        text = event.message.text.strip() if event.message.text else ""

        if step == 'phone':
            state['phone'] = text
            await event.respond("📡 Requesting login code from Telegram... please wait.")
            
            try:
                session_file = get_user_session_name(sender_id)
                user_client = TelegramClient(session_file, api_id, api_hash)
                await user_client.connect()
                
                req = await user_client.send_code_request(state['phone'])
                state['phone_code_hash'] = req.phone_code_hash
                state['user_client'] = user_client
                state['step'] = 'code'
                
                await event.respond(
                    "📱 A login code has been sent to your Telegram app!\n\n"
                    "🚨 **IMPORTANT** 🚨\n"
                    "Send the code with **spaces** between each digit to bypass Telegram's anti-phishing filter.\n"
                    "👉 **Example:** Code `12345` → Send as `1 2 3 4 5`."
                )
            except Exception as e:
                logger.error(f"Failed to send code: {e}")
                await event.respond(f"❌ Failed to request code: {e}\nPlease check your number and try /start again.")
                if sender_id in user_states:
                    del user_states[sender_id]
                
        elif step == 'code':
            code = text.replace(' ', '')
            if not code.isdigit() or len(code) < 5:
                await event.respond("❌ Invalid format. Please send the code with spaces between digits (e.g., `1 2 3 4 5`).")
                return
                
            await event.respond("🔐 Logging in... please wait.")
            user_client = state['user_client']
            try:
                await user_client.sign_in(phone=state['phone'], code=code, phone_code_hash=state['phone_code_hash'])
                active_userbots[sender_id] = user_client
                state['step'] = 'start_link'
                await event.respond("✅ **Login Successful!**\n\nNow, please send the **Starting Message Link** (e.g. https://t.me/c/123456789/100):")
                
            except errors.SessionPasswordNeededError:
                state['step'] = 'password'
                await event.respond("🔒 Two-Step Verification detected.\nPlease send your **2FA password**.")
            except Exception as e:
                logger.error(f"Failed to sign in: {e}")
                await event.respond(f"❌ Login failed: {e}\nPlease try /start again.")
                await user_client.disconnect()
                if sender_id in user_states:
                    del user_states[sender_id]
                
        elif step == 'password':
            await event.respond("🔐 Verifying password...")
            user_client = state['user_client']
            try:
                await user_client.sign_in(password=text)
                active_userbots[sender_id] = user_client
                state['step'] = 'start_link'
                await event.respond("✅ **Login Successful!**\n\nNow, please send the **Starting Message Link** (e.g. https://t.me/c/123456789/100):")
            except Exception as e:
                logger.error(f"Failed 2FA sign in: {e}")
                await event.respond(f"❌ 2FA failed: {e}\nPlease try /start again.")
                await user_client.disconnect()
                if sender_id in user_states:
                    del user_states[sender_id]
                
        elif step == 'start_link':
            chat_id, msg_id = parse_telegram_link(text)
            if not chat_id or not msg_id:
                await event.respond("❌ Invalid link format. Please send a valid Telegram message link.")
                return
            state['chat_id'] = chat_id
            state['start_id'] = msg_id
            state['step'] = 'end_link'
            await event.respond(
                f"✅ Start message detected (ID: `{msg_id}`).\n\n"
                "Now, send the **Ending Message Link** (from the same group):",
                buttons=BACK_BTN
            )

        elif step == 'end_link':
            chat_id, msg_id = parse_telegram_link(text)
            if not chat_id or not msg_id or chat_id != state['chat_id']:
                await event.respond("❌ Invalid link or group mismatch. Must be from the **SAME group** as the start link.")
                return
            if msg_id < state['start_id']:
                await event.respond("❌ End message ID is smaller than start ID. Please send a valid ending link.")
                return
            state['end_id'] = msg_id
            state['step'] = 'dest_group'
            await event.respond(
                f"✅ End message detected (ID: `{msg_id}`).\n\n"
                "Now, send the **Destination Group ID** (e.g. `-100123456789` or username):",
                buttons=BACK_BTN
            )

        elif step == 'dest_group':
            try:
                dest_group = int(text)
            except ValueError:
                dest_group = text
            state['dest_group'] = dest_group
            state['step'] = 'thumb_option'
            ffmpeg_note = "\n✅ FFmpeg On — videos will be streamable." if FFMPEG_AVAILABLE else "\n⚠️ FFmpeg Off — install FFmpeg for streaming."
            buttons = [
                [Button.inline("🖼️ Yes, Custom Thumbnail", data=b"thumb_yes"),
                 Button.inline("❌ No Thumbnail", data=b"thumb_no")],
                BACK_BTN[0]
            ]
            await event.respond(
                f"Do you want a **custom thumbnail** for uploaded videos?{ffmpeg_note}",
                buttons=buttons
            )

        elif step == 'wait_thumbnail':
            if event.message.photo:
                os.makedirs(TEMP_DIR, exist_ok=True)
                path = f"{TEMP_DIR}/thumb_{sender_id}.jpg"
                await event.message.download_media(file=path)
                state['thumb_path'] = path
                state['step'] = 'caption_option'
                buttons = [
                    [Button.inline("📝 Keep Original", data=b"cap_keep")],
                    [Button.inline("🗑️ Remove All", data=b"cap_remove")],
                    [Button.inline("✏️ Custom Caption", data=b"cap_custom")],
                    BACK_BTN[0]
                ]
                await event.respond("✅ Thumbnail saved!\n\nHow do you want to handle **captions**?", buttons=buttons)
            else:
                await event.respond("❌ That is not a valid photo. Please send an **image** as the thumbnail.", buttons=BACK_BTN)

        elif step == 'wait_caption':
            state['caption_rule'] = text
            await event.respond("✅ Custom caption template saved!")
            await start_task_from_state(event, state, sender_id)

    try:
        await bot_client.run_until_disconnected()
    except Exception as e:
        logger.error(f"Controller Bot disconnected: {e}")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot stopped by user.")
