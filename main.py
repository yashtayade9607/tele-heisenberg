import os
import io
import asyncio
import logging
import time
import re
import shutil
import subprocess
from concurrent.futures import ThreadPoolExecutor
from dotenv import load_dotenv
from telethon import TelegramClient, events, errors, Button
from telethon.tl.types import (
    MessageMediaWebPage, MessageMediaUnsupported, MessageEmpty,
    MessageMediaPhoto, MessageMediaDocument,
    InputMediaDocument, InputMediaPhoto,
    InputDocument, InputPhoto,
    DocumentAttributeVideo, DocumentAttributeImageSize, DocumentAttributeFilename
)
from telethon.tl.functions.messages import SendMediaRequest, SendMessageRequest
from telethon import functions, types
import mimetypes
from fast_telethon import FastTelethon

# Thread pool for blocking operations (FFmpeg, disk I/O)
_thread_pool = ThreadPoolExecutor(max_workers=4)

# Number of parallel consumer workers for concurrent forwarding
NUM_WORKERS = 3

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
    logger.info("FFmpeg not found in PATH. Fast-start streamable processing disabled.")

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
        return message.message or ""
    if template == 'remove':
        return ""
        
    filename = "Unknown"
    size = "Unknown"
    
    if message.file:
        filename = message.file.name or "Unknown"
        size = format_size(message.file.size)
        
    date_str = message.date.strftime("%Y-%m-%d %H:%M:%S") if message.date else "Unknown"
    
    from telethon.extensions import markdown
    original_caption_md = markdown.unparse(message.message or "", message.entities or [])
    
    formatted = template.replace("{filename}", filename)
    formatted = formatted.replace("{size}", size)
    formatted = formatted.replace("{date}", date_str)
    formatted = formatted.replace("{original_caption}", original_caption_md)
    
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

def _ffmpeg_run_sync(input_path):
    """Synchronous FFmpeg call — intended to run in a thread pool."""
    base, ext = os.path.splitext(input_path)
    output_path = base + "_streamable" + (ext if ext else ".mp4")
    cmd = [
        "ffmpeg", "-y",
        "-i", input_path,
        "-c", "copy",
        "-movflags", "+faststart",
        "-metadata", "title=",
        "-metadata", "artist=",
        "-metadata", "author=",
        "-metadata", "comment=",
        output_path
    ]
    try:
        result = subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, timeout=120)
        if result.returncode == 0 and os.path.exists(output_path):
            return output_path
        err = result.stderr.decode('utf-8', errors='ignore')[-500:]
        logger.error(f"FFmpeg failed: {err}")
        return None
    except subprocess.TimeoutExpired:
        logger.error("FFmpeg timed out.")
        return None
    except Exception as e:
        logger.error(f"FFmpeg exception: {e}")
        return None

async def process_video_for_streaming(input_path):
    """Non-blocking FFmpeg: runs in thread pool so asyncio loop is never blocked."""
    if not FFMPEG_AVAILABLE:
        return None
    loop = asyncio.get_event_loop()
    result = await loop.run_in_executor(_thread_pool, _ffmpeg_run_sync, input_path)
    if result:
        logger.info(f"FFmpeg processing complete: {result}")
    return result

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

    from telethon.extensions import markdown as md_ext

    # Build final caption + entities
    final_caption = format_caption(caption_rule, message)
    is_custom_cap = caption_rule and caption_rule not in ('keep', 'remove', None)

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

    dest_peer = await client.get_input_entity(dest_group)

    # Resolve caption text + entities for SendMediaRequest
    if final_parse_mode == 'md' and final_caption:
        from telethon.extensions import markdown
        msg_text, msg_entities = markdown.parse(final_caption)
    elif final_entities:
        msg_text = final_caption or ''
        msg_entities = final_entities
    else:
        msg_text = final_caption or ''
        msg_entities = []

    # ---------------------------------------------------------------
    # TIER 1: True server-side copy via InputMedia references
    # Telegram copies the file on their servers — zero bytes transferred.
    # Works for any file size in milliseconds.
    # ---------------------------------------------------------------
    if not thumb_path:
        try:
            if isinstance(message.media, MessageMediaDocument) and message.document:
                doc = message.document
                input_media = InputMediaDocument(
                    id=InputDocument(
                        id=doc.id,
                        access_hash=doc.access_hash,
                        file_reference=doc.file_reference
                    ),
                    spoiler=False
                )
                await client(SendMediaRequest(
                    peer=dest_peer,
                    media=input_media,
                    message=msg_text,
                    entities=msg_entities
                ))
                logger.info(f"Message {message.id} server-copied (Document) instantly.")
                return 'fast_copy'

            elif isinstance(message.media, MessageMediaPhoto) and message.photo:
                photo = message.photo
                input_media = InputMediaPhoto(
                    id=InputPhoto(
                        id=photo.id,
                        access_hash=photo.access_hash,
                        file_reference=photo.file_reference
                    ),
                    spoiler=False
                )
                await client(SendMediaRequest(
                    peer=dest_peer,
                    media=input_media,
                    message=msg_text,
                    entities=msg_entities
                ))
                logger.info(f"Message {message.id} server-copied (Photo) instantly.")
                return 'fast_copy'

            elif not message.media or isinstance(message.media, (MessageMediaWebPage, MessageMediaUnsupported)):
                # Text-only
                if msg_text:
                    await client(SendMessageRequest(
                        peer=dest_peer,
                        message=msg_text,
                        entities=msg_entities
                    ))
                    logger.info(f"Message {message.id} sent as text.")
                return 'fast_copy'

        except errors.FileReferenceExpiredError:
            logger.warning(f"Message {message.id}: File reference expired. Refreshing and retrying...")
            # Refresh the message from server to get a fresh file_reference
            try:
                fresh_msg = await client.get_messages(message.chat_id or message.peer_id, ids=message.id)
                if fresh_msg:
                    message = fresh_msg
                    # Recurse once with fresh reference
                    return await forward_message(
                        client, message, dest_group, thumb_path, caption_rule,
                        bot_client=bot_client, admin_id=admin_id,
                        status_msg_id=status_msg_id, progress_ctx=progress_ctx
                    )
            except Exception as refresh_e:
                logger.warning(f"Refresh failed: {refresh_e}. Falling back to download...")
        except (errors.ChatForwardsRestrictedError, errors.MediaEmptyError,
                errors.MessageIdInvalidError, errors.FileIdInvalidError):
            logger.info(f"Message {message.id}: Server-copy blocked by Telegram. Falling back to download/upload...")
        except Exception as e:
            logger.warning(f"Server-copy failed for {message.id}: {e}. Falling back to download...")

    # ---------------------------------------------------------------
    # TIER 2: send_file with media reference (catches other media types)
    # ---------------------------------------------------------------
    if not thumb_path:
        try:
            if message.media and not isinstance(message.media, (MessageMediaWebPage, MessageMediaUnsupported)):
                await client.send_file(
                    dest_group,
                    message.media,
                    caption=final_caption,
                    formatting_entities=final_entities,
                    parse_mode=final_parse_mode
                )
                logger.info(f"Message {message.id} copied via send_file reference.")
                return 'fast_copy'
        except (errors.ChatForwardsRestrictedError, errors.MediaEmptyError,
                errors.FileReferenceExpiredError, errors.MessageIdInvalidError):
            logger.info(f"Message {message.id}: send_file reference failed. Falling back to download/upload...")
        except Exception as e:
            logger.warning(f"send_file reference failed for {message.id}: {e}. Falling back to download...")

    # --- 2. Fallback to Full Download/Upload ---
    path = None
    streamable_path = None
    try:
        if message.media and not isinstance(message.media, (MessageMediaWebPage, MessageMediaUnsupported)):
            file_size = message.file.size if message.file else 0
            # Use MIME-inferred extension instead of blindly using 'file.bin'
            file_name = get_proper_filename(message)

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
                logger.info(f"Message {message.id}: Large file ({format_size(file_size)}), downloading to disk ({uid})...")
                os.makedirs(TEMP_DIR, exist_ok=True)
                dl_target = os.path.join(TEMP_DIR, unique_prefix + safe_name)

                # Notify user that download is starting
                if bot_client and admin_id:
                    await bot_client.send_message(
                        admin_id,
                        f"📥 **Downloading large file...**\n"
                        f"📌 Msg `{message.id}` | 📦 Size: {format_size(file_size)}\n"
                        f"⏳ Please wait..."
                    )

                # Use native Telethon download — handles DC migration, auth, protected chats reliably
                _last_dl_update = [0.0]
                _dl_msg_id = [None]

                async def dl_progress(current, total):
                    now = time.time()
                    if now - _last_dl_update[0] > 3 and bot_client and admin_id:
                        _last_dl_update[0] = now
                        pct = (current / total * 100) if total else 0
                        txt = (
                            f"📥 **Downloading...**\n"
                            f"📌 Msg `{message.id}` | {pct:.1f}%\n"
                            f"   {format_size(current)} / {format_size(total)}"
                        )
                        if _dl_msg_id[0]:
                            await safe_bot_edit(bot_client, admin_id, _dl_msg_id[0], txt)
                        else:
                            sent = await bot_client.send_message(admin_id, txt)
                            _dl_msg_id[0] = sent.id

                path = await client.download_media(message, file=dl_target, progress_callback=dl_progress)

                if path:
                    upload_path = path
                    is_vid = is_video_file(path)

                    # Apply FFmpeg fast-start for video files (makes them streamable)
                    if is_vid and FFMPEG_AVAILABLE:
                        logger.info(f"Message {message.id}: Applying FFmpeg fast-start (non-blocking)...")
                        streamable_path = await process_video_for_streaming(path)
                        if streamable_path:
                            upload_path = streamable_path

                    # Smart thumb
                    effective_thumb = None
                    if is_vid:
                        if thumb_path:
                            effective_thumb = thumb_path
                        else:
                            effective_thumb = await get_original_thumb(client, message)

                    # Upload with FastTelethon parallel uploader (much faster than sequential)
                    if bot_client and admin_id:
                        up_notify = await bot_client.send_message(
                            admin_id,
                            f"📤 **Uploading...**\n📌 Msg `{message.id}` | 📦 {format_size(os.path.getsize(upload_path))}"
                        )
                    else:
                        up_notify = None

                    _last_ul_update = [0.0]
                    async def ul_progress(current, total):
                        now = time.time()
                        if now - _last_ul_update[0] > 3 and bot_client and admin_id and up_notify:
                            _last_ul_update[0] = now
                            pct = (current / total * 100) if total else 0
                            txt = (
                                f"📤 **Uploading...**\n"
                                f"📌 Msg `{message.id}` | {pct:.1f}%\n"
                                f"   {format_size(current)} / {format_size(total)}"
                            )
                            await safe_bot_edit(bot_client, admin_id, up_notify.id, txt)

                    fast_up = FastTelethon(client)
                    uploaded_file = await fast_up.upload_file(upload_path, progress_callback=ul_progress)

                    if uploaded_file:
                        doc_attrs = None
                        if hasattr(message, 'document') and message.document:
                            doc_attrs = message.document.attributes
                        await client.send_file(
                            dest_group, uploaded_file,
                            caption=final_caption,
                            formatting_entities=final_entities,
                            parse_mode=final_parse_mode,
                            thumb=effective_thumb,
                            supports_streaming=bool(is_vid),
                            attributes=doc_attrs
                        )
                    else:
                        logger.error(f"FastTelethon upload failed for msg {message.id}, trying native upload...")
                        await client.send_file(
                            dest_group, upload_path,
                            caption=final_caption,
                            formatting_entities=final_entities,
                            parse_mode=final_parse_mode,
                            thumb=effective_thumb,
                            supports_streaming=bool(is_vid)
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

        mode_label = "Fast Copy / Hybrid Download"

        # Save to global stats for /status
        if admin_id in forwarding_tasks:
            forwarding_tasks[admin_id]['stats'] = {
                'total': total_ids, 'processed': 0, 'success': 0, 'skipped': 0,
                'current_msg_id': start_id, 'mode': mode_label
            }

        ffmpeg_status = "✅ On" if FFMPEG_AVAILABLE else "❌ Off"
        await safe_bot_edit(bot_client, admin_id, status_msg.id,
            f"🔍 Chat resolved!\nMode: **{mode_label}**\nFFmpeg: {ffmpeg_status}\n\nStarting...")

        # --- High-Performance Pipeline: batch fetch + multiple parallel workers ---
        # Bounded queue: enough buffer for all workers to stay busy
        upload_queue = asyncio.Queue(maxsize=NUM_WORKERS * 3)
        SENTINEL = object()

        # --- High-Performance Sequential Pipeline ---
        # We fetch messages in batches to reduce API latency, then process them sequentially to guarantee exact order.
        all_ids = list(range(start_id, end_id + 1))
        BATCH = 100  # Telegram allows up to 100 IDs per request
        
        for i in range(0, len(all_ids), BATCH):
            batch_ids = all_ids[i:i + BATCH]
            try:
                messages = await user_client.get_messages(entity, ids=batch_ids)
                # get_messages with a list returns results in the exact same order
                for message in messages:
                    if not message or isinstance(message, MessageEmpty):
                        skipped += 1
                        processed += 1
                        continue
                        
                    try:
                        res = await forward_message(
                            user_client, message, dest_group, thumb_path, caption_rule,
                            bot_client=bot_client, admin_id=admin_id,
                            status_msg_id=status_msg.id, progress_ctx=progress_ctx
                        )
                        if res:
                            success += 1
                    except errors.FloodWaitError as fw:
                        logger.warning(f"FloodWait {fw.seconds}s for msg {message.id}. Waiting...")
                        await bot_client.send_message(admin_id, f"⏳ FloodWait: pausing {fw.seconds}s...")
                        await asyncio.sleep(fw.seconds + 2)
                        try:
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
                        logger.error(f"Error processing msg {message.id}: {e}")
                    finally:
                        processed += 1
                        progress_ctx.update({'processed': processed, 'success': success})
                        
                        if admin_id in forwarding_tasks:
                            forwarding_tasks[admin_id]['stats'].update({
                                'processed': processed,
                                'success': success,
                                'skipped': skipped,
                                'current_msg_id': message.id
                            })

                        now = time.time()
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
                            
            except Exception as e:
                logger.error(f"[User {admin_id}] Batch fetch error (ids {batch_ids[0]}-{batch_ids[-1]}): {e}")
                skipped += len(batch_ids)
                processed += len(batch_ids)

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
        forwarding_tasks[admin_id]['task'].cancel()
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
             Button.inline("⚡ Instant Copy (No Thumb)", data=b"thumb_no")],
            BACK_BTN[0]
        ]
        await r(f"Do you want a **custom thumbnail** for videos?\n\n⚠️ **WARNING:** Using a custom thumbnail completely disables Instant Copy and forces a slow download/upload process.{ffmpeg_note}", buttons)
    elif step == 'wait_thumbnail':
        await r("🖼️ Send the **photo** to use as thumbnail for videos.", BACK_BTN)
    elif step == 'caption_option':
        buttons = [
            [Button.inline("📝 Keep Original", data=b"cap_keep")],
            [Button.inline("🗑️ Remove All", data=b"cap_remove")],
            [Button.inline("✏️ Custom Template", data=b"cap_custom")],
            [Button.inline("✍️ Edit Current Caption", data=b"cap_edit_current")],
            [Button.inline("🏷️ Add 'Extracted by'", data=b"cap_add_extracted")],
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
    elif step == 'wait_edit_caption' or step == 'wait_add_extracted':
        await r("✏️ Send your caption input...", BACK_BTN)

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

    bot_client = TelegramClient(
        BOT_SESSION, api_id, api_hash,
        connection_retries=10,
        request_retries=5,
        flood_sleep_threshold=60,
    )
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
            forwarding_tasks[sender_id]['task'].cancel()
        
        task = asyncio.create_task(start_forwarding_task(
            user_client, chat_id, start_id, end_id, dest_group,
            bot_client, sender_id, thumb_path, caption_rule
        ))
        forwarding_tasks[sender_id] = {'task': task, 'stats': {}}
        
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
            stats = forwarding_tasks[sender_id].get('stats', {})
            txt = (
                f"🔄 **Forwarding Task Running**\n\n"
                f"📊 Progress: {stats.get('processed', 0)} / {stats.get('total', 0)}\n"
                f"✅ Sent: {stats.get('success', 0)}\n"
                f"⏭️ Skipped: {stats.get('skipped', 0)}\n"
                f"📌 Current Msg ID: {stats.get('current_msg_id', 'N/A')}\n"
                f"🔧 Mode: {stats.get('mode', 'N/A')}\n\n"
                f"Use /stop to cancel it (you'll stay logged in)."
            )
            await event.respond(txt)
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
                    [Button.inline("✏️ Custom Template", data=b"cap_custom")],
                    [Button.inline("✍️ Edit Current Caption", data=b"cap_edit_current")],
                    [Button.inline("🏷️ Add 'Extracted by'", data=b"cap_add_extracted")],
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
            elif data == 'cap_edit_current':
                state['step'] = 'wait_edit_caption'
                await event.edit("Fetching original caption...")
                try:
                    msg = await state['user_client'].get_messages(state['chat_id'], ids=state['start_id'])
                    from telethon.extensions import markdown
                    orig_text = markdown.unparse(msg.message or "", msg.entities or [])
                    if orig_text:
                        await event.edit(
                            f"📝 **Edit Current Caption**\n\n"
                            f"Tap the text below to copy it, make your changes, and send it back to me:\n\n"
                            f"`{orig_text}`",
                            buttons=BACK_BTN
                        )
                    else:
                        await event.edit(
                            "❌ The selected starting message has no caption.\n"
                            "Please send the new caption you'd like to use:",
                            buttons=BACK_BTN
                        )
                except Exception as e:
                    await event.edit(f"❌ Failed to fetch message: {e}\nPlease send your custom caption:")
                    
            elif data == 'cap_add_extracted':
                state['step'] = 'wait_add_extracted'
                await event.edit(
                    "🏷️ **Add 'Extracted by' Text**\n\n"
                    "Send the text you want to append to the end of the original caption.\n"
                    "Example: `\n\n🚀 Extracted by @MyChannel`",
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

        elif step == 'wait_edit_caption':
            state['caption_rule'] = text
            await event.respond("✅ Edited caption saved!")
            await start_task_from_state(event, state, sender_id)
            
        elif step == 'wait_add_extracted':
            state['caption_rule'] = "{original_caption}\n" + text
            await event.respond("✅ 'Extracted by' text saved!")
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
