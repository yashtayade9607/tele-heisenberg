import os
import math
import asyncio
from telethon import utils, helpers
from telethon.tl.functions.upload import GetFileRequest, SaveBigFilePartRequest
from telethon.tl.types import InputFileBig


class FastTelethon:
    def __init__(self, client):
        self.client = client

    async def download_file(self, message, file_path, progress_callback=None, workers=4):
        media = message.media
        if not media:
            return None

        file_ref = utils.get_input_location(media)
        if not file_ref:
            return None

        file_size = getattr(message.file, 'size', 0)
        if not file_size:
            # Fallback for small/unknown size files
            return await self.client.download_media(message, file=file_path, progress_callback=progress_callback)

        chunk_size = 1024 * 1024  # 1MB chunks
        parts = math.ceil(file_size / chunk_size)

        # Pre-allocate file on disk
        def create_file(p, size):
            with open(p, 'wb') as f:
                f.truncate(size)

        await asyncio.to_thread(create_file, file_path, file_size)

        downloaded = [0]

        async def download_worker(queue):
            while True:
                task = await queue.get()
                if task is None:
                    break
                part_idx = task
                try:
                    offset = part_idx * chunk_size
                    limit = min(chunk_size, file_size - offset)

                    result = await self.client(GetFileRequest(
                        location=file_ref,
                        offset=offset,
                        limit=limit
                    ))

                    def write_chunk(p, off, b):
                        with open(p, 'r+b') as f:
                            f.seek(off)
                            f.write(b)

                    await asyncio.to_thread(write_chunk, file_path, offset, result.bytes)

                    downloaded[0] += len(result.bytes)
                    if progress_callback:
                        await progress_callback(downloaded[0], file_size)

                    # Yield control back to event loop
                    await asyncio.sleep(0.01)

                except Exception as e:
                    # On failure, re-queue this part for retry
                    await queue.put(part_idx)
                finally:
                    queue.task_done()

        queue = asyncio.Queue()
        for i in range(parts):
            queue.put_nowait(i)

        worker_tasks = [asyncio.create_task(download_worker(queue)) for _ in range(workers)]

        await queue.join()

        for _ in range(workers):
            await queue.put(None)

        await asyncio.gather(*worker_tasks)
        return file_path

    async def upload_file(self, file_path, progress_callback=None, workers=4):
        if not os.path.exists(file_path):
            return None

        file_size = os.path.getsize(file_path)
        if file_size == 0:
            return None

        chunk_size = 512 * 1024  # 512KB chunks
        parts = math.ceil(file_size / chunk_size)
        file_id = helpers.generate_random_long()

        uploaded = [0]

        async def upload_worker(queue):
            while True:
                task = await queue.get()
                if task is None:
                    break
                part_idx = task
                try:
                    offset = part_idx * chunk_size

                    def read_chunk(p, off, size):
                        with open(p, 'rb') as f:
                            f.seek(off)
                            return f.read(size)

                    chunk = await asyncio.to_thread(read_chunk, file_path, offset, chunk_size)

                    if not chunk:
                        queue.task_done()
                        continue

                    await self.client(SaveBigFilePartRequest(
                        file_id=file_id,
                        file_part=part_idx,
                        file_total_parts=parts,
                        bytes=chunk
                    ))

                    uploaded[0] += len(chunk)
                    if progress_callback:
                        await progress_callback(uploaded[0], file_size)

                    # Yield control back to event loop
                    await asyncio.sleep(0.01)

                except Exception as e:
                    # On failure, re-queue this part for retry
                    await queue.put(part_idx)
                finally:
                    queue.task_done()

        queue = asyncio.Queue()
        for i in range(parts):
            queue.put_nowait(i)

        worker_tasks = [asyncio.create_task(upload_worker(queue)) for _ in range(workers)]

        await queue.join()

        for _ in range(workers):
            await queue.put(None)

        await asyncio.gather(*worker_tasks)

        return InputFileBig(
            id=file_id,
            parts=parts,
            name=os.path.basename(file_path)
        )
