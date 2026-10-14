import asyncio
import os
import re
import time
from typing import Union

import aiohttp
import yt_dlp
from pyrogram.enums import MessageEntityType
from pyrogram.types import Message
from py_yt import VideosSearch, Playlist

DOWNLOAD_DIR = "downloads"

AUDIO_EXTENSIONS = ("webm", "m4a", "mp3", "ogg")
VIDEO_EXTENSIONS = ("mp4", "mkv", "webm")

# How long a failing API stays in cooldown (seconds)
API_COOLDOWN = 20
# Consecutive fails before putting API into cooldown
MAX_CONSECUTIVE_FAILS = 1


def _env_dir(name: str) -> str:
    value = os.environ.get(name, "").strip()
    return os.path.abspath(os.path.expanduser(value)) if value else ""


AUDIO_DOWNLOAD_PATH = _env_dir("AUDIO_DOWNLOAD_PATH")
VIDEO_DOWNLOAD_PATH = _env_dir("VIDEO_DOWNLOAD_PATH")


def is_external_path(path) -> bool:
    if not path:
        return False
    full = os.path.abspath(str(path))
    for base in (AUDIO_DOWNLOAD_PATH, VIDEO_DOWNLOAD_PATH):
        if base and full.startswith(base + os.sep):
            return True
    return False


def _find_external(directory, video_id, extensions, resp=None):
    names = []
    if resp is not None:
        try:
            disposition = resp.content_disposition
            if disposition and disposition.filename:
                name = os.path.basename(disposition.filename)
                if name.startswith(video_id + "."):
                    names.append(name)
        except Exception:
            pass
    names.extend(f"{video_id}.{ext}" for ext in extensions)
    for name in names:
        path = os.path.join(directory, name)
        if os.path.isfile(path) and os.path.getsize(path) > 0:
            return path
    return None


def time_to_seconds(time):
    stringt = str(time)
    return sum(int(x) * 60 ** i for i, x in enumerate(reversed(stringt.split(":"))))


# =========================================================
# Shruti API Pool — rotation + failover + cooldown
# =========================================================

class ShrutiAPI:
    def __init__(self, url: str, key: str, label: str = None):
        self.url = url.rstrip("/")
        self.key = key
        self.label = label or f"{self.url} [{key[:6]}...]"
        self.fail_count = 0
        self.cooldown_until = 0.0
        self.total_calls = 0
        self.total_ok = 0

    @property
    def available(self) -> bool:
        return time.time() >= self.cooldown_until

    def mark_success(self):
        self.fail_count = 0
        self.cooldown_until = 0.0
        self.total_calls += 1
        self.total_ok += 1

    def mark_failure(self):
        self.fail_count += 1
        self.total_calls += 1
        if self.fail_count >= MAX_CONSECUTIVE_FAILS:
            self.cooldown_until = time.time() + API_COOLDOWN

    def __repr__(self):
        if self.available:
            state = "ok"
        else:
            state = f"cool({int(self.cooldown_until - time.time())}s)"
        return f"<{self.label} {state} ok={self.total_ok}/{self.total_calls}>"


class ShrutiAPIPool:
    def __init__(self):
        self.apis = []
        self._index = 0
        self._load()

    def _load(self):
        urls_raw = (os.environ.get("SHRUTI_API_URLS") or "").strip()
        keys_raw = (os.environ.get("SHRUTI_API_KEYS") or "").strip()

        urls = [u.strip() for u in urls_raw.split(",") if u.strip()] if urls_raw else []
        keys = [k.strip() for k in keys_raw.split(",") if k.strip()] if keys_raw else []

        if urls:
            for i, u in enumerate(urls):
                k = keys[i] if i < len(keys) else (keys[-1] if keys else "")
                if k:
                    label = f"{u} [key{i+1}:{k[:6]}...]"
                    self.apis.append(ShrutiAPI(u, k, label))

        # Fallback to legacy single API
        if not self.apis:
            u = (os.environ.get("SHRUTI_API_URL")
                 or "https://api.shrutibots.in").strip()
            k = (os.environ.get("SHRUTI_API_KEY") or "").strip()
            if u and k:
                self.apis.append(ShrutiAPI(u, k, f"{u} [legacy]"))

        print(f"[ShrutiAPI] Loaded {len(self.apis)} API(s): {self.apis}")

    @property
    def has_apis(self) -> bool:
        return bool(self.apis)

    def _ordered(self):
        n = len(self.apis)
        if n == 0:
            return []
        rotated = [self.apis[(self._index + i) % n] for i in range(n)]
        healthy = [a for a in rotated if a.available]
        cooled = [a for a in rotated if not a.available]
        return healthy + cooled

    def _advance(self):
        if self.apis:
            self._index = (self._index + 1) % len(self.apis)

    async def get_json(self, endpoint: str, params: dict, timeout: int = 25):
        if not self.apis:
            return None
        for api in self._ordered():
            p = dict(params)
            p["api_key"] = api.key
            url = f"{api.url}{endpoint}"
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(
                        url, params=p,
                        timeout=aiohttp.ClientTimeout(total=timeout),
                    ) as resp:
                        if resp.status == 200:
                            try:
                                data = await resp.json()
                            except Exception:
                                data = await resp.text()
                            api.mark_success()
                            self._advance()
                            return data
                        api.mark_failure()
            except (asyncio.TimeoutError, aiohttp.ClientError):
                api.mark_failure()
            except Exception:
                api.mark_failure()
        return None

    async def download_to_file(self, endpoint, params, file_path, timeout=300):
        if not self.apis:
            return False, None

        for api in self._ordered():
            p = dict(params)
            p["api_key"] = api.key
            url = f"{api.url}{endpoint}"

            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(
                        url, params=p,
                        timeout=aiohttp.ClientTimeout(total=timeout),
                    ) as resp:
                        if resp.status != 200:
                            api.mark_failure()
                            continue

                        written = 0
                        ok = True
                        try:
                            with open(file_path, "wb") as f:
                                async for chunk in resp.content.iter_chunked(131072):
                                    f.write(chunk)
                                    written += len(chunk)
                        except Exception:
                            ok = False

                        if (not ok or written == 0
                                or not os.path.isfile(file_path)
                                or os.path.getsize(file_path) == 0):
                            api.mark_failure()
                            if os.path.isfile(file_path):
                                try:
                                    os.remove(file_path)
                                except Exception:
                                    pass
                            continue

                        api.mark_success()
                        self._advance()
                        return True, api
            except (asyncio.TimeoutError, aiohttp.ClientError):
                api.mark_failure()
                if os.path.isfile(file_path):
                    try:
                        os.remove(file_path)
                    except Exception:
                        pass
                continue
            except Exception:
                api.mark_failure()
                if os.path.isfile(file_path):
                    try:
                        os.remove(file_path)
                    except Exception:
                        pass
                continue

        return False, None


API_POOL = ShrutiAPIPool()


# =========================================================
# Download helpers (audio / video)
# =========================================================

async def _download_media(link: str, kind: str, timeout: int) -> str:
    video_id = link.split("v=")[-1].split("&")[0] if "v=" in link else link
    if not video_id or len(video_id) < 3:
        return None

    is_audio = kind == "audio"
    external = AUDIO_DOWNLOAD_PATH if is_audio else VIDEO_DOWNLOAD_PATH
    extensions = AUDIO_EXTENSIONS if is_audio else VIDEO_EXTENSIONS

    if external:
        found = _find_external(external, video_id, extensions)
        if found:
            return found

    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    file_path = os.path.join(DOWNLOAD_DIR, f"{video_id}.{'mp3' if is_audio else 'mp4'}")
    if os.path.exists(file_path) and os.path.getsize(file_path) > 0:
        return file_path

    target = file_path
    if external:
        os.makedirs(external, exist_ok=True)
        ext = "mp3" if is_audio else "mp4"
        target = os.path.join(external, f"{video_id}.{ext}")

    ok, api_used = await API_POOL.download_to_file(
        "/download",
        {"url": video_id, "type": kind},
        target,
        timeout=timeout,
    )
    if not ok:
        return None

    if external:
        found = _find_external(external, video_id, extensions)
        if found:
            return found
    if os.path.isfile(target) and os.path.getsize(target) > 0:
        return target
    return None


async def download_song(link: str) -> str:
    return await _download_media(link, "audio", 90)


async def download_video(link: str) -> str:
    return await _download_media(link, "video", 180)


# =========================================================
# Autoplay (multi-API, keeps seed_id support)
# =========================================================

AUTOPLAY_REQUEST_TIMEOUT = 20
AUTOPLAY_MAX_RETRIES = 3
AUTOPLAY_RETRY_DELAY = 1


async def get_autoplay(
    video_id: str,
    timeout: int = AUTOPLAY_REQUEST_TIMEOUT,
    retries: int = AUTOPLAY_MAX_RETRIES,
    seed_id: str = None,
) -> list:
    video_id = video_id.split("v=")[-1].split("&")[0] if "v=" in video_id else video_id
    if not video_id or len(video_id) < 3:
        return []

    params = {"video_id": video_id}
    if seed_id and seed_id != video_id:
        params["seed_id"] = seed_id

    for attempt in range(retries):
        data = await API_POOL.get_json(
            "/autoplay",
            params,
            timeout=timeout,
        )
        if isinstance(data, dict):
            tracks = data.get("tracks") or []
            if tracks:
                return tracks
        if attempt < retries - 1:
            await asyncio.sleep(AUTOPLAY_RETRY_DELAY)
    return []


# =========================================================
# YouTubeAPI — same interface as before
# =========================================================

class YouTubeAPI:
    def __init__(self):
        self.base = "https://www.youtube.com/watch?v="
        self.regex = r"(?:youtube\.com|youtu\.be)"
        self.status = "https://www.youtube.com/oembed?url="
        self.listbase = "https://youtube.com/playlist?list="
        self.reg = re.compile(r"\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])")

    async def exists(self, link: str, videoid: Union[bool, str] = None):
        if videoid:
            link = self.base + link
        return bool(re.search(self.regex, link))

    async def url(self, message_1: Message) -> Union[str, None]:
        messages = [message_1]
        if message_1.reply_to_message:
            messages.append(message_1.reply_to_message)
        for message in messages:
            if message.entities:
                for entity in message.entities:
                    if entity.type == MessageEntityType.URL:
                        text = message.text or message.caption
                        return text[entity.offset: entity.offset + entity.length]
            elif message.caption_entities:
                for entity in message.caption_entities:
                    if entity.type == MessageEntityType.TEXT_LINK:
                        return entity.url
        return None

    async def details(self, link: str, videoid: Union[bool, str] = None):
        if videoid:
            link = self.base + link
        if "&" in link:
            link = link.split("&")[0]
        results = VideosSearch(link, limit=1)
        for result in (await results.next())["result"]:
            title = result["title"]
            duration_min = result["duration"]
            thumbnail = result["thumbnails"][0]["url"].split("?")[0]
            vidid = result["id"]
            duration_sec = int(time_to_seconds(duration_min)) if duration_min else 0
        return title, duration_min, duration_sec, thumbnail, vidid

    async def title(self, link: str, videoid: Union[bool, str] = None):
        if videoid:
            link = self.base + link
        if "&" in link:
            link = link.split("&")[0]
        results = VideosSearch(link, limit=1)
        for result in (await results.next())["result"]:
            return result["title"]

    async def duration(self, link: str, videoid: Union[bool, str] = None):
        if videoid:
            link = self.base + link
        if "&" in link:
            link = link.split("&")[0]
        results = VideosSearch(link, limit=1)
        for result in (await results.next())["result"]:
            return result["duration"]

    async def thumbnail(self, link: str, videoid: Union[bool, str] = None):
        if videoid:
            link = self.base + link
        if "&" in link:
            link = link.split("&")[0]
        results = VideosSearch(link, limit=1)
        for result in (await results.next())["result"]:
            return result["thumbnails"][0]["url"].split("?")[0]

    async def video(self, link: str, videoid: Union[bool, str] = None):
        if videoid:
            link = self.base + link
        if "&" in link:
            link = link.split("&")[0]
        try:
            downloaded_file = await download_video(link)
            if downloaded_file:
                return 1, downloaded_file
            return 0, "Video download failed"
        except Exception as e:
            return 0, f"Video download error: {e}"

    async def playlist(self, link, limit, user_id, videoid: Union[bool, str] = None):
        if videoid:
            link = self.listbase + link
        if "&" in link:
            link = link.split("&")[0]
        try:
            plist = await Playlist.get(link)
        except Exception:
            return []
        videos = plist.get("videos") or []
        ids = []
        for data in videos[:limit]:
            if not data:
                continue
            vid = data.get("id")
            if not vid:
                continue
            ids.append(vid)
        return ids

    async def track(self, link: str, videoid: Union[bool, str] = None):
        if videoid:
            link = self.base + link
        if "&" in link:
            link = link.split("&")[0]
        results = VideosSearch(link, limit=1)
        for result in (await results.next())["result"]:
            title = result["title"]
            duration_min = result["duration"]
            vidid = result["id"]
            yturl = result["link"]
            thumbnail = result["thumbnails"][0]["url"].split("?")[0]
        track_details = {
            "title": title,
            "link": yturl,
            "vidid": vidid,
            "duration_min": duration_min,
            "thumb": thumbnail,
        }
        return track_details, vidid

    async def formats(self, link: str, videoid: Union[bool, str] = None):
        if videoid:
            link = self.base + link
        if "&" in link:
            link = link.split("&")[0]
        ytdl_opts = {"quiet": True}
        ydl = yt_dlp.YoutubeDL(ytdl_opts)
        with ydl:
            formats_available = []
            r = ydl.extract_info(link, download=False)
            for format in r["formats"]:
                try:
                    if "dash" not in str(format["format"]).lower():
                        formats_available.append(
                            {
                                "format": format["format"],
                                "filesize": format.get("filesize"),
                                "format_id": format["format_id"],
                                "ext": format["ext"],
                                "format_note": format["format_note"],
                                "yturl": link,
                            }
                        )
                except Exception:
                    continue
        return formats_available, link

    async def slider(self, link: str, query_type: int, videoid: Union[bool, str] = None):
        if videoid:
            link = self.base + link
        if "&" in link:
            link = link.split("&")[0]
        a = VideosSearch(link, limit=10)
        result = (await a.next()).get("result")
        title = result[query_type]["title"]
        duration_min = result[query_type]["duration"]
        vidid = result[query_type]["id"]
        thumbnail = result[query_type]["thumbnails"][0]["url"].split("?")[0]
        return title, duration_min, thumbnail, vidid

    async def download(
        self,
        link: str,
        mystic,
        video: Union[bool, str] = None,
        videoid: Union[bool, str] = None,
        songaudio: Union[bool, str] = None,
        songvideo: Union[bool, str] = None,
        format_id: Union[bool, str] = None,
        title: Union[bool, str] = None,
    ) -> str:
        if videoid:
            link = self.base + link
        try:
            if video:
                downloaded_file = await download_video(link)
            else:
                downloaded_file = await download_song(link)
            if downloaded_file:
                return downloaded_file, True
            return None, False
        except Exception:
            return None, False


YouTube = YouTubeAPI()
