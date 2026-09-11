#!/usr/bin/env python3
"""
GrabTube — studio downloader.

  pip install fastapi uvicorn yt-dlp websockets
  python app.py

Optional: put a .env file next to app.py to configure without CLI args.
Docker:  docker run -p 8000:8000 -v grabtube:/tmp/grabtube <image>
"""
from __future__ import annotations
import argparse, asyncio, json, logging, os, re, shlex, shutil, sqlite3
import sys, tempfile, threading, time, uuid, zipfile
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse

import uvicorn, yt_dlp
from fastapi import (FastAPI, HTTPException, BackgroundTasks, WebSocket,
                     WebSocketDisconnect, Request, Response as FResponse)
from fastapi.responses import (HTMLResponse, FileResponse, JSONResponse,
                               StreamingResponse, Response, PlainTextResponse)
from pydantic import BaseModel, field_validator

try:
    from yt_dlp.utils import DownloadCancelled
except ImportError:
    class DownloadCancelled(Exception): ...

# ─────────────────────────────────────────────────────────────
# .env loader (tiny, no dependency)
# ─────────────────────────────────────────────────────────────
def _load_env():
    p = Path(__file__).parent / ".env"
    if not p.exists(): return
    for line in p.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line: continue
        k, _, v = line.partition("=")
        k = k.strip(); v = v.strip().strip('"').strip("'")
        os.environ.setdefault(k, v)
_load_env()

# ─────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────
BASE_DIR = Path(os.environ.get("GRABTUBE_DIR") or (Path(tempfile.gettempdir()) / "grabtube"))
BASE_DIR.mkdir(parents=True, exist_ok=True)
LIBRARY_DIR = BASE_DIR / "library"
LIBRARY_DIR.mkdir(exist_ok=True)
THUMB_DIR = BASE_DIR / "thumbs"
THUMB_DIR.mkdir(exist_ok=True)
DB_PATH = BASE_DIR / "grabtube.db"

DEFAULT_ALLOWED = {"youtube.com","www.youtube.com","m.youtube.com",
                   "music.youtube.com","youtu.be","www.youtu.be"}
ALLOW_ANY_URL = os.environ.get("GRABTUBE_ALLOW_ANY", "").lower() in ("1","true","yes")
ALLOWED_HOSTS = set(DEFAULT_ALLOWED)
if os.environ.get("GRABTUBE_EXTRA_HOSTS"):
    ALLOWED_HOSTS.update(h.strip() for h in os.environ["GRABTUBE_EXTRA_HOSTS"].split(",") if h.strip())

BASE_PATH = os.environ.get("GRABTUBE_BASE_PATH", "").rstrip("/")
MAX_CONCURRENT = int(os.environ.get("GRABTUBE_CONCURRENCY", "4"))
JOB_RETENTION = 3600
FILE_RETENTION = 3600
CLEANUP_EVERY = 300
MAX_FILENAME = 140
SUB_CHECK_INTERVAL = 900  # 15 min minimum between subscription checks
JSON_LOGS = os.environ.get("GRABTUBE_JSON_LOGS", "").lower() in ("1","true","yes")

if JSON_LOGS:
    class _J(logging.Formatter):
        def format(self, r):
            return json.dumps({"t": time.time(), "lvl": r.levelname,
                               "msg": r.getMessage()})
    h = logging.StreamHandler()
    h.setFormatter(_J())
    logging.basicConfig(level=logging.INFO, handlers=[h])
else:
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname).1s %(message)s",
                        datefmt="%H:%M:%S")
log = logging.getLogger("grabtube")

# ─────────────────────────────────────────────────────────────
# DB
# ─────────────────────────────────────────────────────────────
_db_lock = threading.Lock()
_db = sqlite3.connect(DB_PATH, check_same_thread=False)
_db.row_factory = sqlite3.Row
_db.execute("PRAGMA journal_mode=WAL")
_db.execute("PRAGMA synchronous=NORMAL")
_db.execute("PRAGMA foreign_keys=ON")

_db.executescript("""
CREATE TABLE IF NOT EXISTS schema_version (v INTEGER PRIMARY KEY);

CREATE TABLE IF NOT EXISTS history (
    job_id TEXT PRIMARY KEY, url TEXT NOT NULL, video_id TEXT,
    title TEXT, uploader TEXT, duration INTEGER, thumbnail TEXT,
    status TEXT NOT NULL, format_label TEXT, file_path TEXT,
    file_size INTEGER, error TEXT, payload TEXT, keep INTEGER DEFAULT 0,
    watched INTEGER DEFAULT 0, resume_seconds REAL DEFAULT 0,
    rating INTEGER, tags TEXT, notes TEXT, bookmarks TEXT,
    created_at REAL NOT NULL, finished_at REAL
);
CREATE TABLE IF NOT EXISTS favorites (
    video_id TEXT PRIMARY KEY, url TEXT NOT NULL, title TEXT,
    uploader TEXT, duration INTEGER, thumbnail TEXT, created_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS presets (
    id TEXT PRIMARY KEY, name TEXT NOT NULL, payload TEXT NOT NULL,
    created_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS profiles (
    id TEXT PRIMARY KEY, name TEXT NOT NULL, config TEXT NOT NULL,
    is_default INTEGER DEFAULT 0, created_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS subscriptions (
    id TEXT PRIMARY KEY, url TEXT NOT NULL, kind TEXT, title TEXT,
    preset TEXT DEFAULT '720', profile_id TEXT, organize_by TEXT,
    check_every INTEGER DEFAULT 3600, last_check REAL, last_new REAL,
    enabled INTEGER DEFAULT 1, created_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS sub_seen (
    sub_id TEXT, video_id TEXT, seen_at REAL,
    PRIMARY KEY (sub_id, video_id)
);
CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT);
CREATE TABLE IF NOT EXISTS errors (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id TEXT, message TEXT, trace TEXT, created_at REAL
);
""")

def _cols(t):
    return {r[1] for r in _db.execute(f"PRAGMA table_info({t})").fetchall()}
def _ensure(t, cols):
    have = _cols(t)
    for n, ty in cols.items():
        if n not in have:
            _db.execute(f"ALTER TABLE {t} ADD COLUMN {n} {ty}")
_ensure("history", {"video_id":"TEXT","title":"TEXT","uploader":"TEXT",
    "duration":"INTEGER","thumbnail":"TEXT","format_label":"TEXT",
    "file_path":"TEXT","file_size":"INTEGER","error":"TEXT","payload":"TEXT",
    "keep":"INTEGER DEFAULT 0","watched":"INTEGER DEFAULT 0",
    "resume_seconds":"REAL DEFAULT 0","rating":"INTEGER","tags":"TEXT",
    "notes":"TEXT","bookmarks":"TEXT","finished_at":"REAL"})
_ensure("favorites", {"title":"TEXT","uploader":"TEXT","duration":"INTEGER","thumbnail":"TEXT"})
_ensure("presets", {"payload":"TEXT","created_at":"REAL"})
_db.execute("CREATE INDEX IF NOT EXISTS idx_created ON history(created_at DESC)")
_db.execute("CREATE INDEX IF NOT EXISTS idx_video ON history(video_id)")
_db.execute("CREATE INDEX IF NOT EXISTS idx_keep ON history(keep)")
_db.execute("CREATE INDEX IF NOT EXISTS idx_status ON history(status)")
_db.commit()

def db_upsert(jid, **f):
    cols = ", ".join(f"{k}=?" for k in f)
    with _db_lock:
        cur = _db.execute(f"UPDATE history SET {cols} WHERE job_id=?", (*f.values(), jid))
        if cur.rowcount == 0:
            keys = ["job_id", *f.keys()]
            ph = ",".join("?"*len(keys))
            _db.execute(f"INSERT INTO history ({','.join(keys)}) VALUES ({ph})",
                        (jid, *f.values()))
        _db.commit()

def db_list(limit=60, q=None, status=None, sort=None, keep_only=False, tag=None):
    w, a = [], []
    if q: w.append("(title LIKE ? OR url LIKE ? OR uploader LIKE ?)"); a += [f"%{q}%"]*3
    if status: w.append("status = ?"); a.append(status)
    if keep_only: w.append("keep = 1")
    if tag: w.append("tags LIKE ?"); a.append(f'%"{tag}"%')
    where = ("WHERE " + " AND ".join(w)) if w else ""
    order = "created_at DESC"
    if sort == "size": order = "COALESCE(file_size,0) DESC"
    if sort == "oldest": order = "created_at ASC"
    if sort == "rating": order = "COALESCE(rating,0) DESC"
    with _db_lock:
        rows = _db.execute(
            f"SELECT * FROM history {where} ORDER BY {order} LIMIT ?",
            (*a, max(1, min(limit, 500)))).fetchall()
    return [dict(r) for r in rows]

def db_get(jid):
    with _db_lock:
        r = _db.execute("SELECT * FROM history WHERE job_id=?", (jid,)).fetchone()
    return dict(r) if r else None

def db_delete(jid):
    with _db_lock:
        _db.execute("DELETE FROM history WHERE job_id=?", (jid,)); _db.commit()

def db_patch(jid, **fields):
    if not fields: return
    cols = ", ".join(f"{k}=?" for k in fields)
    with _db_lock:
        _db.execute(f"UPDATE history SET {cols} WHERE job_id=?",
                    (*fields.values(), jid))
        _db.commit()

def has_video(vid):
    if not vid: return None
    with _db_lock:
        r = _db.execute("SELECT job_id FROM history WHERE video_id=? AND status='done' "
                        "ORDER BY created_at DESC LIMIT 1", (vid,)).fetchone()
    return dict(r)["job_id"] if r else None

def fav_toggle(v):
    vid = v.get("id")
    if not vid: return False
    with _db_lock:
        if _db.execute("SELECT 1 FROM favorites WHERE video_id=?", (vid,)).fetchone():
            _db.execute("DELETE FROM favorites WHERE video_id=?", (vid,)); _db.commit()
            return False
        _db.execute("""INSERT INTO favorites (video_id,url,title,uploader,duration,thumbnail,created_at)
          VALUES (?,?,?,?,?,?,?)""",
          (vid, v.get("url") or f"https://www.youtube.com/watch?v={vid}",
           v.get("title"), v.get("uploader"), v.get("duration"),
           v.get("thumbnail"), time.time()))
        _db.commit(); return True

def fav_list():
    with _db_lock:
        return [dict(r) for r in _db.execute(
            "SELECT * FROM favorites ORDER BY created_at DESC").fetchall()]

def fav_ids():
    with _db_lock:
        return {r["video_id"] for r in _db.execute("SELECT video_id FROM favorites").fetchall()}

def fav_delete(vid):
    with _db_lock:
        _db.execute("DELETE FROM favorites WHERE video_id=?", (vid,)); _db.commit()

def preset_list():
    with _db_lock:
        return [dict(r) for r in _db.execute("SELECT * FROM presets ORDER BY created_at DESC").fetchall()]
def preset_save(name, payload):
    pid = uuid.uuid4().hex[:10]
    with _db_lock:
        _db.execute("INSERT INTO presets (id,name,payload,created_at) VALUES (?,?,?,?)",
                    (pid, name, json.dumps(payload), time.time())); _db.commit()
    return pid
def preset_delete(pid):
    with _db_lock:
        _db.execute("DELETE FROM presets WHERE id=?", (pid,)); _db.commit()

def profile_list():
    with _db_lock:
        rows = [dict(r) for r in _db.execute("SELECT * FROM profiles ORDER BY created_at ASC").fetchall()]
    for r in rows:
        try: r["config"] = json.loads(r["config"])
        except Exception: r["config"] = {}
    return rows
def profile_save(name, config, is_default=False):
    pid = uuid.uuid4().hex[:10]
    with _db_lock:
        if is_default:
            _db.execute("UPDATE profiles SET is_default=0")
        _db.execute("INSERT INTO profiles (id,name,config,is_default,created_at) VALUES (?,?,?,?,?)",
                    (pid, name, json.dumps(config), 1 if is_default else 0, time.time()))
        _db.commit()
    return pid
def profile_delete(pid):
    with _db_lock:
        _db.execute("DELETE FROM profiles WHERE id=?", (pid,)); _db.commit()

def settings_all():
    with _db_lock:
        return {r["key"]: r["value"] for r in _db.execute("SELECT * FROM settings").fetchall()}
def settings_set(k, v):
    with _db_lock:
        _db.execute("INSERT INTO settings (key,value) VALUES (?,?) "
                    "ON CONFLICT(key) DO UPDATE SET value=excluded.value", (k, v))
        _db.commit()

def sub_list():
    with _db_lock:
        return [dict(r) for r in _db.execute("SELECT * FROM subscriptions ORDER BY created_at DESC").fetchall()]
def sub_get(sid):
    with _db_lock:
        r = _db.execute("SELECT * FROM subscriptions WHERE id=?", (sid,)).fetchone()
    return dict(r) if r else None
def sub_save(url, kind, title, preset, profile_id, organize_by, check_every):
    sid = uuid.uuid4().hex[:10]
    with _db_lock:
        _db.execute("""INSERT INTO subscriptions
          (id,url,kind,title,preset,profile_id,organize_by,check_every,created_at)
          VALUES (?,?,?,?,?,?,?,?,?)""",
          (sid, url, kind, title, preset, profile_id, organize_by, check_every, time.time()))
        _db.commit()
    return sid
def sub_delete(sid):
    with _db_lock:
        _db.execute("DELETE FROM subscriptions WHERE id=?", (sid,))
        _db.execute("DELETE FROM sub_seen WHERE sub_id=?", (sid,))
        _db.commit()
def sub_toggle(sid, enabled):
    with _db_lock:
        _db.execute("UPDATE subscriptions SET enabled=? WHERE id=?",
                    (1 if enabled else 0, sid)); _db.commit()
def sub_seen_vids(sid):
    with _db_lock:
        return {r["video_id"] for r in _db.execute(
            "SELECT video_id FROM sub_seen WHERE sub_id=?", (sid,)).fetchall()}
def sub_mark_seen(sid, vids):
    if not vids: return
    with _db_lock:
        _db.executemany("INSERT OR IGNORE INTO sub_seen (sub_id,video_id,seen_at) VALUES (?,?,?)",
                        [(sid, v, time.time()) for v in vids])
        _db.commit()

def stats_payload():
    with _db_lock:
        t = _db.execute("SELECT COUNT(*) c, COALESCE(SUM(file_size),0) s FROM history WHERE status='done'").fetchone()
        days = _db.execute("""SELECT strftime('%Y-%m-%d', created_at,'unixepoch','localtime') d,
          COUNT(*) c, COALESCE(SUM(file_size),0) s FROM history
          WHERE status='done' GROUP BY d ORDER BY d DESC LIMIT 30""").fetchall()
        kinds = _db.execute("""SELECT format_label k, COUNT(*) c FROM history
          WHERE status='done' GROUP BY k ORDER BY c DESC LIMIT 10""").fetchall()
        hourly = _db.execute("""SELECT strftime('%w %H', created_at,'unixepoch','localtime') w,
          COUNT(*) c FROM history WHERE status='done' GROUP BY w""").fetchall()
        all_tags = {}
        for r in _db.execute("SELECT tags FROM history WHERE tags IS NOT NULL").fetchall():
            try:
                for tg in json.loads(r["tags"]):
                    all_tags[tg] = all_tags.get(tg, 0) + 1
            except Exception: pass
    return {"total_count": t["c"], "total_size": t["s"],
            "by_day": [dict(r) for r in reversed(days)],
            "by_kind": [dict(r) for r in kinds],
            "heatmap": [dict(r) for r in hourly],
            "tags": sorted(all_tags.items(), key=lambda x: -x[1])}

def log_error(jid, msg, trace=""):
    try:
        with _db_lock:
            _db.execute("INSERT INTO errors (job_id,message,trace,created_at) VALUES (?,?,?,?)",
                        (jid, msg, trace[:2000], time.time()))
            _db.commit()
    except Exception: pass

# ─────────────────────────────────────────────────────────────
# WS
# ─────────────────────────────────────────────────────────────
class WSMan:
    GLOBAL = "__all__"
    def __init__(self):
        self.subs = defaultdict(set); self._lock = asyncio.Lock()
        self._loop = None; self._last = {}
    def bind(self, loop): self._loop = loop
    async def sub(self, key, ws):
        await ws.accept()
        async with self._lock: self.subs[key].add(ws)
    def unsub(self, key, ws):
        self.subs.get(key, set()).discard(ws)
        if not self.subs.get(key): self.subs.pop(key, None)
    def pub(self, key, msg, th=0):
        if not self._loop: return
        now = time.time()*1000
        tk = f"{key}:{msg.get('type','')}"
        if th and now - self._last.get(tk,0) < th: return
        self._last[tk] = now
        asyncio.run_coroutine_threadsafe(self._send(key, msg), self._loop)
        if key != self.GLOBAL:
            asyncio.run_coroutine_threadsafe(self._send(self.GLOBAL, msg), self._loop)
    async def _send(self, key, msg):
        async with self._lock: subs = list(self.subs.get(key, ()))
        dead = []
        for ws in subs:
            try: await ws.send_json(msg)
            except Exception: dead.append(ws)
        for ws in dead: self.unsub(key, ws)
    def bcast(self, msg, th=0): self.pub(self.GLOBAL, msg, th)

ws = WSMan()
JOBS = {}
JOBS_LOCK = threading.Lock()
CANCELS = {}
SEM = threading.Semaphore(MAX_CONCURRENT)
PAUSED = threading.Event()
SESSION = {"bytes": 0, "count": 0, "started": time.time()}
UNDO_STACK = []  # list of {kind, payload, expires}

def session_snapshot():
    return {"bytes": SESSION["bytes"], "count": SESSION["count"],
            "uptime": time.time() - SESSION["started"]}

def push_undo(kind, payload, ttl=8):
    item = {"id": uuid.uuid4().hex[:8], "kind": kind, "payload": payload,
            "expires": time.time() + ttl}
    UNDO_STACK.append(item)
    if len(UNDO_STACK) > 20: UNDO_STACK.pop(0)
    ws.bcast({"type": "undo_available", "undo": item})
    return item["id"]

# ─────────────────────────────────────────────────────────────
# MODELS
# ─────────────────────────────────────────────────────────────
class InfoReq(BaseModel): url: str
class SearchReq(BaseModel): q: str; limit: int = 24
class ChannelReq(BaseModel): url: str; limit: int = 60
class BatchReq(BaseModel):
    urls: list[str]; preset: str = "720"
    audio_format: Optional[str] = None
    organize_by: Optional[str] = None
    skip_dupes: bool = True
    profile_id: Optional[str] = None
class PresetReq(BaseModel): name: str; payload: dict
class ProfileReq(BaseModel): name: str; config: dict; is_default: bool = False
class PatchReq(BaseModel):
    watched: Optional[bool] = None
    resume_seconds: Optional[float] = None
    rating: Optional[int] = None
    tags: Optional[list[str]] = None
    notes: Optional[str] = None
    bookmarks: Optional[list[dict]] = None
    keep: Optional[bool] = None
class SubReq(BaseModel):
    url: str
    kind: str = "channel"
    preset: str = "720"
    profile_id: Optional[str] = None
    organize_by: Optional[str] = None
    check_every: int = 3600
class UndoReq(BaseModel): id: str
class QueueReorderReq(BaseModel): order: list[str]

class DownloadReq(BaseModel):
    url: str
    format_id: Optional[str] = None
    kind: Optional[str] = None
    preset: Optional[str] = None
    audio_format: Optional[str] = None
    video_container: Optional[str] = None
    quality: Optional[int] = None
    subtitles: bool = False
    subtitle_langs: list[str] = ["en.*"]
    sub_files: bool = False
    subtitle_format: Optional[str] = None  # srt|vtt|ass|best
    auto_subs: bool = False
    embed_subs: bool = True
    chapters: bool = False
    split_chapters: bool = False
    chapter_ranges: Optional[list[dict]] = None
    chapter_thumbs: bool = False
    trim_start: Optional[float] = None
    trim_end: Optional[float] = None
    sponsorblock: bool = False
    sponsorblock_categories: list[str] = ["sponsor","selfpromo","interaction"]
    embed_metadata: bool = True
    embed_thumbnail: bool = True
    loudnorm: bool = False
    live_from_start: bool = False
    write_info_json: bool = False
    cookies_from_browser: Optional[str] = None
    cookies_file: Optional[str] = None
    rate_limit: Optional[str] = None
    proxy: Optional[str] = None
    sort: Optional[str] = None
    playlist_items: Optional[str] = None
    filename_template: Optional[str] = None
    organize_by: Optional[str] = None
    extra_args: Optional[str] = None
    skip_dupes: bool = True
    keep: bool = False

    @field_validator("kind")
    @classmethod
    def _k(cls, v):
        if v not in (None, "combined", "video", "audio"): raise ValueError("bad kind")
        return v
    @field_validator("audio_format")
    @classmethod
    def _a(cls, v):
        if v not in (None, "mp3","opus","m4a","flac","wav","best"): raise ValueError("bad audio_format")
        return v
    @field_validator("video_container")
    @classmethod
    def _c(cls, v):
        if v not in (None, "mp4","mkv","webm"): raise ValueError("bad container")
        return v

class PlaylistReq(BaseModel):
    urls: list[str]; preset: str = "720"
    audio_format: Optional[str] = None
    subtitles: bool = False
    subtitle_langs: list[str] = ["en.*"]
    sponsorblock: bool = False
    cookies_from_browser: Optional[str] = None
    cookies_file: Optional[str] = None
    playlist_items: Optional[str] = None

# ─────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────
CODECS = {"avc1":"H.264","avc3":"H.264","hev1":"H.265","hvc1":"H.265",
          "vp09":"VP9","vp9":"VP9","av01":"AV1","mp4a":"AAC","opus":"Opus",
          "vorbis":"Vorbis","ac-3":"AC-3","ec-3":"E-AC-3","flac":"FLAC","mp3":"MP3"}
def codec_name(c):
    if not c or c == "none": return None
    b = c.split(".")[0].lower()
    return CODECS.get(b, b.upper())

def classify(f):
    v, a = f.get("vcodec") or "none", f.get("acodec") or "none"
    if v != "none" and a != "none": return "combined"
    if v != "none": return "video"
    if a != "none": return "audio"
    return "other"

def human_size(n):
    if not n: return ""
    f = float(n)
    for u in ("B","KB","MB","GB"):
        if f < 1024: return f"{f:.1f} {u}"
        f /= 1024
    return f"{f:.1f} TB"

def clean_formats(info):
    out = []
    for f in info.get("formats", []):
        if f.get("format_note") == "storyboard": continue
        if not f.get("format_id"): continue
        k = classify(f)
        if k == "other": continue
        dyn = (f.get("dynamic_range") or "").upper()
        hdr = "HDR" in dyn or f.get("hdr") is True
        out.append({"format_id":f["format_id"],"ext":f.get("ext"),
            "resolution": f.get("resolution") or f.get("format_note") or
                          (f"{f.get('height')}p" if f.get("height") else "audio"),
            "height":f.get("height"),"fps":f.get("fps"),
            "vcodec":codec_name(f.get("vcodec")),"acodec":codec_name(f.get("acodec")),
            "abr":f.get("abr"),"vbr":f.get("vbr"),"tbr":f.get("tbr"),
            "filesize":f.get("filesize") or f.get("filesize_approx"),
            "filesize_str":human_size(f.get("filesize") or f.get("filesize_approx")),
            "kind":k,"note":f.get("format_note") or "","language":f.get("language"),
            "hdr":hdr})
    o = {"combined":0,"video":1,"audio":2}
    out.sort(key=lambda x: (o[x["kind"]], -(x.get("height") or 0), -(x.get("tbr") or 0)))
    return out

def clean_chapters(info):
    return [{"start_time":c.get("start_time"),"end_time":c.get("end_time"),
             "title":c.get("title") or ""} for c in info.get("chapters") or []]

def clean_subs(info):
    man, auto = {}, {}
    for lang, tracks in (info.get("subtitles") or {}).items():
        man[lang] = [{"ext":t.get("ext"),"name":t.get("name")} for t in tracks]
    for lang, tracks in (info.get("automatic_captions") or {}).items():
        if lang in man: continue
        auto[lang] = [{"ext":t.get("ext")} for t in tracks]
    return {"manual": man, "auto": auto}

def safe_filename(name, max_len=MAX_FILENAME):
    name = re.sub(r'[\\/:*?"<>|\x00-\x1f]', "_", name or "download").strip(". ")
    if len(name) > max_len:
        b, e = os.path.splitext(name)
        name = b[:max(1, max_len-len(e))].rstrip(". ") + e
    return name or "download"

def validate_url(url):
    url = (url or "").strip()
    if not url: raise HTTPException(400, "empty URL")
    try: p = urlparse(url)
    except Exception: raise HTTPException(400, "malformed")
    if p.scheme not in ("http","https"): raise HTTPException(400, "only http(s)")
    host = (p.netloc.rsplit("@",1)[-1].split(":")[0] or "").lower()
    if not host: raise HTTPException(400, "missing host")
    if not ALLOW_ANY_URL and not any(host == h or host.endswith("."+h) for h in ALLOWED_HOSTS):
        raise HTTPException(400, f"host not allowed: {host}")
    return url

URL_RE = re.compile(r"https?://[^\s<>\"']+")
def extract_urls(text):
    return [u.rstrip(".,;:!?)\"]") for u in URL_RE.findall(text or "")]

FRIENDLY = [
    (r"private video|sign in", "private or requires sign-in"),
    (r"video unavailable", "unavailable"),
    (r"unsupported url", "unsupported URL"),
    (r"geo.{0,10}restrict", "geo-restricted"),
    (r"copyright", "blocked by copyright"),
    (r"too many requests|HTTP Error 429", "rate-limited — try later"),
    (r"age.{0,20}restrict", "age-restricted — set cookies in settings"),
    (r"ffmpeg", "ffmpeg error"),
    (r"cancell?ed", "cancelled"),
    (r"is not a valid URL|unable to extract", "could not extract this URL"),
    (r"connection.{0,20}(reset|refused|timed out)", "network error — check connection"),
    (r"no space left", "disk full"),
    (r"permission denied", "permission denied"),
]
def friendly(e):
    s = str(e)
    for pat, msg in FRIENDLY:
        if re.search(pat, s, re.I): return msg
    return (s or "unknown error")[:240]

# ─────────────────────────────────────────────────────────────
# THUMBNAIL PROXY (keeps browser from hitting ytimg every render)
# ─────────────────────────────────────────────────────────────
_thumb_lock = threading.Lock()
def cache_thumb(url):
    if not url: return None
    h = abs(hash(url))
    fname = f"{h:x}.jpg"
    dest = THUMB_DIR / fname
    if dest.exists(): return f"/api/thumb/{fname}"
    try:
        import urllib.request
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=8) as r:
            data = r.read()
        if len(data) < 200: return url
        with _thumb_lock:
            dest.write_bytes(data)
        return f"/api/thumb/{fname}"
    except Exception:
        return url

# ─────────────────────────────────────────────────────────────
# PROGRESS HOOKS
# ─────────────────────────────────────────────────────────────
class WSLog:
    __slots__ = ("job_id",)
    def __init__(self, jid): self.job_id = jid
    def _p(self, m, l="info"):
        m = (m or "").rstrip()
        if not m or m.startswith("[debug] "): return
        ws.pub(self.job_id, {"type":"log","line":m,"level":l,
                             "job_id": self.job_id, "ts": time.time()})
    def debug(self,m): self._p(m,"info")
    def info(self,m): self._p(m,"info")
    def warning(self,m): self._p(m,"warn")
    def error(self,m): self._p(m,"err")

def _mk_hooks(jid):
    cancel = CANCELS.get(jid)
    def progress(d):
        if cancel and cancel.is_set(): raise DownloadCancelled("cancelled")
        with JOBS_LOCK:
            j = JOBS.get(jid)
            if not j: return
            if d["status"] == "downloading":
                total = d.get("total_bytes") or d.get("total_bytes_estimate") or 0
                done = d.get("downloaded_bytes", 0)
                j.update(progress=(done/total*100) if total else 0.0,
                         speed=d.get("speed"), eta=d.get("eta"),
                         downloaded=done, total=total,
                         frag_index=d.get("fragment_index"),
                         frag_count=d.get("fragment_count"),
                         stage="downloading")
            elif d["status"] == "finished":
                j.update(progress=100.0, stage="processing")
            snap = dict(j)
        ws.pub(jid, {"type":"job_progress","job":snap}, th=120)
    def postproc(d):
        if cancel and cancel.is_set(): raise DownloadCancelled("cancelled")
        with JOBS_LOCK:
            if jid in JOBS:
                JOBS[jid]["stage"] = "processing"
                snap = dict(JOBS[jid])
            else: snap = None
        if snap: ws.pub(jid, {"type":"job_progress","job":snap}, th=120)
    return progress, postproc

def _finalize(ydl, info):
    p = ydl.prepare_filename(info)
    if os.path.exists(p): return p
    b, _ = os.path.splitext(p)
    for ext in (".mp4",".mkv",".webm",".mp3",".m4a",".opus",".flac",".wav",".ogg"):
        if os.path.exists(b+ext): return b+ext
    return p

# ─────────────────────────────────────────────────────────────
# OPTION BUILDERS
# ─────────────────────────────────────────────────────────────
def _base():
    return {"quiet":True,"no_warnings":True,"noplaylist":True,
            "retries":5,"fragment_retries":5,
            "trim_file_name":MAX_FILENAME,"windowsfilenames":True,
            "noprogress":True,"concurrent_fragment_downloads":4,
            "continuedl":True,"overwrites":False,
            "ignoreerrors":False}

def _rate(s):
    s = (s or "").strip().upper()
    m = re.match(r"^(\d+(?:\.\d+)?)\s*([KMG]?)B?$", s)
    if not m: return None
    return int(float(m.group(1)) * {"":1,"K":1024,"M":1024**2,"G":1024**3}[m.group(2)])

def _net(o, rl=None, px=None, ckb=None, ckf=None):
    if rl:
        r = _rate(rl)
        if r: o["ratelimit"] = r
    if px: o["proxy"] = px
    if ckf and os.path.exists(ckf): o["cookiefile"] = ckf
    if ckb: o["cookiesfrombrowser"] = (ckb,)

def _qfmt(q, sort):
    if sort: return sort
    if q: return f"bestvideo[height<={q}]+bestaudio/best[height<={q}]/best"
    return "bestvideo+bestaudio/best"

def _template(organize_by, filename_template):
    base = filename_template or "%(title).120s.%(ext)s"
    if not base.endswith("%(ext)s"):
        base = base.rstrip(".") + ".%(ext)s"
    if organize_by == "uploader":
        return os.path.join("%(uploader).60s", base)
    if organize_by == "date":
        return os.path.join("%(upload_date>%Y)s", "%(upload_date>%Y-%m)s", base)
    if organize_by == "both":
        return os.path.join("%(uploader).60s", "%(upload_date>%Y)s", base)
    return base

def build_opts(**kw):
    o = _base()
    _net(o, kw.get("rate_limit"), kw.get("proxy"),
         kw.get("cookies_from_browser"), kw.get("cookies_file"))
    if kw.get("playlist_items"): o["playlist_items"] = kw["playlist_items"]
    if kw.get("live_from_start"): o["live_from_start"] = True
    if kw.get("write_info_json"): o["writeinfojson"] = True

    is_audio = bool(kw.get("audio_format")) or kw.get("preset") == "mp3"
    pps = []

    if is_audio:
        codec = kw.get("audio_format")
        if codec == "best": codec = None
        if kw.get("preset") == "mp3" and not codec: codec = "mp3"
        o["format"] = "bestaudio/best"
        if codec:
            pps.append({"key":"FFmpegExtractAudio","preferredcodec":codec,
                        "preferredquality":"0" if codec=="flac" else "192"})
    else:
        o["format"] = _qfmt(kw.get("quality"), kw.get("sort"))
        o["merge_output_format"] = kw.get("video_container") or "mp4"

    ranges = None
    if kw.get("chapter_ranges"):
        ranges = list(kw["chapter_ranges"])
    elif kw.get("trim_start") is not None and kw.get("trim_end") is not None:
        ranges = [{"start_time": float(kw["trim_start"]),
                   "end_time": float(kw["trim_end"])}]
    if ranges:
        o["download_ranges"] = lambda info, ydl: ranges
        o["force_keyframes_at_cuts"] = True

    if kw.get("loudnorm"):
        o["postprocessor_args"] = {"ffmpeg": ["-af", "loudnorm=I=-16:TP=-1.5:LRA=11"]}

    if kw.get("sponsorblock"):
        cats = kw.get("sponsorblock_categories") or ["sponsor","selfpromo","interaction"]
        o["sponsorblock_remove"] = cats
        pps.append({"key":"SponsorBlock","categories":cats,"when":"after_filter"})
        pps.append({"key":"ModifyChapters","remove_sponsor_segments":cats})

    split = kw.get("chapters") and kw.get("split_chapters") and not ranges
    if split:
        o["writethumbnail"] = True
        pps.append({"key":"FFmpegSplitChapters"})

    if kw.get("subtitles"):
        o["writesubtitles"] = True
        o["subtitleslangs"] = kw.get("subtitle_langs") or ["en.*"]
        o["writeautomaticsub"] = bool(kw.get("auto_subs"))
        fmt = kw.get("subtitle_format")
        if fmt and fmt != "best":
            o["subtitlesformat"] = fmt
        if kw.get("embed_subs", True) and not kw.get("sub_files"):
            pps.append({"key":"FFmpegEmbedSubtitle"})

    if kw.get("embed_metadata", True):
        pps.append({"key":"FFmpegMetadata",
                    "add_chapters":bool(kw.get("chapters")), "add_metadata":True})
    if kw.get("embed_thumbnail", True) and not split:
        o["writethumbnail"] = True
        pps.append({"key":"EmbedThumbnail","already_have_thumbnail":False})

    if pps: o["postprocessors"] = pps

    # custom yt-dlp args (whitelisted keys only for safety)
    extra = kw.get("extra_args")
    if extra:
        try:
            tokens = shlex.split(extra)
            i = 0
            while i < len(tokens):
                t = tokens[i]
                if t.startswith("--") and i + 1 < len(tokens) and not tokens[i+1].startswith("--"):
                    key = t[2:].replace("-", "_")
                    if key in o: o[key] = tokens[i+1]
                    i += 2
                elif t.startswith("--"):
                    key = t[2:].replace("-", "_")
                    if key in o: o[key] = True
                    i += 1
                else: i += 1
        except Exception as e:
            log.warning("extra_args parse failed: %s", e)
    return o

def build_fmt_opts(fid, kind, extra=None):
    o = _base()
    if kind == "audio": o["format"] = fid
    elif kind == "combined":
        o["format"] = fid; o["merge_output_format"] = "mp4"
    else:
        o["format"] = f"{fid}+bestaudio/{fid}"; o["merge_output_format"] = "mp4"
    if extra: _net(o, extra.get("rate_limit"), extra.get("proxy"),
                   extra.get("cookies_from_browser"), extra.get("cookies_file"))
    return o

# ─────────────────────────────────────────────────────────────
# WEBHOOK
# ─────────────────────────────────────────────────────────────
def fire_webhook(payload):
    wh = settings_all().get("webhook_url")
    if not wh: return
    def _go():
        try:
            import urllib.request
            req = urllib.request.Request(wh, data=json.dumps(payload).encode(),
                                          headers={"Content-Type":"application/json"})
            urllib.request.urlopen(req, timeout=8)
        except Exception as e:
            log.warning("webhook failed: %s", e)
    threading.Thread(target=_go, daemon=True).start()

# ─────────────────────────────────────────────────────────────
# DOWNLOAD WORKER
# ─────────────────────────────────────────────────────────────
def do_download(jid, url, opts, tmpl, payload=None):
    try:
        while PAUSED.is_set():
            time.sleep(0.5)
        with SEM:
            with JOBS_LOCK:
                j = JOBS.get(jid)
                if not j or j.get("status") == "cancelled": return
                j.update(status="running", stage="starting", progress=0.0)
            db_upsert(jid, status="running")
            ws.bcast({"type":"job_started","job_id":jid,
                      "title": JOBS[jid].get("title"), "url": url})
            pr, pp = _mk_hooks(jid)
            o = dict(opts); o["outtmpl"] = tmpl
            o["progress_hooks"] = [pr]; o["postprocessor_hooks"] = [pp]
            o["logger"] = WSLog(jid)
            with yt_dlp.YoutubeDL(o) as ydl:
                info = ydl.extract_info(url, download=True)
                path = _finalize(ydl, info)
            size = os.path.getsize(path) if os.path.exists(path) else 0
            SESSION["bytes"] += size; SESSION["count"] += 1
            with JOBS_LOCK:
                j = JOBS.get(jid)
                if j:
                    j.update(status="done", progress=100.0, stage="done",
                             file=path, title=info.get("title","download"),
                             ext=os.path.splitext(path)[1].lstrip("."),
                             filesize=size, finished_at=time.time(),
                             video_id=info.get("id"))
                    snap = dict(j)
                else: snap = None
            thumb = cache_thumb(info.get("thumbnail"))
            db_upsert(jid, status="done", title=info.get("title"),
                      video_id=info.get("id"), uploader=info.get("uploader"),
                      duration=info.get("duration"), thumbnail=thumb,
                      file_path=path, file_size=size, finished_at=time.time())
            if snap:
                snap["thumbnail"] = thumb
                ws.bcast({"type":"job_done","job":snap,"session": session_snapshot()})
                fire_webhook({"event":"done","job_id":jid,"title":info.get("title"),
                              "file_size":size,"path":path})
    except DownloadCancelled:
        with JOBS_LOCK:
            if jid in JOBS:
                JOBS[jid].update(status="cancelled", stage="cancelled", finished_at=time.time())
        db_upsert(jid, status="cancelled", finished_at=time.time())
        ws.bcast({"type":"job_cancelled","job_id":jid})
    except Exception as e:
        import traceback
        log.exception("job %s failed", jid)
        msg = friendly(e)
        log_error(jid, msg, traceback.format_exc())
        with JOBS_LOCK:
            if jid in JOBS:
                JOBS[jid].update(status="error", error=msg, finished_at=time.time())
        db_upsert(jid, status="error", error=msg, finished_at=time.time())
        ws.bcast({"type":"job_error","job_id":jid,"error":msg})
        fire_webhook({"event":"error","job_id":jid,"error":msg})
    finally:
        CANCELS.pop(jid, None)

def do_playlist(jid, urls, req):
    try:
        with JOBS_LOCK:
            JOBS[jid].update(status="running", stage="starting", progress=0.0,
                             total_items=len(urls), done_items=0,
                             items=[{"url":u,"title":None,"status":"queued"} for u in urls])
        db_upsert(jid, status="running")
        ws.bcast({"type":"job_started","job_id":jid,
                  "title": f"playlist · {len(urls)} items"})
        work = BASE_DIR / f"pl_{jid}"; work.mkdir(exist_ok=True)
        cancel = CANCELS.get(jid)
        def one(idx, u):
            if cancel and cancel.is_set(): raise DownloadCancelled("cancelled")
            o = build_opts(preset=req.preset, audio_format=req.audio_format,
                           subtitles=req.subtitles, subtitle_langs=req.subtitle_langs,
                           sponsorblock=req.sponsorblock,
                           cookies_from_browser=req.cookies_from_browser,
                           cookies_file=req.cookies_file,
                           playlist_items=req.playlist_items)
            o["outtmpl"] = str(work / f"{idx:03d}_%(title).100s_%(id)s.%(ext)s")
            with yt_dlp.YoutubeDL(o) as ydl:
                info = ydl.extract_info(u, download=True)
            return info.get("title", f"item {idx+1}")
        with ThreadPoolExecutor(max_workers=MAX_CONCURRENT) as pool:
            futs = {pool.submit(one, i, u): i for i, u in enumerate(urls)}
            for fut in as_completed(futs):
                if cancel and cancel.is_set(): break
                idx = futs[fut]
                with JOBS_LOCK:
                    j = JOBS.get(jid)
                    if not j: return
                    it = j["items"][idx]
                    try: it["title"] = fut.result(); it["status"] = "ok"
                    except DownloadCancelled: it["status"] = "cancelled"
                    except Exception as e:
                        it["status"] = "error"; it["error"] = friendly(e)
                    j["done_items"] = (j.get("done_items") or 0) + 1
                    j["progress"] = j["done_items"] / len(urls) * 100
                    snap = dict(j)
                ws.pub(jid, {"type":"job_progress","job":snap}, th=180)
        if cancel and cancel.is_set():
            shutil.rmtree(work, ignore_errors=True)
            with JOBS_LOCK:
                if jid in JOBS: JOBS[jid].update(status="cancelled", stage="cancelled", finished_at=time.time())
            db_upsert(jid, status="cancelled", finished_at=time.time())
            ws.bcast({"type":"job_cancelled","job_id":jid}); return
        zip_path = BASE_DIR / f"playlist_{jid}.zip"
        with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
            for f in sorted(work.rglob("*")):
                if f.is_file(): zf.write(f, f.relative_to(work))
        shutil.rmtree(work, ignore_errors=True)
        size = zip_path.stat().st_size
        SESSION["bytes"] += size; SESSION["count"] += 1
        with JOBS_LOCK:
            if jid in JOBS:
                JOBS[jid].update(status="done", stage="done", progress=100.0,
                                 file=str(zip_path), title="playlist", ext="zip",
                                 filesize=size, finished_at=time.time())
                snap = dict(JOBS[jid])
            else: snap = None
        db_upsert(jid, status="done", title="playlist",
                  file_path=str(zip_path), file_size=size, finished_at=time.time())
        if snap:
            ws.bcast({"type":"job_done","job":snap,"session": session_snapshot()})
            fire_webhook({"event":"done","job_id":jid,"title":"playlist","file_size":size})
    except DownloadCancelled:
        with JOBS_LOCK:
            if jid in JOBS: JOBS[jid].update(status="cancelled", stage="cancelled", finished_at=time.time())
        db_upsert(jid, status="cancelled", finished_at=time.time())
        ws.bcast({"type":"job_cancelled","job_id":jid})
    except Exception as e:
        import traceback
        log.exception("playlist %s failed", jid)
        msg = friendly(e)
        log_error(jid, msg, traceback.format_exc())
        with JOBS_LOCK:
            if jid in JOBS: JOBS[jid].update(status="error", error=msg, finished_at=time.time())
        db_upsert(jid, status="error", error=msg, finished_at=time.time())
        ws.bcast({"type":"job_error","job_id":jid,"error":msg})
    finally:
        CANCELS.pop(jid, None)

# ─────────────────────────────────────────────────────────────
# SUBSCRIPTIONS WORKER
# ─────────────────────────────────────────────────────────────
def _run_sub(sub):
    if not sub.get("enabled"): return
    last = sub.get("last_check") or 0
    if time.time() - last < max(SUB_CHECK_INTERVAL, sub.get("check_every") or 3600):
        return
    sid = sub["id"]
    with _db_lock:
        _db.execute("UPDATE subscriptions SET last_check=? WHERE id=?", (time.time(), sid))
        _db.commit()
    try:
        url = sub["url"]
        opts = {"quiet":True,"no_warnings":True,"extract_flat":"in_playlist",
                "skip_download":True,"playlistend": 30}
        with yt_dlp.YoutubeDL(opts) as ydl:
            data = ydl.extract_info(url, download=False)
        entries = [e for e in (data.get("entries") or []) if e and e.get("id")]
        seen = sub_seen_vids(sid)
        new_entries = [e for e in entries if e["id"] not in seen]
        if not new_entries:
            sub_mark_seen(sid, [e["id"] for e in entries])
            return
        # queue them
        ws.bcast({"type":"sub_new","sub_id":sid,
                  "title":sub.get("title") or url,"count":len(new_entries)})
        for e in new_entries[:10]:
            jid = uuid.uuid4().hex
            preset = sub.get("preset") or "720"
            o = build_opts(preset=preset,
                           organize_by=sub.get("organize_by"))
            tmpl = str(LIBRARY_DIR / _template(sub.get("organize_by"), None))
            CANCELS[jid] = threading.Event()
            with JOBS_LOCK:
                JOBS[jid] = {"status":"queued","progress":0.0,"stage":"queued",
                             "url": f"https://www.youtube.com/watch?v={e['id']}",
                             "label": f"sub · {preset}", "created_at": time.time(),
                             "title": e.get("title")}
            db_upsert(jid, url=f"https://www.youtube.com/watch?v={e['id']}",
                      status="queued", format_label=f"sub · {preset}",
                      created_at=time.time(), keep=1)
            threading.Thread(target=do_download, args=(
                jid, f"https://www.youtube.com/watch?v={e['id']}", o, tmpl, None),
                daemon=True).start()
        sub_mark_seen(sid, [e["id"] for e in entries])
        with _db_lock:
            _db.execute("UPDATE subscriptions SET last_new=? WHERE id=?",
                        (time.time(), sid)); _db.commit()
    except Exception as e:
        log.warning("sub check failed: %s", e)

def _sub_loop():
    time.sleep(15)
    while True:
        try:
            for s in sub_list():
                if s.get("enabled"): _run_sub(s)
        except Exception: log.exception("sub loop")
        time.sleep(120)

# ─────────────────────────────────────────────────────────────
# CLEANUP
# ─────────────────────────────────────────────────────────────
def _file_is_kept(path):
    with _db_lock:
        r = _db.execute("SELECT 1 FROM history WHERE file_path=? AND keep=1 LIMIT 1", (path,)).fetchone()
    return bool(r)

def _cleanup_loop():
    while True:
        try:
            time.sleep(CLEANUP_EVERY)
            now = time.time()
            with JOBS_LOCK:
                stale = [k for k, j in JOBS.items()
                         if j.get("status") in ("done","error","cancelled")
                         and now - j.get("finished_at", now) > JOB_RETENTION]
                for k in stale:
                    JOBS.pop(k, None); CANCELS.pop(k, None)
            # undo stack eviction
            global UNDO_STACK
            UNDO_STACK[:] = [u for u in UNDO_STACK if u["expires"] > now]
            # file purge
            for f in BASE_DIR.iterdir():
                try:
                    if not f.is_file(): continue
                    if f.name in ("grabtube.db",) or f.name.endswith("-wal") or f.name.endswith("-shm"): continue
                    if LIBRARY_DIR in f.parents or THUMB_DIR in f.parents: continue
                    if _file_is_kept(str(f)): continue
                    if now - f.stat().st_mtime > FILE_RETENTION:
                        f.unlink()
                except Exception: pass
        except Exception: log.exception("cleanup")

def _maybe_update_ytdlp():
    if settings_all().get("auto_update_ytdlp") != "1": return
    try:
        import subprocess
        subprocess.run([sys.executable, "-m", "pip", "install", "--quiet",
                        "--upgrade", "yt-dlp"], timeout=180, check=False)
        log.info("yt-dlp auto-update checked")
    except Exception as e:
        log.warning("auto-update skipped: %s", e)

@asynccontextmanager
async def lifespan(app):
    ws.bind(asyncio.get_running_loop())
    threading.Thread(target=_maybe_update_ytdlp, daemon=True).start()
    threading.Thread(target=_sub_loop, daemon=True).start()
    threading.Thread(target=_cleanup_loop, daemon=True).start()
    yield

app = FastAPI(title="GrabTube", lifespan=lifespan)

@app.exception_handler(HTTPException)
async def _http_exc(req: Request, exc: HTTPException):
    if req.headers.get("accept","").startswith("text/html"):
        return HTMLResponse(f"""<!DOCTYPE html><html><head><meta charset="utf-8">
        <title>{exc.status_code}</title>
        <link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=Fraunces:ital,opsz,wght@0,9..144,400;1,9..144,600&family=JetBrains+Mono&display=swap">
        <style>body{{font-family:'Fraunces',Georgia,serif;background:#F2EEE3;color:#1A1815;
        display:grid;place-items:center;height:100vh;margin:0;text-align:center}}
        h1{{font-size:112px;margin:0;letter-spacing:-.04em;color:#B23A2C;font-style:italic}}
        p{{font-family:'JetBrains Mono',monospace;font-size:13px;color:#7A736A}}
        a{{color:#B23A2C;text-decoration:none;border-bottom:1px solid}}</style></head>
        <body><div><h1>{exc.status_code}</h1>
        <p>{exc.detail or 'something broke'}</p>
        <p style="margin-top:32px"><a href="{BASE_PATH or '/'}">← back</a></p></div></body></html>""",
        status_code=exc.status_code)
    return JSONResponse({"detail": exc.detail}, status_code=exc.status_code)


# ─────────────────────────────────────────────────────────────
# ROUTES
# ─────────────────────────────────────────────────────────────
@app.get("/api/health")
async def health():
    d = shutil.disk_usage(str(BASE_DIR))
    active = sum(1 for j in JOBS.values() if j.get("status") in ("queued","running"))
    return {"ok":True,"active":active,"concurrency":MAX_CONCURRENT,
            "ffmpeg":bool(shutil.which("ffmpeg")),
            "allow_any_url":ALLOW_ANY_URL,
            "disk_free":d.free,"disk_total":d.total,
            "websockets": _has_ws(),
            "paused": PAUSED.is_set(),
            "base_path": BASE_PATH,
            "session": session_snapshot()}

def _has_ws():
    for m in ("websockets","wsproto"):
        try: __import__(m); return True
        except ImportError: pass
    return False

@app.get("/metrics")
async def metrics():
    d = shutil.disk_usage(str(BASE_DIR))
    stats = stats_payload()
    lines = [
        f"grabtube_downloads_total {stats['total_count']}",
        f"grabtube_bytes_total {stats['total_size']}",
        f"grabtube_session_bytes {SESSION['bytes']}",
        f"grabtube_session_files {SESSION['count']}",
        f"grabtube_uptime_seconds {time.time() - SESSION['started']:.0f}",
        f"grabtube_disk_free_bytes {d.free}",
        f"grabtube_active_jobs {sum(1 for j in JOBS.values() if j.get('status') in ('queued','running'))}",
        f"grabtube_ffmpeg_present {1 if shutil.which('ffmpeg') else 0}",
    ]
    return PlainTextResponse("\n".join(lines) + "\n", media_type="text/plain; version=0.0.4")

@app.get("/api/thumb/{name}")
async def thumb(name: str):
    name = safe_filename(name, max_len=64)
    p = THUMB_DIR / name
    if not p.exists(): raise HTTPException(404, "gone")
    return FileResponse(p, media_type="image/jpeg",
                        headers={"Cache-Control": "public, max-age=86400"})

@app.post("/api/info")
async def api_info(req: InfoReq):
    url = validate_url(req.url)
    opts = {"quiet":True,"no_warnings":True,"skip_download":True,
            "extract_flat":"in_playlist","noplaylist":False}
    try:
        with yt_dlp.YoutubeDL(opts) as ydl:
            data = ydl.extract_info(url, download=False)
    except Exception as e:
        raise HTTPException(400, friendly(e))
    if data.get("_type") == "playlist":
        entries = []
        for e in data.get("entries") or []:
            if not e: continue
            entries.append({"id":e.get("id"),"title":e.get("title"),
                "url": e.get("url") or e.get("webpage_url") or
                       (f"https://www.youtube.com/watch?v={e['id']}" if e.get("id") else None),
                "duration":e.get("duration"),
                "thumbnail": cache_thumb(e.get("thumbnail") or (e.get("thumbnails") or [{}])[-1].get("url"))})
        return {"type":"playlist","title":data.get("title") or "Playlist",
                "uploader":data.get("uploader"),"count":len(entries),"entries":entries}
    vid = data.get("id")
    existing = has_video(vid)
    return {"type":"video","id":vid,"title":data.get("title"),
            "uploader":data.get("uploader"),"duration":data.get("duration"),
            "thumbnail": cache_thumb(data.get("thumbnail")),
            "view_count":data.get("view_count"),"like_count":data.get("like_count"),
            "upload_date":data.get("upload_date"),
            "formats":clean_formats(data),"chapters":clean_chapters(data),
            "subtitles":clean_subs(data),
            "is_favorite": vid in fav_ids(),
            "already_have": bool(existing),
            "existing_job": existing,
            "url": url}

@app.post("/api/channel")
async def api_channel(req: ChannelReq):
    url = validate_url(req.url)
    if "/videos" not in url and "/streams" not in url:
        url = url.rstrip("/") + "/videos"
    opts = {"quiet":True,"no_warnings":True,"extract_flat":"in_playlist",
            "skip_download":True,"playlistend": max(1, min(int(req.limit or 60), 200))}
    try:
        with yt_dlp.YoutubeDL(opts) as ydl:
            data = ydl.extract_info(url, download=False)
    except Exception as e:
        raise HTTPException(400, friendly(e))
    entries = []
    for e in data.get("entries") or []:
        if not e or not e.get("id"): continue
        entries.append({"id":e.get("id"),"title":e.get("title"),
            "url":f"https://www.youtube.com/watch?v={e['id']}",
            "duration":e.get("duration"),"view_count":e.get("view_count"),
            "thumbnail": cache_thumb(e.get("thumbnail") or (e.get("thumbnails") or [{}])[-1].get("url"))})
    return {"title": data.get("title") or "Channel","count":len(entries),"entries":entries,
            "channel_url": url}

@app.post("/api/search")
async def api_search(req: SearchReq):
    q = (req.q or "").strip()
    if not q: raise HTTPException(400, "empty query")
    limit = max(1, min(int(req.limit or 24), 40))
    opts = {"quiet":True,"no_warnings":True,"skip_download":True,
            "extract_flat":"in_playlist","noplaylist":False}
    try:
        with yt_dlp.YoutubeDL(opts) as ydl:
            data = ydl.extract_info(f"ytsearch{limit}:{q}", download=False)
    except Exception as e:
        raise HTTPException(400, friendly(e))
    items = []
    for e in data.get("entries") or []:
        if not e or not e.get("id"): continue
        items.append({"id":e.get("id"),"title":e.get("title"),
            "url":f"https://www.youtube.com/watch?v={e['id']}",
            "uploader":e.get("uploader") or e.get("channel"),
            "duration":e.get("duration"),"view_count":e.get("view_count"),
            "thumbnail": cache_thumb(e.get("thumbnail") or (e.get("thumbnails") or [{}])[-1].get("url")),
            "already_have": bool(has_video(e.get("id")))})
    ws.bcast({"type":"search","q":q,"count":len(items)})
    return {"items": items, "q": q}

@app.post("/api/transcript")
async def api_transcript(req: InfoReq):
    url = validate_url(req.url)
    opts = {"quiet":True,"no_warnings":True,"skip_download":True,"writesubtitles":True,
            "writeautomaticsub":True,"subtitleslangs":["en.*"],"subtitlesformat":"vtt"}
    try:
        with yt_dlp.YoutubeDL(opts) as ydl:
            data = ydl.extract_info(url, download=False)
    except Exception as e:
        raise HTTPException(400, friendly(e))
    # extract transcript if available
    subs = data.get("subtitles") or {}
    autos = data.get("automatic_captions") or {}
    track = None
    for src in (subs, autos):
        for lang, tracks in src.items():
            if lang.startswith("en"):
                for t in tracks:
                    if t.get("ext") == "vtt": track = t.get("url"); break
                if track: break
        if track: break
    text = ""
    if track:
        try:
            import urllib.request
            with urllib.request.urlopen(track, timeout=10) as r:
                vtt = r.read().decode("utf-8", errors="ignore")
            # strip vtt to plain lines
            lines = []
            for ln in vtt.splitlines():
                if "-->" in ln or ln.startswith(("WEBVTT","NOTE","Kind:","Language:")) or not ln.strip(): continue
                lines.append(re.sub(r"<[^>]+>", "", ln).strip())
            # de-dupe consecutive
            out, prev = [], None
            for ln in lines:
                if ln != prev: out.append(ln)
                prev = ln
            text = "\n".join(out)
        except Exception as e:
            log.warning("transcript fetch failed: %s", e)
    return {"title": data.get("title"), "text": text,
            "has_transcript": bool(text)}

@app.get("/api/dupe/{video_id}")
async def api_dupe(video_id: str):
    return {"exists": bool(has_video(video_id)), "job_id": has_video(video_id)}

@app.post("/api/download")
async def api_download(req: DownloadReq, bg: BackgroundTasks):
    url = validate_url(req.url)
    jid = uuid.uuid4().hex
    payload = req.model_dump()
    if req.organize_by:
        tmpl = str(LIBRARY_DIR / _template(req.organize_by, req.filename_template))
    else:
        tmpl = str(BASE_DIR / f"{jid}.%(ext)s")
    if req.format_id:
        opts = build_fmt_opts(req.format_id, req.kind, extra=req.model_dump())
        label = f"fmt {req.format_id}"
    else:
        opts = build_opts(
            preset=req.preset, audio_format=req.audio_format,
            video_container=req.video_container, quality=req.quality,
            subtitles=req.subtitles, subtitle_langs=req.subtitle_langs,
            sub_files=req.sub_files, subtitle_format=req.subtitle_format,
            auto_subs=req.auto_subs, embed_subs=req.embed_subs,
            chapters=req.chapters, split_chapters=req.split_chapters,
            chapter_thumbs=req.chapter_thumbs, chapter_ranges=req.chapter_ranges,
            trim_start=req.trim_start, trim_end=req.trim_end,
            sponsorblock=req.sponsorblock,
            sponsorblock_categories=req.sponsorblock_categories,
            embed_metadata=req.embed_metadata, embed_thumbnail=req.embed_thumbnail,
            loudnorm=req.loudnorm, live_from_start=req.live_from_start,
            write_info_json=req.write_info_json,
            rate_limit=req.rate_limit, proxy=req.proxy,
            cookies_from_browser=req.cookies_from_browser,
            cookies_file=req.cookies_file, sort=req.sort,
            playlist_items=req.playlist_items, extra_args=req.extra_args)
        label = req.preset or req.audio_format or (f"{req.quality}p" if req.quality else "best")
        if req.trim_start is not None and req.trim_end is not None:
            label += f" · trim {int(req.trim_start)}-{int(req.trim_end)}s"
    CANCELS[jid] = threading.Event()
    with JOBS_LOCK:
        JOBS[jid] = {"status":"queued","progress":0.0,"stage":"queued",
                     "url":url,"label":label,"created_at":time.time(),"title":None}
    db_upsert(jid, url=url, status="queued", format_label=label,
              created_at=time.time(), payload=json.dumps(payload),
              keep=1 if req.keep else 0)
    bg.add_task(do_download, jid, url, opts, tmpl, payload)
    ws.bcast({"type":"job_queued","job_id":jid,"url":url,"label":label})
    return {"job_id": jid}

@app.post("/api/playlist")
async def api_playlist(req: PlaylistReq, bg: BackgroundTasks):
    if not req.urls: raise HTTPException(400, "no URLs")
    urls = [validate_url(u) for u in req.urls]
    jid = uuid.uuid4().hex
    CANCELS[jid] = threading.Event()
    with JOBS_LOCK:
        JOBS[jid] = {"status":"queued","progress":0.0,"stage":"queued",
                     "url":f"{len(urls)} items","label":f"zip · {req.preset}",
                     "created_at":time.time()}
    db_upsert(jid, url=f"{len(urls)} items", status="queued",
              format_label=f"zip · {req.preset}", created_at=time.time(),
              payload=json.dumps(req.model_dump()))
    bg.add_task(do_playlist, jid, urls, req)
    ws.bcast({"type":"job_queued","job_id":jid,"url":f"{len(urls)} items",
              "label":f"zip · {req.preset}"})
    return {"job_id": jid}

@app.post("/api/paste")
async def api_paste(req: BatchReq, bg: BackgroundTasks):
    if not req.urls: raise HTTPException(400, "no URLs")
    raw = []
    for r in req.urls: raw.extend(extract_urls(r))
    urls = [u for u in dict.fromkeys(raw) if u]
    if not urls: raise HTTPException(400, "no URLs found")
    urls = [validate_url(u) for u in urls]
    jid = uuid.uuid4().hex
    CANCELS[jid] = threading.Event()
    with JOBS_LOCK:
        JOBS[jid] = {"status":"queued","progress":0.0,"stage":"queued",
                     "url":f"{len(urls)} urls","label":f"batch · {req.preset}",
                     "created_at":time.time()}
    db_upsert(jid, url=f"{len(urls)} urls", status="queued",
              format_label=f"batch · {req.preset}", created_at=time.time())
    preq = PlaylistReq(urls=urls, preset=req.preset, audio_format=req.audio_format)
    bg.add_task(do_playlist, jid, urls, preq)
    ws.bcast({"type":"job_queued","job_id":jid,"url":f"{len(urls)} urls",
              "label":f"batch · {req.preset}"})
    return {"job_id": jid, "count": len(urls)}

@app.post("/api/cancel/{jid}")
async def api_cancel(jid: str):
    with JOBS_LOCK:
        j = JOBS.get(jid)
        if not j: raise HTTPException(404, "not found")
        if j.get("status") in ("done","error","cancelled"):
            return {"ok":True,"already":j["status"]}
        j["stage"] = "cancelling"
    ev = CANCELS.get(jid)
    if ev: ev.set()
    ws.bcast({"type":"job_cancelling","job_id":jid})
    return {"ok": True}

@app.get("/api/status/{jid}")
async def api_status(jid: str):
    with JOBS_LOCK:
        j = JOBS.get(jid)
        if j: return dict(j)
    db = db_get(jid)
    if db: return db
    raise HTTPException(404, "not found")

@app.get("/api/jobs")
async def api_jobs():
    with JOBS_LOCK:
        items = sorted(JOBS.items(), key=lambda kv: kv[1].get("created_at",0), reverse=True)
        return {"items": [{"job_id":k, **v} for k, v in items], "paused": PAUSED.is_set()}

@app.post("/api/queue/pause")
async def api_queue_pause(payload: dict):
    if payload.get("paused"): PAUSED.set()
    else: PAUSED.clear()
    ws.bcast({"type":"queue_paused","paused": PAUSED.is_set()})
    return {"paused": PAUSED.is_set()}

@app.post("/api/queue/priority/{jid}")
async def api_queue_priority(jid: str):
    with JOBS_LOCK:
        if jid not in JOBS: raise HTTPException(404, "not found")
        JOBS[jid]["priority"] = -1  # sort first
        JOBS[jid]["created_at"] = 0
    return {"ok": True}

@app.get("/api/history")
async def api_history(limit: int = 60, q: Optional[str] = None,
                      status: Optional[str] = None, sort: Optional[str] = None,
                      keep_only: bool = False, tag: Optional[str] = None):
    return {"items": db_list(limit, q, status, sort, keep_only, tag)}

@app.get("/api/library")
async def api_library(limit: int = 200, q: Optional[str] = None,
                       sort: Optional[str] = None, tag: Optional[str] = None):
    items = db_list(limit, q, "done", sort or "size", True, tag)
    return {"items": items}

@app.delete("/api/history/{jid}")
async def api_history_del(jid: str):
    row = db_get(jid)
    db_delete(jid)
    with JOBS_LOCK: JOBS.pop(jid, None)
    if row:
        push_undo("delete", {"row": row})
    ws.bcast({"type":"history_deleted","job_id":jid})
    return {"ok": True, "undo_id": UNDO_STACK[-1]["id"] if UNDO_STACK else None}

@app.post("/api/history/{jid}/patch")
async def api_history_patch(jid: str, req: PatchReq):
    fields = {}
    if req.watched is not None: fields["watched"] = 1 if req.watched else 0
    if req.resume_seconds is not None: fields["resume_seconds"] = req.resume_seconds
    if req.rating is not None:
        if not (0 <= req.rating <= 5): raise HTTPException(400, "rating must be 0-5")
        fields["rating"] = req.rating
    if req.tags is not None: fields["tags"] = json.dumps([t.strip() for t in req.tags if t.strip()][:20])
    if req.notes is not None: fields["notes"] = req.notes[:2000]
    if req.bookmarks is not None: fields["bookmarks"] = json.dumps(req.bookmarks[:200])
    if req.keep is not None: fields["keep"] = 1 if req.keep else 0
    if fields:
        db_patch(jid, **fields)
        ws.bcast({"type":"history_patched","job_id":jid,"fields":list(fields.keys())})
    return {"ok": True}

@app.post("/api/keep/{jid}")
async def api_keep(jid: str, payload: dict):
    keep = bool(payload.get("keep"))
    db_patch(jid, keep=1 if keep else 0)
    with JOBS_LOCK:
        if jid in JOBS: JOBS[jid]["keep"] = 1 if keep else 0
    ws.bcast({"type":"keep_toggled","job_id":jid,"keep":keep})
    return {"ok": True, "keep": keep}

@app.post("/api/history/{jid}/retry")
async def api_history_retry(jid: str, bg: BackgroundTasks):
    db = db_get(jid)
    if not db: raise HTTPException(404, "not found")
    new_id = uuid.uuid4().hex
    url = db["url"]; validate_url(url)
    payload = {}
    try: payload = json.loads(db.get("payload") or "{}")
    except Exception: payload = {}
    if payload:
        try: dreq = DownloadReq(**{**payload, "url": url})
        except Exception: dreq = DownloadReq(url=url, preset="best")
    else:
        dreq = DownloadReq(url=url, preset="best")
    label = db.get("format_label") or dreq.preset or "best"
    tmpl = str(BASE_DIR / f"{new_id}.%(ext)s")
    opts = build_opts(preset=dreq.preset, audio_format=dreq.audio_format,
        video_container=dreq.video_container, quality=dreq.quality,
        subtitles=dreq.subtitles, subtitle_langs=dreq.subtitle_langs,
        chapters=dreq.chapters, split_chapters=dreq.split_chapters,
        sponsorblock=dreq.sponsorblock, embed_metadata=dreq.embed_metadata,
        embed_thumbnail=dreq.embed_thumbnail, rate_limit=dreq.rate_limit,
        proxy=dreq.proxy, cookies_from_browser=dreq.cookies_from_browser,
        cookies_file=dreq.cookies_file)
    CANCELS[new_id] = threading.Event()
    with JOBS_LOCK:
        JOBS[new_id] = {"status":"queued","progress":0.0,"stage":"queued",
                        "url":url,"label":label,"created_at":time.time()}
    db_upsert(new_id, url=url, status="queued", format_label=label,
              created_at=time.time(), payload=json.dumps(dreq.model_dump()))
    bg.add_task(do_download, new_id, url, opts, tmpl, dreq.model_dump())
    ws.bcast({"type":"job_queued","job_id":new_id,"url":url,"label":label})
    return {"job_id": new_id}

@app.post("/api/undo")
async def api_undo(req: UndoReq):
    now = time.time()
    for i, u in enumerate(UNDO_STACK):
        if u["id"] == req.id and u["expires"] > now:
            kind, payload = u["kind"], u["payload"]
            UNDO_STACK.pop(i)
            if kind == "delete" and "row" in payload:
                row = payload["row"]
                # reinsert
                fields = {k: v for k, v in row.items() if k != "job_id"}
                db_upsert(row["job_id"], **fields)
                with JOBS_LOCK:
                    JOBS[row["job_id"]] = {k: v for k, v in row.items() if k != "job_id"}
                    JOBS[row["job_id"]]["job_id"] = row["job_id"]
            ws.bcast({"type":"undo_applied","id": req.id})
            return {"ok": True}
    raise HTTPException(404, "undo expired")

@app.post("/api/batch/delete")
async def api_batch_delete(payload: dict):
    ids = payload.get("ids") or []
    rows = []
    for jid in ids[:200]:
        r = db_get(jid)
        if r: rows.append(r); db_delete(jid)
        with JOBS_LOCK: JOBS.pop(jid, None)
    if rows: push_undo("delete_batch", {"rows": rows})
    ws.bcast({"type":"batch_deleted","count":len(rows)})
    return {"ok": True, "count": len(rows)}

@app.post("/api/batch/keep")
async def api_batch_keep(payload: dict):
    ids = payload.get("ids") or []
    keep = bool(payload.get("keep"))
    for jid in ids[:500]:
        db_patch(jid, keep=1 if keep else 0)
    ws.bcast({"type":"batch_keep","count":len(ids),"keep":keep})
    return {"ok": True}

@app.get("/api/favorites")
async def api_fav_list(): return {"items": fav_list()}

@app.post("/api/favorites/toggle")
async def api_fav_toggle(payload: dict):
    added = fav_toggle(payload)
    ws.bcast({"type":"favorite_toggled","video_id":payload.get("id"),"added":added})
    return {"favorite": added}

@app.delete("/api/favorites/{vid}")
async def api_fav_del(vid: str):
    fav_delete(vid)
    ws.bcast({"type":"favorite_removed","video_id":vid})
    return {"ok": True}

@app.get("/api/presets")
async def api_presets():
    items = preset_list()
    for it in items:
        try: it["payload"] = json.loads(it["payload"])
        except Exception: it["payload"] = {}
    return {"items": items}

@app.post("/api/presets")
async def api_preset_save(req: PresetReq):
    pid = preset_save(req.name, req.payload)
    ws.bcast({"type":"preset_saved","id":pid,"name":req.name})
    return {"id": pid}

@app.delete("/api/presets/{pid}")
async def api_preset_del(pid: str):
    preset_delete(pid)
    ws.bcast({"type":"preset_deleted","id":pid})
    return {"ok": True}

@app.get("/api/profiles")
async def api_profiles(): return {"items": profile_list()}
@app.post("/api/profiles")
async def api_profile_save(req: ProfileReq):
    pid = profile_save(req.name, req.config, req.is_default)
    ws.bcast({"type":"profile_saved","id":pid,"name":req.name})
    return {"id": pid}
@app.delete("/api/profiles/{pid}")
async def api_profile_del(pid: str):
    profile_delete(pid); ws.bcast({"type":"profile_deleted","id":pid})
    return {"ok": True}

@app.get("/api/subscriptions")
async def api_subs(): return {"items": sub_list()}
@app.post("/api/subscriptions")
async def api_sub_add(req: SubReq):
    url = validate_url(req.url)
    try:
        opts = {"quiet":True,"no_warnings":True,"extract_flat":"in_playlist","skip_download":True,"playlistend": 1}
        with yt_dlp.YoutubeDL(opts) as ydl:
            data = ydl.extract_info(url, download=False)
        title = data.get("title") or url
        kind = req.kind
        if data.get("_type") == "playlist": kind = "playlist"
        else: kind = "channel"
    except Exception as e:
        raise HTTPException(400, friendly(e))
    sid = sub_save(url, kind, title, req.preset, req.profile_id,
                   req.organize_by, max(SUB_CHECK_INTERVAL, req.check_every))
    ws.bcast({"type":"sub_added","id":sid,"title":title})
    return {"id": sid, "title": title, "kind": kind}
@app.delete("/api/subscriptions/{sid}")
async def api_sub_del(sid: str):
    sub_delete(sid); ws.bcast({"type":"sub_deleted","id":sid})
    return {"ok": True}
@app.post("/api/subscriptions/{sid}/toggle")
async def api_sub_toggle(sid: str, payload: dict):
    sub_toggle(sid, bool(payload.get("enabled")))
    ws.bcast({"type":"sub_toggled","id":sid,"enabled":bool(payload.get("enabled"))})
    return {"ok": True}
@app.post("/api/subscriptions/{sid}/check")
async def api_sub_check(sid: str):
    s = sub_get(sid)
    if not s: raise HTTPException(404, "not found")
    # force check
    with _db_lock:
        _db.execute("UPDATE subscriptions SET last_check=0 WHERE id=?", (sid,)); _db.commit()
    threading.Thread(target=_run_sub, args=(sub_get(sid),), daemon=True).start()
    return {"ok": True}

@app.get("/api/settings")
async def api_settings(): return settings_all()

@app.post("/api/settings")
async def api_settings_set(payload: dict):
    for k, v in (payload or {}).items():
        settings_set(k, str(v))
    return {"ok": True}

@app.get("/api/stats")
async def api_stats(): return stats_payload()

@app.get("/api/export/history")
async def api_export_history():
    items = db_list(500)
    payload = json.dumps({"version": 1, "exported_at": time.time(), "items": items}, indent=2)
    return Response(payload, media_type="application/json",
                    headers={"Content-Disposition": "attachment; filename=grabtube-history.json"})

@app.post("/api/import/history")
async def api_import_history(payload: dict):
    items = payload.get("items") or []
    n = 0
    for it in items:
        if not it.get("job_id"): continue
        try:
            db_upsert(it["job_id"], **{k: v for k, v in it.items() if k != "job_id"})
            n += 1
        except Exception: continue
    return {"ok": True, "imported": n}

@app.get("/api/file/{jid}")
async def api_file(jid: str):
    with JOBS_LOCK: j = JOBS.get(jid)
    if j and j.get("status") == "done":
        path = j["file"]; title = safe_filename(j.get("title") or "download")
        ext = safe_filename(j.get("ext") or "bin", max_len=10).lstrip(".")
    else:
        db = db_get(jid)
        if not db or db.get("status") != "done": raise HTTPException(404, "not ready")
        path = db["file_path"]; title = safe_filename(db.get("title") or "download")
        ext = os.path.splitext(path)[1].lstrip(".") or "bin"
    if not path or not os.path.exists(path): raise HTTPException(410, "gone")
    return FileResponse(path, media_type="application/octet-stream",
                        filename=f"{title}.{ext}")

@app.get("/api/stream/{jid}")
async def api_stream(jid: str, request: Request):
    with JOBS_LOCK: j = JOBS.get(jid)
    if j and j.get("status") == "done": path = j["file"]
    else:
        db = db_get(jid)
        if not db or db.get("status") != "done": raise HTTPException(404, "not ready")
        path = db["file_path"]
    if not path or not os.path.exists(path): raise HTTPException(410, "gone")
    size = os.path.getsize(path)
    ext = os.path.splitext(path)[1].lower().lstrip(".")
    ctype = {"mp4":"video/mp4","webm":"video/webm","mkv":"video/x-matroska",
             "mp3":"audio/mpeg","m4a":"audio/mp4","opus":"audio/opus",
             "flac":"audio/flac","wav":"audio/wav","ogg":"audio/ogg"}.get(ext, "application/octet-stream")
    range_header = request.headers.get("range")
    if not range_header:
        return FileResponse(path, media_type=ctype)
    m = re.match(r"bytes=(\d+)-(\d*)", range_header)
    if not m: raise HTTPException(416)
    start = int(m.group(1)); end = int(m.group(2)) if m.group(2) else size - 1
    end = min(end, size - 1); length = end - start + 1
    def _iter():
        with open(path, "rb") as f:
            f.seek(start); remaining = length
            while remaining > 0:
                chunk = f.read(min(64*1024, remaining))
                if not chunk: break
                remaining -= len(chunk); yield chunk
    headers = {"Content-Range": f"bytes {start}-{end}/{size}",
               "Accept-Ranges": "bytes", "Content-Length": str(length)}
    return StreamingResponse(_iter(), status_code=206, headers=headers, media_type=ctype)

@app.websocket("/ws")
async def ws_global(sock: WebSocket):
    await ws.sub(WSMan.GLOBAL, sock)
    try:
        with JOBS_LOCK:
            snap = [{"job_id":k, **v} for k, v in JOBS.items()]
        await sock.send_json({"type":"snapshot","jobs":snap,
                              "session": session_snapshot(),
                              "paused": PAUSED.is_set(),
                              "health":{"ffmpeg": bool(shutil.which("ffmpeg")),
                                        "concurrency": MAX_CONCURRENT,
                                        "base_path": BASE_PATH}})
        while True: await sock.receive_text()
    except WebSocketDisconnect: pass
    finally: ws.unsub(WSMan.GLOBAL, sock)

@app.websocket("/ws/{jid}")
async def ws_job(sock: WebSocket, jid: str):
    await ws.sub(jid, sock)
    try:
        with JOBS_LOCK: j = JOBS.get(jid)
        if j: await sock.send_json({"type":"job_progress","job":dict(j)})
        while True: await sock.receive_text()
    except WebSocketDisconnect: pass
    finally: ws.unsub(jid, sock)

@app.get("/manifest.webmanifest")
async def manifest():
    return Response(json.dumps({
        "name":"GrabTube","short_name":"GrabTube","start_url": BASE_PATH or "/",
        "display":"standalone","background_color":"#F2EEE3","theme_color":"#B23A2C",
        "icons":[{"src":"data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 32 32'%3E%3Crect width='32' height='32' fill='%23F2EEE3'/%3E%3Ctext x='6' y='24' font-family='Georgia' font-size='22' font-weight='700' fill='%23B23A2C'%3EG%3C/text%3E%3C/svg%3E",
                  "sizes":"any","type":"image/svg+xml"}]
    }), media_type="application/manifest+json")

# ─────────────────────────────────────────────────────────────
# HTML
# ─────────────────────────────────────────────────────────────
HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<meta name="theme-color" content="#B23A2C">
<link rel="manifest" href="manifest.webmanifest">
<title>GrabTube</title>
<link rel="icon" href="data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 32 32'%3E%3Crect width='32' height='32' fill='%23F2EEE3'/%3E%3Ctext x='6' y='24' font-family='Georgia' font-size='22' font-weight='700' fill='%23B23A2C'%3EG%3C/text%3E%3C/svg%3E">
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Fraunces:ital,opsz,wght@0,9..144,400;0,9..144,600;0,9..144,700;1,9..144,600;1,9..144,700&family=Inter+Tight:wght@400;500;600&family=JetBrains+Mono:wght@400;500;700&display=swap" rel="stylesheet">
<style>
:root{
  --paper:#F2EEE3; --paper-2:#E8E2D2; --paper-3:#DDD5C0;
  --ink:#1A1815; --ink-2:#4A4540; --ink-3:#7A736A;
  --rule:#CFC7B5; --rule-2:#A89E88;
  --red:#B23A2C; --red-2:#8C2A1E; --navy:#2D4A7C;
  --ok:#3D6B4E; --warn:#A57820; --err:#8F2C1F;
  --serif:'Fraunces',Georgia,serif;
  --sans:'Inter Tight',system-ui,-apple-system,sans-serif;
  --mono:'JetBrains Mono',ui-monospace,Menlo,Consolas,monospace;
  --density:1; --scale:1;
}
[data-theme="dark"]{
  --paper:#171512; --paper-2:#1F1C18; --paper-3:#282420;
  --ink:#EDE4D3; --ink-2:#B9AF9A; --ink-3:#7D7565;
  --rule:#332E28; --rule-2:#4A4238;
  --red:#E85A3C; --red-2:#FF7050; --navy:#6E8CC4;
  --ok:#7FB26B; --warn:#D9A340; --err:#E8674B;
}
[data-theme="sepia"]{
  --paper:#E8DCC0; --paper-2:#DDD0B0; --paper-3:#CFC09A;
  --ink:#2A2016; --ink-2:#5A4A38; --ink-3:#8A7A62;
  --rule:#B8A888; --rule-2:#9A8A6A; --red:#8E2A18; --red-2:#B23A2C; --navy:#2A4066;
}
[data-theme="hc"]{
  --paper:#000000; --paper-2:#0a0a0a; --paper-3:#141414;
  --ink:#FFFFFF; --ink-2:#E0E0E0; --ink-3:#B0B0B0;
  --rule:#404040; --rule-2:#606060;
  --red:#FF5252; --red-2:#FF8080; --navy:#82B1FF;
  --ok:#69F0AE; --warn:#FFD740; --err:#FF5252;
}
[data-density="compact"]{ --density:0.7; }
[data-density="roomy"]{ --density:1.15; }

*{box-sizing:border-box}
html,body{margin:0;padding:0}
body{
  font-family:var(--sans);background:var(--paper);color:var(--ink);
  font-size:calc(14.5px * var(--scale));line-height:1.55;
  -webkit-font-smoothing:antialiased;min-height:100vh;padding-bottom:44px;
  transition:background-color .2s,color .2s;
}
::selection{background:var(--red);color:var(--paper)}
:focus-visible{outline:2px solid var(--red);outline-offset:2px;border-radius:2px}
::-webkit-scrollbar{width:10px;height:10px}
::-webkit-scrollbar-track{background:transparent}
::-webkit-scrollbar-thumb{background:var(--rule-2);border-radius:0}
.sr-only{position:absolute;width:1px;height:1px;padding:0;margin:-1px;overflow:hidden;
  clip:rect(0,0,0,0);white-space:nowrap;border:0}
.skip-link{position:absolute;left:-9999px;top:0;background:var(--red);color:var(--paper);
  padding:12px 20px;z-index:1000;font-family:var(--mono);font-size:12px;text-decoration:none}
.skip-link:focus{left:0}

body::before{content:"";position:fixed;top:0;left:0;right:0;height:2px;z-index:200;
  background:var(--red);transform-origin:left;transform:scaleX(0);
  animation:sweep 1s cubic-bezier(.7,.05,.3,.95) .1s 1 forwards}
@keyframes sweep{to{transform:scaleX(1);opacity:0}}

.masthead{border-bottom:2px solid var(--ink);padding:18px 40px 14px;
  display:grid;grid-template-columns:auto 1fr auto;gap:40px;align-items:baseline;
  position:sticky;top:0;background:var(--paper);z-index:40;transition:opacity .2s}
body.focus-mode .masthead{opacity:.35}
body.focus-mode .masthead:hover{opacity:1}
.masthead .brand{font-family:var(--serif);font-size:26px;font-weight:700;
  letter-spacing:-.03em;line-height:1;font-variation-settings:"opsz" 144,"SOFT" 50}
.masthead .brand .amp{color:var(--red);font-style:italic}
.masthead nav{display:flex;gap:22px;font-family:var(--mono);font-size:11.5px;
  letter-spacing:.08em;text-transform:uppercase;flex-wrap:wrap}
.masthead nav a{color:var(--ink-2);text-decoration:none;cursor:pointer;
  padding-bottom:3px;border-bottom:2px solid transparent;transition:.12s}
.masthead nav a:hover{color:var(--ink)}
.masthead nav a.active{color:var(--ink);border-bottom-color:var(--red)}
.masthead .status{font-family:var(--mono);font-size:11px;color:var(--ink-2);
  display:flex;align-items:center;gap:12px;flex-wrap:wrap;justify-content:flex-end}
.dot{display:inline-block;width:6px;height:6px;border-radius:50%;
  background:var(--ink-3);margin-right:6px;vertical-align:middle}
.dot.ok{background:var(--ok)} .dot.warn{background:var(--warn)}
.dot.err{background:var(--err)} .dot.live{background:var(--red);animation:blip 1.1s ease-in-out infinite}
@keyframes blip{50%{opacity:.35}}
.tools{display:flex;gap:4px;font-family:var(--mono);font-size:11px}
.tool{border:1px solid var(--rule-2);background:var(--paper);color:var(--ink-2);
  padding:4px 8px;cursor:pointer;transition:all .12s;font-family:var(--mono);font-size:11px}
.tool:hover{border-color:var(--ink);color:var(--ink)}
.tool.on{background:var(--red);border-color:var(--red);color:var(--paper)}
.profile-select{font-family:var(--mono);font-size:11px;background:var(--paper);
  border:1px solid var(--rule-2);color:var(--ink);padding:4px 6px;cursor:pointer;outline:0}

.hero{max-width:1240px;margin:0 auto;padding:56px 40px 20px}
.hero .kicker{font-family:var(--mono);font-size:11px;letter-spacing:.16em;
  text-transform:uppercase;color:var(--red);margin-bottom:16px;
  display:flex;align-items:center;gap:12px}
.hero .kicker::after{content:"";flex:1;height:1px;background:var(--rule)}
.hero h1{font-family:var(--serif);font-size:clamp(42px,6vw,76px);
  font-weight:600;letter-spacing:-.04em;line-height:.95;margin:0 0 6px}
.hero h1 em{font-style:italic;color:var(--red)}
.hero p{font-size:16px;color:var(--ink-2);max-width:60ch;margin:18px 0 0;line-height:1.55}

.command{max-width:1240px;margin:32px auto 0;padding:0 40px}
.command .frame{border:1.5px solid var(--ink);background:var(--paper);
  display:flex;align-items:stretch;box-shadow:4px 4px 0 var(--ink);
  transition:transform .12s,box-shadow .12s;position:relative}
.command .frame:focus-within{transform:translate(2px,2px);box-shadow:2px 2px 0 var(--ink)}
.command .frame.drop{border-color:var(--red);box-shadow:4px 4px 0 var(--red);background:rgba(178,58,44,.04)}
.command .glyph{padding:0 22px;background:var(--ink);color:var(--paper);
  display:grid;place-items:center;font-family:var(--serif);font-style:italic;font-size:22px}
.command input{flex:1;background:transparent;border:0;outline:0;
  font-family:var(--mono);font-size:15.5px;color:var(--ink);
  padding:calc(22px * var(--density)) 20px;caret-color:var(--red);font-weight:500}
.command input::placeholder{color:var(--ink-3);font-weight:400}
.command button{border:0;border-left:1.5px solid var(--ink);background:var(--paper);
  color:var(--ink);font-family:var(--mono);font-size:12px;font-weight:700;
  letter-spacing:.14em;text-transform:uppercase;padding:0 30px;cursor:pointer;transition:.12s}
.command button:hover{background:var(--ink);color:var(--paper)}
.command button:disabled{background:var(--paper-2);color:var(--ink-3);cursor:not-allowed}
.command .hint{font-family:var(--mono);font-size:11px;color:var(--ink-3);
  margin-top:12px;letter-spacing:.05em;display:flex;flex-wrap:wrap;gap:18px}
.command .hint kbd{font-family:var(--mono);font-size:10px;background:var(--paper-2);
  border:1px solid var(--rule);padding:1px 5px;color:var(--ink)}
.autocomplete{position:absolute;top:100%;left:0;right:0;background:var(--paper);
  border:1.5px solid var(--ink);border-top:0;box-shadow:4px 4px 0 var(--ink);
  max-height:280px;overflow-y:auto;z-index:60;display:none}
.autocomplete.on{display:block}
.autocomplete .row{padding:10px 20px;font-family:var(--mono);font-size:12px;
  color:var(--ink-2);cursor:pointer;border-bottom:1px solid var(--rule);display:flex;gap:12px;align-items:center}
.autocomplete .row:hover,.autocomplete .row.sel{background:var(--paper-2);color:var(--ink)}
.autocomplete .row .sym{color:var(--red);font-family:var(--serif);font-style:italic}
.autocomplete .row .meta{margin-left:auto;color:var(--ink-3);font-size:10.5px}

.presets{max-width:1240px;margin:24px auto 0;padding:0 40px;
  display:flex;flex-wrap:wrap;gap:10px;align-items:center}
.presets .label{font-family:var(--mono);font-size:10.5px;letter-spacing:.16em;
  text-transform:uppercase;color:var(--ink-3);margin-right:6px}
.pill{font-family:var(--mono);font-size:11.5px;font-weight:500;padding:6px 12px;
  border:1px solid var(--rule-2);border-radius:14px;background:var(--paper);
  color:var(--ink-2);cursor:pointer;transition:all .12s;letter-spacing:.02em;user-select:none}
.pill:hover{border-color:var(--ink);color:var(--ink)}
.pill.on{background:var(--red);border-color:var(--red);color:var(--paper)}
.pill.ghost{border-style:dashed;color:var(--ink-3)}
.pill.ghost:hover{color:var(--red);border-color:var(--red)}
.pill .x{margin-left:8px;opacity:.5}
.pill .x:hover{opacity:1}

.sheet{max-width:1240px;margin:44px auto 0;padding:0 40px;
  display:grid;grid-template-columns:minmax(0,1fr) 320px;gap:48px}
@media (max-width:1080px){.sheet{grid-template-columns:1fr}}
.sheet .side{position:sticky;top:112px;align-self:start;
  display:flex;flex-direction:column;gap:32px}
body.focus-mode .side{display:none}
body.focus-mode .sheet{grid-template-columns:1fr}

.sect{margin-bottom:44px}
.sect-head{display:flex;align-items:baseline;gap:16px;
  border-bottom:1.5px solid var(--ink);padding-bottom:10px;margin-bottom:20px;flex-wrap:wrap}
.sect-head h2{font-family:var(--serif);font-weight:600;font-size:22px;
  letter-spacing:-.02em;margin:0;line-height:1}
.sect-head .num{font-family:var(--mono);font-size:10px;letter-spacing:.16em;color:var(--red);font-weight:700}
.sect-head .right{margin-left:auto;font-family:var(--mono);font-size:11px;
  color:var(--ink-3);letter-spacing:.05em;display:flex;align-items:baseline;gap:10px;flex-wrap:wrap}

.vid{display:grid;grid-template-columns:260px 1fr;gap:28px}
@media (max-width:680px){.vid{grid-template-columns:1fr}}
.thumb{aspect-ratio:16/9;background:#1A1815 center/cover no-repeat;
  border:1px solid var(--rule);position:relative;overflow:hidden;cursor:pointer}
.thumb .badge-dur{position:absolute;right:6px;bottom:6px;background:rgba(0,0,0,.75);
  color:var(--paper);font-family:var(--mono);font-size:10.5px;padding:2px 6px}
.thumb .play-btn{position:absolute;inset:0;display:grid;place-items:center;background:rgba(0,0,0,.15)}
.thumb:hover .play-btn{background:rgba(0,0,0,.28)}
.thumb .play-btn::after{content:"";width:52px;height:52px;background:var(--paper);
  clip-path:polygon(34% 26%, 34% 74%, 78% 50%);
  filter:drop-shadow(0 3px 10px rgba(0,0,0,.4))}
.thumb iframe{position:absolute;inset:0;width:100%;height:100%;border:0}
.vid .info h3{font-family:var(--serif);font-size:26px;font-weight:600;
  letter-spacing:-.02em;line-height:1.15;margin:0 0 12px}
.vid .meta{font-family:var(--mono);font-size:11.5px;color:var(--ink-3);
  display:flex;flex-wrap:wrap;gap:14px;margin-bottom:16px;letter-spacing:.02em}
.vid .meta b{color:var(--ink-2);font-weight:500}
.tag-row{display:flex;flex-wrap:wrap;gap:6px;margin-bottom:18px}
.tag{font-family:var(--mono);font-size:10.5px;letter-spacing:.06em;padding:3px 8px;
  background:var(--paper-2);border:1px solid var(--rule);color:var(--ink-2);text-transform:uppercase}
.tag.warn{color:var(--warn);border-color:var(--warn)}
.tag.ok{color:var(--ok);border-color:var(--ok)}

.btns{display:flex;flex-wrap:wrap;gap:8px;align-items:center}
.btn{font-family:var(--mono);font-size:11.5px;font-weight:500;letter-spacing:.06em;
  text-transform:uppercase;padding:8px 14px;border:1px solid var(--ink);
  background:var(--paper);color:var(--ink);cursor:pointer;transition:all .1s;white-space:nowrap}
.btn:hover{background:var(--ink);color:var(--paper)}
.btn.primary{background:var(--red);border-color:var(--red);color:var(--paper)}
.btn.primary:hover{background:var(--red-2);border-color:var(--red-2)}
.btn.ghost{background:transparent;border-color:var(--rule-2);color:var(--ink-2)}
.btn.ghost:hover{border-color:var(--ink);background:var(--paper-2);color:var(--ink)}
.btn[disabled]{opacity:.4;pointer-events:none}
.btn.icon{padding:8px 10px}

.switch{display:inline-flex;align-items:center;gap:8px;font-family:var(--mono);
  font-size:11px;color:var(--ink-3);letter-spacing:.06em;text-transform:uppercase;
  cursor:pointer;user-select:none;padding:8px 12px;border:1px solid var(--rule);background:var(--paper)}
.switch:hover{color:var(--ink)}
.switch input{appearance:none;width:0;height:0;position:absolute}
.switch .box{width:14px;height:14px;border:1px solid var(--ink-2);background:var(--paper);
  position:relative;transition:all .12s}
.switch input:checked ~ .box{background:var(--red);border-color:var(--red)}
.switch input:checked ~ .box::after{content:"";position:absolute;left:3.5px;top:0.5px;
  width:4px;height:8px;border:solid var(--paper);
  border-width:0 2px 2px 0;transform:rotate(45deg)}
.switch input:checked ~ .txt{color:var(--ink)}
.switch input:focus-visible ~ .box{outline:2px solid var(--red);outline-offset:2px}

.formats{width:100%;border-collapse:collapse;font-family:var(--mono);font-size:12px}
.formats thead th{text-align:left;padding:10px 10px;border-bottom:1.5px solid var(--ink);
  font-size:10px;font-weight:500;letter-spacing:.14em;color:var(--ink-3);
  text-transform:uppercase;background:var(--paper);position:sticky;top:80px;z-index:2;
  cursor:pointer;user-select:none}
.formats thead th:hover{color:var(--ink)}
.formats thead th.sorted::after{content:" ↓";color:var(--red)}
.formats thead th.sorted.asc::after{content:" ↑"}
.formats tbody tr{border-bottom:1px solid var(--rule);cursor:pointer;transition:background .1s}
.formats tbody tr:hover{background:var(--paper-2)}
.formats tbody tr.pick{background:rgba(178,58,44,.06);box-shadow:inset 3px 0 0 var(--red)}
.formats tbody tr.compare{background:rgba(45,74,124,.06);box-shadow:inset 3px 0 0 var(--navy)}
.formats td{padding:calc(10px * var(--density)) 10px;vertical-align:middle;color:var(--ink-2)}
.formats td.lead{color:var(--ink);font-weight:500}
.formats td.num{text-align:right;font-variant-numeric:tabular-nums}
.formats .kind{display:inline-block;font-size:9.5px;letter-spacing:.1em;padding:2px 6px;
  font-weight:500;border:1px solid}
.kind.combined{color:var(--ok);border-color:var(--ok)}
.kind.video{color:var(--red);border-color:var(--red)}
.kind.audio{color:var(--navy);border-color:var(--navy)}
.hdr-tag,.lang-tag,.dupe-tag{display:inline-block;font-size:9px;letter-spacing:.08em;
  padding:1px 5px;margin-left:6px;border:1px solid}
.hdr-tag{color:var(--err);border-color:var(--err)}
.lang-tag{color:var(--ink-3);border-color:var(--rule-2)}
.dupe-tag{color:var(--ok);border-color:var(--ok)}
.codec-h264{color:#A57820} .codec-h265{color:#8C2A1E}
.codec-vp9{color:#2D4A7C} .codec-av1{color:#7A3A7C}
.br-bar{display:inline-block;width:54px;height:6px;background:var(--paper-2);
  border:1px solid var(--rule);position:relative;vertical-align:middle;margin-right:8px}
.br-bar i{display:block;height:100%;background:var(--ink-2)}

.preview-grid{display:grid;grid-template-columns:1fr 1fr;gap:24px;margin-top:28px;
  padding-top:28px;border-top:1px solid var(--rule)}
@media (max-width:760px){.preview-grid{grid-template-columns:1fr}}
.prev-card h4{font-family:var(--serif);font-size:16px;font-weight:600;margin:0 0 4px;letter-spacing:-.01em}
.prev-card .sub{font-family:var(--mono);font-size:10.5px;color:var(--ink-3);
  letter-spacing:.06em;margin-bottom:14px;text-transform:uppercase}
.ch-list{max-height:220px;overflow-y:auto;border-top:1px solid var(--rule)}
.ch-row{display:grid;grid-template-columns:auto auto 1fr auto;gap:12px;align-items:baseline;
  padding:8px 4px;border-bottom:1px solid var(--rule);font-family:var(--mono);
  font-size:11.5px;color:var(--ink-2)}
.ch-row:hover{background:var(--paper-2)}
.ch-row input{accent-color:var(--red);width:13px;height:13px;cursor:pointer}
.ch-row .idx{color:var(--ink-3);font-size:10px;letter-spacing:.05em}
.ch-row .nm{color:var(--ink);overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.ch-row .tm{color:var(--ink-3);font-size:11px}
.chips{display:flex;flex-wrap:wrap;gap:6px;max-height:220px;overflow-y:auto;
  border-top:1px solid var(--rule);padding-top:12px}
.lang-chip{font-family:var(--mono);font-size:11px;padding:4px 9px;
  border:1px solid var(--rule);background:var(--paper);color:var(--ink-2);
  cursor:pointer;transition:all .1s;user-select:none}
.lang-chip:hover{border-color:var(--ink)}
.lang-chip.on{background:var(--red);border-color:var(--red);color:var(--paper)}
.lang-chip.auto{border-style:dashed}
.lang-chip.auto.on{background:var(--navy);border-color:var(--navy)}
.lang-chip .ext{opacity:.65;margin-left:5px;font-size:10px}
.trim-wrap{display:flex;align-items:center;gap:10px;flex-wrap:wrap;
  font-family:var(--mono);font-size:11.5px;color:var(--ink-3);margin-top:12px}
.trim-wrap input,.trim-wrap select{font-family:var(--mono);font-size:12px;
  background:var(--paper);border:1px solid var(--rule);padding:5px 8px;
  color:var(--ink);width:100px;outline:0}
.trim-wrap input:focus,.trim-wrap select:focus{border-color:var(--ink)}

.aside-sect{margin-bottom:36px}
.aside-sect h3{font-family:var(--serif);font-size:16px;font-weight:600;
  letter-spacing:-.01em;margin:0 0 4px}
.aside-sect .sub{font-family:var(--mono);font-size:10px;letter-spacing:.14em;
  color:var(--ink-3);text-transform:uppercase;margin-bottom:12px}
.aside-sect hr{border:0;border-top:1px solid var(--ink);margin:0 0 14px}
.kv{display:grid;grid-template-columns:auto 1fr;gap:6px 16px;
  font-family:var(--mono);font-size:11.5px}
.kv dt{color:var(--ink-3);letter-spacing:.03em}
.kv dd{color:var(--ink);text-align:right;margin:0;white-space:nowrap;
  overflow:hidden;text-overflow:ellipsis;font-variant-numeric:tabular-nums}
.kv dd.ok{color:var(--ok)} .kv dd.warn{color:var(--warn)}
.kv dd.err{color:var(--err)} .kv dd.dim{color:var(--ink-3)}

.q-mini{border:1px solid var(--rule);background:var(--paper-2);padding:calc(12px * var(--density))}
.q-mini + .q-mini{margin-top:8px}
.q-mini .t{font-size:12.5px;color:var(--ink);line-height:1.3;
  display:-webkit-box;-webkit-line-clamp:2;-webkit-box-orient:vertical;overflow:hidden}
.q-mini .s{display:flex;justify-content:space-between;align-items:center;
  font-family:var(--mono);font-size:10px;color:var(--ink-3);letter-spacing:.06em;
  margin-top:8px;text-transform:uppercase}
.q-mini .st{padding:1px 6px;border:1px solid currentColor}
.q-mini .st.running{color:var(--red)} .q-mini .st.done{color:var(--ok)}
.q-mini .st.error{color:var(--err)} .q-mini .st.queued{color:var(--ink-3)}
.q-mini .st.cancelled{color:var(--ink-3)}
.q-mini .bar{height:2px;background:var(--rule);margin-top:8px;position:relative;overflow:hidden}
.q-mini .bar i{display:block;height:100%;background:var(--red);width:0%;transition:width .3s}

.search-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(220px,1fr));gap:22px}
.card{cursor:pointer;position:relative;transition:transform .14s}
.card:hover{transform:translateY(-3px)}
.card .thumb{aspect-ratio:16/9;border:1px solid var(--rule);position:relative;
  background:#1A1815 center/cover}
.card .row-star{position:absolute;top:6px;right:6px;z-index:2;width:26px;height:26px;
  background:var(--paper);border:1px solid var(--rule);display:grid;place-items:center;
  font-size:13px;color:var(--ink-3);cursor:pointer;transition:all .12s}
.card .row-star:hover{border-color:var(--ink)}
.card .row-star.on{background:var(--red);color:var(--paper);border-color:var(--red)}
.card .row-check{position:absolute;top:6px;left:6px;z-index:2;width:22px;height:22px;
  background:var(--paper);border:1px solid var(--rule);display:grid;place-items:center;
  font-size:12px;color:transparent;cursor:pointer;transition:all .12s}
.card .row-check.on{background:var(--navy);color:var(--paper);border-color:var(--navy)}
.card .title{font-size:13.5px;line-height:1.35;color:var(--ink);margin-top:10px;
  display:-webkit-box;-webkit-line-clamp:2;-webkit-box-orient:vertical;overflow:hidden}
.card .byline{font-family:var(--mono);font-size:10.5px;color:var(--ink-3);
  margin-top:5px;letter-spacing:.03em}

.bulk-bar{display:none;align-items:center;gap:14px;padding:12px 16px;
  border:1.5px solid var(--navy);background:rgba(45,74,124,.05);margin-bottom:20px;
  font-family:var(--mono);font-size:12px;color:var(--ink)}
.bulk-bar.on{display:flex}
.bulk-bar .n{color:var(--navy);font-weight:700}

.q-item{display:grid;grid-template-columns:auto 56px 1fr auto;gap:16px;align-items:center;
  padding:16px 0;border-bottom:1px solid var(--rule);cursor:grab}
.q-item:last-child{border-bottom:0}
.q-item.dragging{opacity:.4}
.q-item.drop-target{box-shadow:inset 0 3px 0 var(--red)}
.q-item .grab{font-family:var(--mono);color:var(--ink-3);font-size:14px;cursor:grab;user-select:none}
.ring{position:relative;width:56px;height:56px;flex-shrink:0}
.ring svg{transform:rotate(-90deg)}
.ring circle{fill:none;stroke-width:2.5}
.ring .track{stroke:var(--rule)}
.ring .arc{stroke:var(--red);stroke-linecap:butt;transition:stroke-dashoffset .35s ease}
.ring.done .arc{stroke:var(--ok)} .ring.err .arc{stroke:var(--err)}
.ring .pct{position:absolute;inset:0;display:grid;place-items:center;
  font-family:var(--mono);font-size:10px;color:var(--ink-2);font-variant-numeric:tabular-nums}
.q-body .t{font-size:14px;color:var(--ink);line-height:1.35;margin-bottom:4px}
.q-body .m{font-family:var(--mono);font-size:11px;color:var(--ink-3);
  letter-spacing:.04em;display:flex;gap:14px;flex-wrap:wrap}
.q-body .m b{color:var(--ink-2);font-weight:500}
.q-act{display:flex;gap:6px;flex-wrap:wrap;justify-content:flex-end}

.day-head{font-family:var(--mono);font-size:10.5px;letter-spacing:.16em;
  color:var(--ink-3);text-transform:uppercase;border-top:1px solid var(--ink);
  padding-top:12px;margin:28px 0 12px;display:flex;justify-content:space-between}
.day-head:first-child{margin-top:0}
.h-item{display:grid;grid-template-columns:1fr auto;gap:22px;align-items:center;
  padding:12px 0;border-bottom:1px solid var(--rule)}
.h-item.selected{background:rgba(45,74,124,.05);box-shadow:inset 3px 0 0 var(--navy)}
.h-item .t{color:var(--ink);font-size:13.5px;line-height:1.35;
  overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.h-item .m{font-family:var(--mono);font-size:10.5px;color:var(--ink-3);
  margin-top:4px;letter-spacing:.04em;display:flex;gap:14px;flex-wrap:wrap}
.h-item .m .st.ok{color:var(--ok)} .h-item .m .st.err{color:var(--err)}
.h-item .m .st.cn{color:var(--ink-3)} .h-item .m .keep{color:var(--warn)}
.h-item .m .rating{color:var(--warn);letter-spacing:.1em}
.h-item .acts{display:flex;gap:6px;flex-wrap:wrap;justify-content:flex-end}

.stat-row{display:grid;grid-template-columns:repeat(4,1fr);gap:0;
  border:1.5px solid var(--ink);margin-bottom:34px}
@media (max-width:760px){.stat-row{grid-template-columns:1fr 1fr}}
.stat-box{padding:20px 22px;border-right:1px solid var(--rule)}
.stat-box:last-child{border-right:0}
@media (max-width:760px){
  .stat-box:nth-child(2){border-right:0}
  .stat-box:nth-child(1),.stat-box:nth-child(2){border-bottom:1px solid var(--rule)}
}
.stat-box .k{font-family:var(--mono);font-size:10px;letter-spacing:.14em;
  color:var(--ink-3);text-transform:uppercase;margin-bottom:12px}
.stat-box .v{font-family:var(--serif);font-size:38px;font-weight:600;
  letter-spacing:-.03em;line-height:1;color:var(--ink);font-variant-numeric:tabular-nums}
.stat-box .v.red{color:var(--red)} .stat-box .v.navy{color:var(--navy)}

.chart-wrap{margin-top:10px}
.chart{display:flex;align-items:flex-end;gap:2px;height:200px;
  border-bottom:1px solid var(--ink);padding-bottom:4px}
.chart .bar-col{flex:1;min-width:6px;height:100%;display:flex;
  flex-direction:column;justify-content:flex-end;position:relative}
.chart .bar{background:var(--ink-2);transition:height .5s ease;min-height:2px;
  position:relative;cursor:help}
.chart .bar:hover{background:var(--red)}
.chart .bar::after{content:attr(data-v);position:absolute;bottom:100%;left:50%;
  transform:translateX(-50%) translateY(-6px);background:var(--ink);color:var(--paper);
  font-family:var(--mono);font-size:10.5px;padding:4px 8px;white-space:nowrap;
  opacity:0;pointer-events:none;transition:opacity .12s;z-index:5}
.chart .bar:hover::after{opacity:1}
.chart-x{display:flex;gap:2px;font-family:var(--mono);font-size:9px;
  color:var(--ink-3);margin-top:6px}
.chart-x .lbl{flex:1;min-width:6px;text-align:center;writing-mode:vertical-lr;
  transform:rotate(180deg);height:36px;overflow:hidden}

.kind-row{display:grid;grid-template-columns:150px 1fr auto;gap:14px;align-items:center;
  padding:8px 0;border-bottom:1px solid var(--rule);font-family:var(--mono);
  font-size:11.5px;color:var(--ink-2)}
.kind-row .n{color:var(--ink);overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.kind-row .track{height:5px;background:var(--paper-2);border:1px solid var(--rule);position:relative}
.kind-row .track i{display:block;height:100%;background:var(--ink-2)}

.heatmap{display:grid;grid-template-columns:auto repeat(24,1fr);gap:2px;
  font-family:var(--mono);font-size:9px;color:var(--ink-3);margin-top:10px}
.heatmap .cell{aspect-ratio:1;background:var(--paper-2);border:1px solid var(--rule);
  min-width:12px;position:relative}
.heatmap .cell[data-v="1"]{background:rgba(178,58,44,.2)}
.heatmap .cell[data-v="2"]{background:rgba(178,58,44,.4)}
.heatmap .cell[data-v="3"]{background:rgba(178,58,44,.7)}
.heatmap .cell[data-v="4"]{background:var(--red)}
.heatmap .lbl{display:flex;align-items:center;font-size:9px;color:var(--ink-3);padding-right:6px}
.heatmap .hour{text-align:center;font-size:8px;color:var(--ink-3)}

.fav-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(240px,1fr));gap:24px}
.fav-card .thumb{aspect-ratio:16/9;border:1px solid var(--rule);
  background:#1A1815 center/cover;position:relative}
.fav-card .t{font-size:13.5px;line-height:1.35;color:var(--ink);margin-top:10px;
  display:-webkit-box;-webkit-line-clamp:2;-webkit-box-orient:vertical;overflow:hidden}
.fav-card .m{font-family:var(--mono);font-size:10.5px;color:var(--ink-3);
  margin-top:5px;letter-spacing:.04em}
.fav-card .row{display:flex;gap:6px;margin-top:10px;flex-wrap:wrap}

.library-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(220px,1fr));gap:22px}
.lib-card{position:relative;cursor:pointer;transition:transform .14s}
.lib-card:hover{transform:translateY(-3px)}
.lib-card .thumb{aspect-ratio:16/9;border:1px solid var(--rule);background:#1A1815 center/cover;position:relative}
.lib-card .watched-badge{position:absolute;bottom:6px;left:6px;background:rgba(0,0,0,.75);
  color:#7FB26B;font-family:var(--mono);font-size:10px;padding:2px 6px}
.lib-card .rating-badge{position:absolute;top:6px;left:6px;background:rgba(0,0,0,.75);
  color:#FFD740;font-family:var(--mono);font-size:11px;padding:2px 6px;letter-spacing:.1em}
.lib-card .t{font-size:13px;line-height:1.35;color:var(--ink);margin-top:10px;
  display:-webkit-box;-webkit-line-clamp:2;-webkit-box-orient:vertical;overflow:hidden}
.lib-card .m{font-family:var(--mono);font-size:10.5px;color:var(--ink-3);
  margin-top:5px;letter-spacing:.04em;display:flex;gap:10px;flex-wrap:wrap}
.lib-card .tags{display:flex;gap:4px;flex-wrap:wrap;margin-top:6px}
.lib-card .tag-chip{font-family:var(--mono);font-size:9.5px;padding:1px 6px;
  border:1px solid var(--rule-2);color:var(--ink-3);text-transform:uppercase}

.modal-wrap{position:fixed;inset:0;background:rgba(26,24,21,.45);backdrop-filter:blur(3px);
  z-index:80;display:none;align-items:center;justify-content:center;padding:24px}
.modal-wrap.show{display:flex}
.modal{width:100%;max-width:680px;background:var(--paper);border:1.5px solid var(--ink);
  box-shadow:8px 8px 0 var(--ink);max-height:88vh;display:flex;flex-direction:column;
  animation:pop .22s cubic-bezier(.3,.9,.4,1) both}
.modal.wide{max-width:900px}
@keyframes pop{from{transform:scale(.97) translateY(8px);opacity:0}to{transform:none;opacity:1}}
.modal .mh{display:flex;align-items:center;gap:14px;padding:16px 20px;
  border-bottom:1.5px solid var(--ink);font-family:var(--mono);font-size:11px;
  letter-spacing:.16em;text-transform:uppercase;color:var(--ink-3)}
.modal .mh .x{margin-left:auto;background:none;border:0;color:var(--ink-3);
  cursor:pointer;font-size:16px;padding:0 4px;font-family:var(--mono)}
.modal .mh .x:hover{color:var(--ink)}
.modal .mb{padding:24px;overflow-y:auto}
.modal .stage{font-family:var(--mono);font-size:12px;color:var(--red);
  letter-spacing:.06em;text-transform:uppercase;margin-bottom:6px}
.modal .title{font-family:var(--serif);font-size:20px;font-weight:600;
  letter-spacing:-.02em;line-height:1.2;margin-bottom:20px;color:var(--ink);
  display:-webkit-box;-webkit-line-clamp:2;-webkit-box-orient:vertical;overflow:hidden}

.meter{position:relative;height:14px;background:var(--paper-2);border:1px solid var(--ink);overflow:hidden}
.meter .fill{position:absolute;top:0;left:0;bottom:0;width:0%;
  background:var(--red);transition:width .25s ease}
.meter .fill::after{content:"";position:absolute;right:0;top:0;bottom:0;width:2px;background:var(--ink)}
.stat-grid{display:grid;grid-template-columns:repeat(4,1fr);gap:0;margin-top:20px;border:1px solid var(--ink)}
@media (max-width:520px){.stat-grid{grid-template-columns:1fr 1fr}}
.stat-grid .cell{padding:12px 14px;border-right:1px solid var(--rule)}
.stat-grid .cell:last-child{border-right:0}
@media (max-width:520px){
  .stat-grid .cell:nth-child(2){border-right:0}
  .stat-grid .cell:nth-child(1),.stat-grid .cell:nth-child(2){border-bottom:1px solid var(--rule)}
}
.stat-grid .k{font-family:var(--mono);font-size:9px;letter-spacing:.14em;
  color:var(--ink-3);text-transform:uppercase;margin-bottom:6px}
.stat-grid .v{font-family:var(--mono);font-size:14px;color:var(--ink);
  font-variant-numeric:tabular-nums;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}

.spark{width:100%;height:56px;border:1px solid var(--rule);
  background:var(--paper-2);margin-top:20px;display:block}
.spark .sp-area{fill:rgba(178,58,44,.12);stroke:none}
.spark .sp-line{fill:none;stroke:var(--red);stroke-width:1.4}

.mlog{margin-top:20px;max-height:140px;overflow-y:auto;border:1px solid var(--rule);
  background:var(--paper-2);padding:10px 14px;font-family:var(--mono);font-size:11px;line-height:1.55}
.mlog .ln{display:grid;grid-template-columns:56px 1fr;gap:12px}
.mlog .ts{color:var(--ink-3)} .mlog .tx{color:var(--ink-2)}
.mlog .ln.warn .tx{color:var(--warn)} .mlog .ln.err .tx{color:var(--err)}

.modal .mfoot{display:flex;gap:10px;justify-content:flex-end;margin-top:22px;flex-wrap:wrap}

.palette-wrap{position:fixed;inset:0;background:rgba(26,24,21,.5);z-index:100;
  display:none;align-items:flex-start;justify-content:center;padding-top:16vh}
.palette-wrap.show{display:flex}
.palette{width:min(560px,92vw);background:var(--paper);border:1.5px solid var(--ink);
  box-shadow:6px 6px 0 var(--ink)}
.palette .input{display:flex;align-items:center;border-bottom:1.5px solid var(--ink);padding:0 20px}
.palette .input .sym{color:var(--red);font-family:var(--serif);font-style:italic;font-size:18px;margin-right:14px}
.palette input{flex:1;background:transparent;border:0;outline:0;color:var(--ink);
  font-family:var(--mono);font-size:14px;padding:18px 0;caret-color:var(--red)}
.palette .results{max-height:340px;overflow-y:auto;padding:8px}
.palette .item{display:grid;grid-template-columns:auto 1fr auto;gap:14px;
  align-items:baseline;padding:10px 14px;cursor:pointer;font-family:var(--mono);
  font-size:12.5px;color:var(--ink-2)}
.palette .item:hover,.palette .item.sel{background:var(--paper-2);color:var(--ink)}
.palette .item.sel{box-shadow:inset 3px 0 0 var(--red)}
.palette .item .k{color:var(--ink-3);font-size:10.5px;letter-spacing:.06em;text-transform:uppercase}
.palette .empty{padding:36px;text-align:center;color:var(--ink-3);
  font-family:var(--mono);font-size:11px;letter-spacing:.04em}

.statusbar{position:fixed;bottom:0;left:0;right:0;background:var(--ink);
  color:var(--paper);font-family:var(--mono);font-size:11px;padding:8px 40px;
  display:flex;gap:24px;align-items:center;z-index:30;letter-spacing:.04em}
.statusbar .live{display:flex;align-items:center;gap:8px}
.statusbar .live .dot{width:6px;height:6px;border-radius:50%;background:var(--red);
  animation:blip 1.1s ease-in-out infinite}
.statusbar .live .dot.idle{background:var(--ink-3);animation:none}
.statusbar .spacer{flex:1}
.statusbar .tick{color:rgba(242,238,227,.75);max-width:44ch;overflow:hidden;
  white-space:nowrap;text-overflow:ellipsis}
.statusbar .tick b{color:var(--paper)}
.statusbar .right{display:flex;gap:20px;color:rgba(242,238,227,.75)}
.statusbar .right b{color:var(--paper);font-weight:500}
.statusbar .pause-btn{background:transparent;border:1px solid rgba(242,238,227,.3);
  color:var(--paper);font-family:var(--mono);font-size:10px;padding:2px 8px;cursor:pointer}
.statusbar .pause-btn:hover{border-color:var(--paper)}

.console{position:fixed;left:0;right:0;bottom:34px;background:var(--paper-2);
  border-top:1.5px solid var(--ink);font-family:var(--mono);font-size:11px;
  z-index:25;max-height:0;overflow:hidden;transition:max-height .28s ease}
.console.open{max-height:min(50vh,420px)}
.console .body{overflow-y:auto;padding:16px 40px;height:min(50vh,420px)}
.console .ln{display:grid;grid-template-columns:56px 1fr;gap:12px;padding:1px 0;line-height:1.6}
.console .ts{color:var(--ink-3)} .console .tx{color:var(--ink-2)}
.console .ln.warn .tx{color:var(--warn)} .console .ln.err .tx{color:var(--err)}

.toasts{position:fixed;bottom:60px;right:24px;z-index:95;
  display:flex;flex-direction:column;gap:8px;align-items:flex-end;pointer-events:none;
  max-width:min(420px,90vw)}
.toast{background:var(--paper);border:1.5px solid var(--ink);padding:11px 16px;
  font-family:var(--mono);font-size:12px;color:var(--ink);box-shadow:4px 4px 0 var(--ink);
  animation:tin .2s ease both;pointer-events:auto;display:flex;gap:14px;align-items:center}
.toast.err{border-color:var(--err);color:var(--err);box-shadow:4px 4px 0 var(--err)}
.toast.ok{border-color:var(--ok);color:var(--ok);box-shadow:4px 4px 0 var(--ok)}
.toast.warn{border-color:var(--warn);color:var(--warn);box-shadow:4px 4px 0 var(--warn)}
.toast .undo{margin-left:auto;background:transparent;border:1px solid currentColor;
  color:inherit;font-family:var(--mono);font-size:10.5px;padding:2px 8px;cursor:pointer;
  text-transform:uppercase;letter-spacing:.06em}
.toast .undo:hover{background:currentColor;filter:brightness(1.2)}
@keyframes tin{from{opacity:0;transform:translateY(6px)}to{opacity:1;transform:none}}

.empty{padding:56px 20px;text-align:center;color:var(--ink-3);
  font-family:var(--mono);font-size:11.5px;letter-spacing:.08em;text-transform:uppercase}
.empty svg{display:block;margin:0 auto 16px;opacity:.35;color:var(--ink-3)}

.skel{border:1.5px solid var(--rule);padding:28px;margin-bottom:20px}
.skel .bar-line{height:14px;background:var(--paper-2);margin-bottom:12px;
  position:relative;overflow:hidden}
.skel .bar-line:nth-child(1){width:70%}
.skel .bar-line:nth-child(2){width:45%}
.skel .bar-line:nth-child(3){width:85%}
.skel .shim{position:absolute;inset:0;
  background:linear-gradient(90deg,transparent,rgba(178,58,44,.08),transparent);
  animation:shim 1.4s linear infinite}
@keyframes shim{from{transform:translateX(-100%)}to{transform:translateX(100%)}}

.dialog-wrap{position:fixed;inset:0;background:rgba(26,24,21,.5);z-index:110;
  display:none;align-items:center;justify-content:center;padding:24px}
.dialog-wrap.show{display:flex}
.dialog{background:var(--paper);border:1.5px solid var(--ink);
  box-shadow:6px 6px 0 var(--ink);max-width:460px;width:100%;padding:26px;
  max-height:90vh;overflow-y:auto}
.dialog h3{font-family:var(--serif);font-size:20px;font-weight:600;
  margin:0 0 10px;letter-spacing:-.02em}
.dialog p{font-size:13.5px;color:var(--ink-2);margin:0 0 20px;line-height:1.5}
.dialog input,.dialog select,.dialog textarea{font-family:var(--mono);font-size:13px;
  background:var(--paper-2);border:1.5px solid var(--ink);padding:10px 12px;
  color:var(--ink);outline:0;width:100%;margin-bottom:14px}
.dialog textarea{resize:vertical;min-height:80px}
.dialog label{display:block;font-family:var(--mono);font-size:11px;
  letter-spacing:.1em;text-transform:uppercase;color:var(--ink-3);margin-bottom:6px}
.dialog .row{display:flex;gap:8px;justify-content:flex-end;margin-top:8px}

.bookmarklet-box{border:1.5px solid var(--ink);background:var(--paper-2);
  padding:16px;font-family:var(--mono);font-size:11.5px;word-break:break-all;
  margin:14px 0;line-height:1.5;color:var(--ink-2);user-select:all;cursor:text}
.bookmarklet-drag{display:inline-block;background:var(--red);color:var(--paper);
  padding:10px 20px;font-family:var(--mono);font-size:12px;text-decoration:none;
  border:1.5px solid var(--ink);box-shadow:3px 3px 0 var(--ink);
  cursor:grab;user-select:none;letter-spacing:.06em;text-transform:uppercase}
.bookmarklet-drag:hover{transform:translate(1px,1px);box-shadow:2px 2px 0 var(--ink)}

.tooltip{position:relative}
.tooltip::after{content:attr(data-tip);position:absolute;bottom:100%;left:50%;
  transform:translateX(-50%) translateY(-6px);background:var(--ink);color:var(--paper);
  font-family:var(--mono);font-size:10.5px;padding:4px 8px;white-space:nowrap;
  opacity:0;pointer-events:none;transition:opacity .12s;z-index:100;
  border:1px solid var(--paper)}
.tooltip:hover::after{opacity:1}

.preview-modal-video{width:100%;aspect-ratio:16/9;background:#000;border:1px solid var(--ink)}
.notes-area{width:100%;min-height:100px;font-family:var(--mono);font-size:12px;
  background:var(--paper-2);border:1px solid var(--rule);padding:10px;color:var(--ink);
  outline:0;resize:vertical}
.notes-area:focus{border-color:var(--ink)}
.tag-input{display:flex;gap:6px;flex-wrap:wrap;margin-bottom:14px}
.tag-input .t{display:inline-flex;align-items:center;gap:6px;font-family:var(--mono);
  font-size:11px;padding:3px 8px;border:1px solid var(--rule-2);color:var(--ink-2);text-transform:uppercase}
.tag-input .t .rm{cursor:pointer;color:var(--ink-3);font-size:13px}
.tag-input .t .rm:hover{color:var(--err)}
.tag-input input{font-family:var(--mono);font-size:11px;background:var(--paper-2);
  border:1px solid var(--rule);padding:3px 8px;color:var(--ink);outline:0;width:100px}
.tag-input input:focus{border-color:var(--ink)}
.rating-row{display:flex;gap:4px;font-size:22px;color:var(--rule-2);margin-bottom:14px}
.rating-row .star{cursor:pointer;transition:color .12s}
.rating-row .star.on{color:var(--warn)}
.rating-row .star:hover{color:var(--warn)}

@media (prefers-reduced-motion: reduce){
  *{animation-duration:.001ms !important;transition-duration:.001ms !important}
  body::before{display:none}
}
@media (max-width:900px){
  .masthead{padding:14px 22px 12px;gap:22px}
  .masthead .brand{font-size:22px}
  .masthead nav{gap:16px;font-size:10.5px}
  .hero{padding:36px 22px 12px}
  .command,.presets,.sheet{padding-left:22px;padding-right:22px}
  .statusbar{padding:8px 22px;font-size:10px;gap:14px}
  .statusbar .right{gap:12px}
  .console .body{padding:14px 22px}
}
@media (max-width:680px){
  .formats .col-fps,.formats .col-vc,.formats .col-ac,.formats .col-br{display:none}
  .statusbar .right span.long{display:none}
  .masthead nav a span.lbl{display:none}
}
</style>
</head>
<body>

<a class="skip-link" href="#main">Skip to content</a>
<div aria-live="polite" class="sr-only" id="srStatus"></div>

<header class="masthead">
  <div class="brand">Grab<span class="amp">&amp;</span>Tube</div>
  <nav id="nav" aria-label="Main navigation">
    <a data-v="download" class="active">Download</a>
    <a data-v="search">Search</a>
    <a data-v="channel">Channel</a>
    <a data-v="library">Library</a>
    <a data-v="subscriptions">Subs</a>
    <a data-v="queue">Queue<span id="navQN" style="margin-left:6px;color:var(--red)"></span></a>
    <a data-v="favorites">Favorites</a>
    <a data-v="history">History</a>
    <a data-v="stats">Stats</a>
  </nav>
  <div class="status">
    <select class="profile-select" id="profileSelect" onchange="applyProfile(this.value)" aria-label="Profile">
      <option value="">— default —</option>
    </select>
    <span><span class="dot" id="stFFdot"></span>ffmpeg</span>
    <span><span class="dot" id="stWSdot"></span>live</span>
    <span class="tools">
      <button class="tool" onclick="cycleTheme()" id="themeBtn" title="Cycle theme (paper/dark/sepia/high-contrast)">◐</button>
      <button class="tool" onclick="cycleDensity()" id="densityBtn" title="Cycle density (compact/cozy/roomy)">≡</button>
      <button class="tool" onclick="cycleScale()" id="scaleBtn" title="Cycle font scale">Aa</button>
      <button class="tool" onclick="toggleFocus()" id="focusBtn" title="Focus mode">◉</button>
      <button class="tool" onclick="showBookmarklet()" title="Bookmarklet">B</button>
      <button class="tool" onclick="openSettings()" title="Settings">⚙</button>
    </span>
  </div>
</header>

<section class="hero" id="hero">
  <div class="kicker">Paste · Inspect · Download</div>
  <h1>Grab anything.<br>Keep it <em>exactly how you want it</em>.</h1>
  <p>Paste one link or a wall of them. Inspect formats, chapters, subtitles. Trim ranges. Save presets. Everything lives locally.</p>
</section>

<section class="command" id="commandShell">
  <div class="frame" id="cmdFrame">
    <span class="glyph" aria-hidden="true">›</span>
    <input id="urlInput" type="text"
      placeholder="https://www.youtube.com/watch?v=… (or paste many at once)"
      autocomplete="off" spellcheck="false" aria-label="Video or playlist URL"
      aria-autocomplete="list" aria-controls="urlAutocomplete">
    <button id="fetchBtn" onclick="cmdGo()">Fetch</button>
    <div class="autocomplete" id="urlAutocomplete" role="listbox"></div>
  </div>
  <div class="hint">
    <span><kbd>/</kbd> focus · <kbd>⌘K</kbd> commands · <kbd>Esc</kbd> clear · drop URLs anywhere · <kbd>d</kbd> dark</span>
  </div>
</section>

<section class="presets" id="presetShell">
  <span class="label">presets</span>
  <span id="presetList"></span>
  <span class="pill ghost" onclick="savePreset()">+ save current</span>
</section>

<main class="sheet" id="main">
  <div id="viewContainer">
    <section class="view" data-view="download"><div id="results"></div></section>

    <section class="view" data-view="search" style="display:none">
      <div class="sect">
        <div class="sect-head"><span class="num">01</span><h2>Search</h2>
          <span class="right" id="sResults"></span></div>
        <div class="command" style="padding:0;max-width:none;margin:0 0 20px 0">
          <div class="frame" style="box-shadow:none">
            <span class="glyph" aria-hidden="true">⌕</span>
            <input id="searchInput" placeholder="search youtube…" autocomplete="off" aria-label="Search YouTube">
            <button onclick="doSearch()">Search</button>
          </div>
        </div>
        <div class="bulk-bar" id="bulkBar">
          <span><span class="n" id="bulkN">0</span> selected</span>
          <span style="flex:1"></span>
          <button class="btn ghost" onclick="bulkClear()">Clear</button>
          <button class="btn primary" onclick="bulkQueue()">Queue all</button>
        </div>
        <div id="searchResults"><div class="empty">search to begin</div></div>
      </div>
    </section>

    <section class="view" data-view="channel" style="display:none">
      <div class="sect">
        <div class="sect-head"><span class="num">02</span><h2>Channel</h2>
          <span class="right" id="chRight"></span></div>
        <div class="command" style="padding:0;max-width:none;margin:0 0 20px 0">
          <div class="frame" style="box-shadow:none">
            <span class="glyph" aria-hidden="true">▤</span>
            <input id="channelInput" placeholder="https://youtube.com/@handle" autocomplete="off" aria-label="Channel URL">
            <button onclick="loadChannel()">Load</button>
          </div>
        </div>
        <div class="trim-wrap" style="margin-bottom:20px">
          <span>Regex filter</span>
          <input id="chFilter" placeholder="e.g. episode \d+" style="width:220px" aria-label="Regex filter">
          <button class="btn ghost" onclick="filterChannel()">Apply</button>
          <button class="btn ghost" onclick="queueChannel()">Queue all shown</button>
          <button class="btn primary" onclick="queueChannelRegex()">Queue matches only</button>
          <button class="btn" onclick="subscribeChannel()">Subscribe</button>
        </div>
        <div id="channelResults"><div class="empty">paste a channel URL</div></div>
      </div>
    </section>

    <section class="view" data-view="library" style="display:none">
      <div class="sect">
        <div class="sect-head"><span class="num">03</span><h2>Library</h2>
          <span class="right" id="libRight"></span></div>
        <div class="trim-wrap" style="margin-bottom:20px">
          <input id="libQ" placeholder="search library…" style="width:280px" oninput="debouncedLibrary()" aria-label="Search library">
          <select id="libSort" onchange="loadLibrary()" aria-label="Sort library">
            <option value="size">size (largest first)</option>
            <option value="">recent</option>
            <option value="rating">rating</option>
          </select>
          <input id="libTag" placeholder="tag filter…" style="width:140px" oninput="debouncedLibrary()" aria-label="Tag filter">
        </div>
        <div id="libraryResults"><div class="empty">loading…</div></div>
      </div>
    </section>

    <section class="view" data-view="subscriptions" style="display:none">
      <div class="sect">
        <div class="sect-head"><span class="num">04</span><h2>Subscriptions</h2>
          <span class="right" id="subRight"></span></div>
        <div class="command" style="padding:0;max-width:none;margin:0 0 20px 0">
          <div class="frame" style="box-shadow:none">
            <span class="glyph" aria-hidden="true">⊕</span>
            <input id="subInput" placeholder="channel or playlist URL to subscribe to" autocomplete="off" aria-label="Subscription URL">
            <button onclick="addSubscription()">Subscribe</button>
          </div>
        </div>
        <div id="subList"><div class="empty">no subscriptions</div></div>
      </div>
    </section>

    <section class="view" data-view="queue" style="display:none">
      <div class="sect">
        <div class="sect-head"><span class="num">05</span><h2>Active queue</h2>
          <span class="right" id="qRight"></span></div>
        <div id="queueList"><div class="empty">queue is empty</div></div>
      </div>
    </section>

    <section class="view" data-view="favorites" style="display:none">
      <div class="sect">
        <div class="sect-head"><span class="num">06</span><h2>Favorites</h2></div>
        <div id="favoritesGrid"><div class="empty">no favorites yet</div></div>
      </div>
    </section>

    <section class="view" data-view="history" style="display:none">
      <div class="sect">
        <div class="sect-head"><span class="num">07</span><h2>History</h2>
          <span class="right">
            <input id="histQ" placeholder="filter…" style="font-family:var(--mono);font-size:11px;background:transparent;border:0;border-bottom:1px solid var(--rule);outline:0;color:var(--ink);width:120px;padding:2px 0" oninput="debouncedHistory()" aria-label="Filter history">
            <select id="histS" onchange="loadHistory()" aria-label="Status filter">
              <option value="">all</option><option value="done">done</option>
              <option value="error">error</option><option value="running">running</option>
              <option value="cancelled">cancelled</option>
            </select>
            <select id="histSort" onchange="loadHistory()" aria-label="Sort history">
              <option value="">recent</option><option value="size">size</option>
              <option value="oldest">oldest</option><option value="rating">rating</option>
            </select>
            <button class="btn ghost" onclick="exportHistory()">Export</button>
            <button class="btn ghost" onclick="importHistory()">Import</button>
            <button class="btn ghost" id="batchModeBtn" onclick="toggleBatchMode()">Batch</button>
          </span></div>
        <div class="bulk-bar" id="histBulkBar">
          <span><span class="n" id="histBulkN">0</span> selected</span>
          <span style="flex:1"></span>
          <button class="btn ghost" onclick="histBulkClear()">Clear</button>
          <button class="btn ghost" onclick="histBulkKeep(true)">Pin all</button>
          <button class="btn ghost" onclick="histBulkKeep(false)">Unpin</button>
          <button class="btn" onclick="histBulkDelete()">Delete</button>
        </div>
        <div id="historyBody"><div class="empty">loading…</div></div>
      </div>
    </section>

    <section class="view" data-view="stats" style="display:none">
      <div class="sect">
        <div class="sect-head"><span class="num">08</span><h2>Numbers</h2></div>
        <div class="stat-row">
          <div class="stat-box"><div class="k">All-time files</div><div class="v" id="sCount">0</div></div>
          <div class="stat-box"><div class="k">All-time size</div><div class="v red" id="sSize">0</div></div>
          <div class="stat-box"><div class="k">This session</div><div class="v navy" id="sSession">0</div></div>
          <div class="stat-box"><div class="k">Daily average</div><div class="v" id="sAvg">0</div></div>
        </div>
      </div>
      <div class="sect">
        <div class="sect-head"><span class="num">09</span><h2>Activity heatmap</h2>
          <span class="right">by hour × weekday</span></div>
        <div class="heatmap" id="heatmap"></div>
      </div>
      <div class="sect">
        <div class="sect-head"><span class="num">10</span><h2>Last 30 days</h2>
          <span class="right" id="sDays">0 days</span></div>
        <div class="chart-wrap">
          <div class="chart" id="stChart"></div>
          <div class="chart-x" id="stChartX"></div>
        </div>
      </div>
      <div class="sect">
        <div class="sect-head"><span class="num">11</span><h2>By format</h2></div>
        <div id="stKinds"></div>
      </div>
      <div class="sect" id="tagsSect" style="display:none">
        <div class="sect-head"><span class="num">12</span><h2>Tags</h2></div>
        <div id="stTags"></div>
      </div>
    </section>
  </div>

  <aside class="side" id="side">
    <div class="aside-sect">
      <h3>Session</h3>
      <div class="sub">since process start</div>
      <hr>
      <dl class="kv">
        <dt>downloaded</dt><dd id="kBytes">0 B</dd>
        <dt>files</dt><dd id="kCount">0</dd>
        <dt>uptime</dt><dd id="kUptime">0s</dd>
      </dl>
    </div>
    <div class="aside-sect">
      <h3>System</h3>
      <div class="sub">local resources</div>
      <hr>
      <dl class="kv">
        <dt>ffmpeg</dt><dd id="kFF">—</dd>
        <dt>queue depth</dt><dd id="kQ">0</dd>
        <dt>slots</dt><dd id="kSlots">—</dd>
        <dt>disk free</dt><dd id="kDisk">—</dd>
        <dt>websockets</dt><dd id="kWs">—</dd>
      </dl>
    </div>
    <div class="aside-sect">
      <h3>Running</h3>
      <div class="sub">active jobs</div>
      <hr>
      <div id="kCurrent"><div style="font-family:var(--mono);font-size:11px;color:var(--ink-3)">idle</div></div>
    </div>
    <div class="aside-sect">
      <h3>Recent</h3>
      <div class="sub">last completed</div>
      <hr>
      <div id="kRecent"><div style="font-family:var(--mono);font-size:11px;color:var(--ink-3)">nothing yet</div></div>
    </div>
  </aside>
</main>

<section class="console" id="console" aria-label="Console output">
  <div class="body" id="consoleBody">
    <div class="ln"><span class="ts">--:--</span><span class="tx">ready.</span></div>
  </div>
</section>

<footer class="statusbar" id="statusbar">
  <div class="live" role="status" aria-live="polite"><span class="dot idle" id="sbDot"></span><span id="sbLive">idle</span></div>
  <div class="tick" id="sbTick">press <b>/</b> to focus · <b>⌘K</b> for commands · click bar for console</div>
  <div class="spacer"></div>
  <div class="right">
    <button class="pause-btn" id="pauseBtn" onclick="togglePause()">pause</button>
    <span class="long">queue <b id="sbQ">0</b></span>
    <span class="long">session <b id="sbSes">0 B</b></span>
    <span>ws <b id="sbWS">—</b></span>
  </div>
</footer>

<div class="modal-wrap" id="modalWrap" role="dialog" aria-modal="true" aria-labelledby="mStage">
  <div class="modal">
    <div class="mh">
      JOB <span id="mJob" style="color:var(--ink-2)"></span>
      <button class="x" onclick="closeModal()" aria-label="Close">✕</button>
    </div>
    <div class="mb">
      <div class="stage" id="mStage">starting…</div>
      <div class="title" id="mTitle"></div>
      <div class="meter" role="progressbar" aria-valuemin="0" aria-valuemax="100" aria-valuenow="0" id="mMeter"><div class="fill" id="mFill"></div></div>
      <div class="stat-grid">
        <div class="cell"><div class="k">Speed</div><div class="v" id="mSpeed">—</div></div>
        <div class="cell"><div class="k">ETA</div><div class="v" id="mEta">—</div></div>
        <div class="cell"><div class="k">Done</div><div class="v" id="mDone">—</div></div>
        <div class="cell"><div class="k">Frags</div><div class="v" id="mFrag">—</div></div>
      </div>
      <svg class="spark" id="spark" preserveAspectRatio="none" viewBox="0 0 400 56" aria-hidden="true">
        <path class="sp-area" id="spArea"></path>
        <path class="sp-line" id="spLine"></path>
      </svg>
      <div class="mlog" id="mLog" aria-live="polite"></div>
      <div class="mfoot">
        <button class="btn" onclick="cancelCurrent()" id="mCancel">Cancel</button>
        <button class="btn ghost" onclick="streamCurrent()" id="mStream" style="display:none">Stream</button>
        <button class="btn primary" onclick="closeModal()">Hide</button>
      </div>
    </div>
  </div>
</div>

<div class="modal-wrap" id="detailWrap" role="dialog" aria-modal="true" aria-labelledby="detailTitle">
  <div class="modal wide">
    <div class="mh">
      LIBRARY <span id="detailId" style="color:var(--ink-2)"></span>
      <button class="x" onclick="closeDetail()" aria-label="Close">✕</button>
    </div>
    <div class="mb" id="detailBody"></div>
  </div>
</div>

<div class="modal-wrap" id="previewWrap" role="dialog" aria-modal="true" aria-labelledby="previewTitle">
  <div class="modal wide">
    <div class="mh">PREVIEW <span id="previewTitle" style="color:var(--ink-2)"></span>
      <button class="x" onclick="closePreview()" aria-label="Close">✕</button>
    </div>
    <div class="mb" id="previewBody"></div>
  </div>
</div>

<div class="modal-wrap" id="transcriptWrap" role="dialog" aria-modal="true" aria-labelledby="transcriptTitle">
  <div class="modal wide">
    <div class="mh">TRANSCRIPT <span id="transcriptTitle" style="color:var(--ink-2)"></span>
      <button class="x" onclick="closeTranscript()" aria-label="Close">✕</button>
    </div>
    <div class="mb">
      <div id="transcriptBody" style="font-family:var(--mono);font-size:13px;line-height:1.7;white-space:pre-wrap;color:var(--ink-2)">loading…</div>
      <div class="mfoot">
        <button class="btn ghost" onclick="copyTranscript()">Copy</button>
        <button class="btn primary" onclick="downloadTranscript()">Download .txt</button>
      </div>
    </div>
  </div>
</div>

<div class="dialog-wrap" id="dialogWrap" role="dialog" aria-modal="true">
  <div class="dialog">
    <h3 id="dlgTitle">Confirm</h3>
    <div id="dlgMsg"></div>
    <input id="dlgInput" style="display:none">
    <div class="row">
      <button class="btn ghost" onclick="closeDialog(false)">Cancel</button>
      <button class="btn primary" id="dlgOk" onclick="closeDialog(true)">OK</button>
    </div>
  </div>
</div>

<div class="palette-wrap" id="paletteWrap">
  <div class="palette">
    <div class="input"><span class="sym" aria-hidden="true">›</span>
      <input id="paletteInput" placeholder="type a command…" autocomplete="off" aria-label="Command palette"></div>
    <div class="results" id="paletteResults" role="listbox"></div>
  </div>
</div>

<div class="toasts" id="toasts" role="status" aria-live="polite"></div>

<script>
const $ = id => document.getElementById(id);
const $$ = s => document.querySelectorAll(s);
const BASE_PATH = (document.querySelector('link[rel="manifest"]')?.href || '').replace(/\/manifest\.webmanifest$/, '');

const S = {
  view:'download', url:null, data:null, filter:'all', selRow:-1, compare:-1,
  sortKey:null, sortAsc:true,
  jobs:new Map(), current:null, ws:null, health:null, polling:null,
  session:{bytes:0,count:0,uptime:0},
  searchItems:[], searchSel:new Set(),
  channelItems:[], channelFiltered:[], channelUrl:null,
  speeds:[], favorites:new Set(), presets:[], profiles:[], settings:{},
  theme:'paper', density:'cozy', scale:1, focusMode:false,
  histBatch:false, histSelected:new Set(),
  undoMap:new Map(),
  heatmap:[],
};

// error boundary
window.addEventListener('error', e => {
  console.error('[grabtube]', e.error || e.message);
  try { toast('error: ' + (e.message || 'unknown'), 'err'); } catch(_){}
});
window.addEventListener('unhandledrejection', e => {
  console.error('[grabtube] unhandled', e.reason);
});

const esc = s => String(s ?? '').replace(/[&<>"']/g, c =>
  ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const apiPath = p => (BASE_PATH && !p.startsWith('http') ? BASE_PATH + p : p);

function fmtDur(s){
  if(!s) return '';
  s = Math.floor(s);
  const h = Math.floor(s/3600), m = Math.floor(s%3600/60), x = s%60;
  return (h?h+':':'') + String(m).padStart(h?2:1,'0') + ':' + String(x).padStart(2,'0');
}
function fmtSpeed(bps){
  if(!bps) return '—';
  const u = ['B/s','KB/s','MB/s','GB/s']; let i = 0;
  while (bps >= 1024 && i < u.length-1){ bps/=1024; i++; }
  return bps.toFixed(1) + ' ' + u[i];
}
function fmtSize(n){
  if(!n && n !== 0) return '0 B';
  const u = ['B','KB','MB','GB','TB']; let i = 0; let f = n;
  while (f >= 1024 && i < u.length-1){ f/=1024; i++; }
  return f.toFixed(f < 10 ? 1 : 0) + ' ' + u[i];
}
function fmtTime(){
  const d = new Date();
  return String(d.getHours()).padStart(2,'0')+':'+
         String(d.getMinutes()).padStart(2,'0');
}
function fmtRel(ts){
  if(!ts) return '';
  const d = (Date.now()/1000) - ts;
  if (d < 60) return Math.floor(d)+'s';
  if (d < 3600) return Math.floor(d/60)+'m';
  if (d < 86400) return Math.floor(d/3600)+'h';
  return Math.floor(d/86400)+'d';
}
function fmtDay(ts){
  const d = new Date(ts*1000), now = new Date();
  if (d.toDateString() === now.toDateString()) return 'Today';
  const y = new Date(now.getTime() - 86400000);
  if (d.toDateString() === y.toDateString()) return 'Yesterday';
  return d.toISOString().slice(0,10);
}
function codecClass(c){
  const x = (c||'').toLowerCase();
  if (x.includes('264')) return 'codec-h264';
  if (x.includes('265') || x.includes('hevc')) return 'codec-h265';
  if (x.includes('vp9') || x.includes('vp09')) return 'codec-vp9';
  if (x.includes('av1') || x.includes('av01')) return 'codec-av1';
  return '';
}
async function api(url, opts){
  const res = await fetch(apiPath(url), opts);
  if(!res.ok){
    let msg = 'HTTP ' + res.status;
    try { const j = await res.json(); msg = j.detail || msg; } catch(_){}
    throw new Error(msg);
  }
  return res.json();
}
function announce(msg){
  const el = $('srStatus'); if (el) el.textContent = msg;
}
function toast(msg, kind, undoId){
  const t = document.createElement('div');
  t.className = 'toast' + (kind ? ' ' + kind : '');
  t.innerHTML = `<span>${esc(msg)}</span>`;
  if (undoId){
    const u = document.createElement('button');
    u.className = 'undo';
    u.textContent = 'undo';
    u.onclick = async () => {
      try { await api('/api/undo', {method:'POST', headers:{'Content-Type':'application/json'},
        body: JSON.stringify({id: undoId})}); t.remove();
        toast('undone', 'ok'); loadHistory(); loadLibrary();
      } catch(e){ toast('undo failed', 'err'); }
    };
    t.appendChild(u);
  }
  $('toasts').appendChild(t);
  setTimeout(() => {
    t.style.transition = 'opacity .2s,transform .2s';
    t.style.opacity = '0'; t.style.transform = 'translateY(4px)';
    setTimeout(() => t.remove(), 240);
  }, undoId ? 8000 : 4200);
  while ($('toasts').children.length > 5) $('toasts').firstChild.remove();
}
function beep(freq, dur, vol){
  if (!S.settings.notify_sound || S.settings.notify_sound === '0') return;
  try {
    const ctx = new (window.AudioContext || window.webkitAudioContext)();
    const o = ctx.createOscillator(), g = ctx.createGain();
    o.frequency.value = freq || 800;
    g.gain.value = (vol || 0.05) * 0.4;
    o.connect(g); g.connect(ctx.destination);
    o.start(); o.stop(ctx.currentTime + (dur || 0.1));
  } catch(_){}
}

/* confirm dialog */
let _dlgResolve = null;
function ask(title, msgHtml, defVal){
  return new Promise(resolve => {
    _dlgResolve = resolve;
    $('dlgTitle').textContent = title;
    $('dlgMsg').innerHTML = msgHtml || '';
    const inp = $('dlgInput');
    if (defVal !== undefined){
      inp.style.display = ''; inp.value = defVal; setTimeout(()=>inp.focus(),50);
    } else inp.style.display = 'none';
    $('dialogWrap').classList.add('show');
    setTimeout(() => $('dlgOk').focus(), 60);
  });
}
function closeDialog(ok){
  $('dialogWrap').classList.remove('show');
  const inp = $('dlgInput');
  const val = ok ? (inp.style.display !== 'none' ? inp.value : true) : false;
  if (_dlgResolve){ _dlgResolve(val); _dlgResolve = null; }
}

/* theme / density / scale / focus */
function applyTheme(){
  document.documentElement.dataset.theme = S.theme === 'paper' ? '' : S.theme;
  $('themeBtn').textContent = {paper:'◐', dark:'☀', sepia:'◑', hc:'◈'}[S.theme] || '◐';
  localStorage.setItem('gt_theme', S.theme);
}
function cycleTheme(){
  const order = ['paper','dark','sepia','hc'];
  S.theme = order[(order.indexOf(S.theme)+1) % order.length];
  applyTheme();
  api('/api/settings', {method:'POST', headers:{'Content-Type':'application/json'},
    body: JSON.stringify({theme: S.theme})}).catch(()=>{});
}
function applyDensity(){
  document.documentElement.dataset.density = S.density;
  $('densityBtn').textContent = {compact:'⋮', cozy:'≡', roomy:'≡≡'}[S.density];
  localStorage.setItem('gt_density', S.density);
}
function cycleDensity(){
  const order = ['compact','cozy','roomy'];
  S.density = order[(order.indexOf(S.density)+1) % order.length];
  applyDensity();
}
function applyScale(){
  document.documentElement.style.setProperty('--scale', S.scale);
  $('scaleBtn').textContent = S.scale < 1 ? 'A' : (S.scale > 1.1 ? 'AA' : 'Aa');
  localStorage.setItem('gt_scale', S.scale);
}
function cycleScale(){
  const opts = [0.9, 1, 1.1, 1.25];
  const i = opts.indexOf(S.scale);
  S.scale = opts[(i+1) % opts.length];
  applyScale();
}
function toggleFocus(){
  S.focusMode = !S.focusMode;
  document.body.classList.toggle('focus-mode', S.focusMode);
  $('focusBtn').classList.toggle('on', S.focusMode);
  localStorage.setItem('gt_focus', S.focusMode ? '1' : '0');
}

/* console */
function toggleConsole(){ $('console').classList.toggle('open'); }
function logLine(line, level){
  const body = $('consoleBody');
  const d = document.createElement('div');
  d.className = 'ln ' + (level || 'info');
  d.innerHTML = `<span class="ts">${fmtTime()}</span><span class="tx">${esc(line)}</span>`;
  body.appendChild(d);
  while (body.children.length > 500) body.firstChild.remove();
  body.scrollTop = body.scrollHeight;
}

/* health + ws */
async function pollHealth(){
  try {
    const h = await api('/api/health');
    S.health = h; S.session = h.session || S.session;
    setDot('stFFdot', h.ffmpeg ? 'ok' : 'err');
    $('kFF').textContent = h.ffmpeg ? 'ready' : 'missing';
    $('kFF').className = h.ffmpeg ? 'ok' : 'err';
    $('kQ').textContent = h.active;
    $('kSlots').textContent = h.concurrency;
    $('kSlots').className = 'dim';
    $('kDisk').textContent = fmtSize(h.disk_free);
    $('kDisk').className = 'dim';
    $('kWs').textContent = h.websockets ? 'on' : 'fallback';
    $('kWs').className = h.websockets ? 'ok' : 'warn';
    $('kBytes').textContent = fmtSize(h.session.bytes);
    $('kCount').textContent = h.session.count;
    const up = Math.floor(h.session.uptime);
    $('kUptime').textContent = up<60?up+'s':(up<3600?Math.floor(up/60)+'m':Math.floor(up/3600)+'h');
    $('sbSes').textContent = fmtSize(h.session.bytes);
    $('sbQ').textContent = h.active;
    $('navQN').textContent = h.active > 0 ? '('+h.active+')' : '';
    $('pauseBtn').textContent = h.paused ? 'resume' : 'pause';
    $('pauseBtn').style.background = h.paused ? 'var(--red)' : 'transparent';
  } catch(_){ setDot('stFFdot','err'); }
  setTimeout(pollHealth, 5000);
}
function setDot(id, cls){ const d = $(id); if (d) d.className = 'dot ' + cls; }

function connectWS(){
  if (S.health && !S.health.websockets){
    startPolling(); return;
  }
  const proto = location.protocol === 'https:' ? 'wss:' : 'ws:';
  let sock;
  try { sock = new WebSocket(`${proto}//${location.host}${BASE_PATH}/ws`); }
  catch(_){ startPolling(); return; }
  S.ws = sock;
  let opened = false;
  sock.onopen = () => { opened = true; setDot('stWSdot','ok'); $('sbWS').textContent = 'linked'; };
  sock.onerror = () => { setDot('stWSdot','warn'); if (!opened) startPolling(); };
  sock.onclose = () => {
    setDot('stWSdot','cold'); $('sbWS').textContent = 'poll';
    if (!S.polling) startPolling();
    else setTimeout(connectWS, 8000);
  };
  sock.onmessage = ev => {
    let m; try { m = JSON.parse(ev.data); } catch(_){ return; }
    handleMsg(m);
  };
}
function startPolling(){
  if (S.polling) return;
  logLine('websocket unavailable — polling', 'warn');
  S.polling = setInterval(async () => {
    try {
      const jr = await api('/api/jobs');
      const items = jr.items;
      $('pauseBtn').textContent = jr.paused ? 'resume' : 'pause';
      const seen = new Set();
      items.forEach(j => {
        seen.add(j.job_id);
        const prev = S.jobs.get(j.job_id);
        S.jobs.set(j.job_id, j);
        if (!prev || prev.status !== j.status){
          if (j.status === 'done') handleMsg({type:'job_done', job:j});
          if (j.status === 'error') handleMsg({type:'job_error', job_id:j.job_id, error:j.error});
          if (j.status === 'cancelled') handleMsg({type:'job_cancelled', job_id:j.job_id});
        }
        if (S.current === j.job_id) updateModal(j);
      });
      [...S.jobs.keys()].forEach(k => { if (!seen.has(k) && S.current !== k) S.jobs.delete(k); });
      renderJobs(); updateTicker();
      const h = await api('/api/health');
      $('sbQ').textContent = h.active;
    } catch(_){}
  }, 1500);
}
function handleMsg(m){
  switch (m.type){
    case 'snapshot':
      (m.jobs || []).forEach(j => S.jobs.set(j.job_id, j));
      if (typeof m.paused === 'boolean'){
        $('pauseBtn').textContent = m.paused ? 'resume' : 'pause';
      }
      renderJobs(); updateTicker(); break;
    case 'job_queued':
      S.jobs.set(m.job_id, {job_id:m.job_id, status:'queued', progress:0,
        url:m.url, label:m.label, title:null, created_at:Date.now()/1000});
      renderJobs(); updateTicker(); break;
    case 'job_started':
      if (S.jobs.has(m.job_id)) S.jobs.get(m.job_id).status = 'running';
      renderJobs(); updateTicker(); break;
    case 'job_progress':
      if (m.job){ S.jobs.set(m.job.job_id, m.job); renderJobs(); updateTicker();
        if (S.current === m.job.job_id) updateModal(m.job); }
      break;
    case 'job_done':
      if (m.job) S.jobs.set(m.job.job_id, m.job);
      if (m.session) S.session = m.session;
      toast('✓ ' + (m.job?.title || '').slice(0,40), 'ok');
      beep(880, 0.15);
      if (S.settings.notify_desktop === '1' && window.Notification && Notification.permission === 'granted'){
        try { new Notification('GrabTube', {body: (m.job?.title || 'download complete').slice(0,80)}); } catch(_){}
      }
      if (S.current === m.job?.job_id){
        setTimeout(() => window.location.href = apiPath(`/api/file/${m.job.job_id}`), 250);
        setTimeout(closeModal, 1200);
      }
      renderJobs(); updateTicker();
      if (S.view === 'history') loadHistory();
      if (S.view === 'stats') loadStats();
      if (S.view === 'library') loadLibrary();
      refreshRecent();
      break;
    case 'job_error':
      toast('✗ ' + (m.error || 'unknown'), 'err');
      beep(220, 0.2);
      if (S.jobs.has(m.job_id)) S.jobs.get(m.job_id).status = 'error';
      if (S.current === m.job_id) closeModal();
      renderJobs(); break;
    case 'job_cancelled':
      toast('cancelled');
      if (S.jobs.has(m.job_id)) S.jobs.get(m.job_id).status = 'cancelled';
      if (S.current === m.job_id) closeModal();
      renderJobs(); updateTicker(); break;
    case 'log':
      logLine(m.line, m.level);
      if (m.job_id === S.current) appendModalLog(m.line, m.level);
      break;
    case 'favorite_toggled':
      if (m.added) S.favorites.add(m.video_id); else S.favorites.delete(m.video_id);
      if (S.view === 'favorites') loadFavorites();
      break;
    case 'favorite_removed':
      S.favorites.delete(m.video_id);
      if (S.view === 'favorites') loadFavorites();
      break;
    case 'preset_saved':
    case 'preset_deleted': loadPresets(); break;
    case 'profile_saved':
    case 'profile_deleted': loadProfiles(); break;
    case 'sub_added':
    case 'sub_deleted':
    case 'sub_toggled':
      if (S.view === 'subscriptions') loadSubscriptions();
      break;
    case 'sub_new':
      toast(`new from ${m.title}: ${m.count} item(s)`, 'ok');
      break;
    case 'queue_paused':
      $('pauseBtn').textContent = m.paused ? 'resume' : 'pause';
      break;
    case 'undo_available':
      if (m.undo) S.undoMap.set(m.undo.id, m.undo);
      break;
    case 'history_deleted':
      if (S.view === 'history') loadHistory();
      if (S.view === 'library') loadLibrary();
      break;
  }
}
function updateTicker(){
  let active = null;
  S.jobs.forEach(j => {
    if (j.status === 'running'){ active = j; return; }
    if (!active && j.status === 'queued') active = j;
  });
  const dot = $('sbDot'), live = $('sbLive'), tick = $('sbTick');
  if (active){
    dot.className = 'dot live';
    live.textContent = active.status;
    if (active.status === 'running'){
      const pct = (active.progress || 0).toFixed(1);
      const spd = active.speed ? fmtSpeed(active.speed) : '';
      tick.innerHTML = `<b>${esc((active.title || active.url || '').slice(0,50))}</b> · ${pct}% ${spd ? '· ' + spd : ''}`;
    } else {
      tick.innerHTML = 'queued · ' + esc((active.url || '').slice(0,60));
    }
  } else {
    dot.className = 'dot idle';
    live.textContent = 'idle';
    tick.innerHTML = 'press <b>/</b> to focus · <b>⌘K</b> for commands · click bar for console';
  }
}
async function togglePause(){
  const paused = $('pauseBtn').textContent !== 'resume';
  try {
    await api('/api/queue/pause', {method:'POST', headers:{'Content-Type':'application/json'},
      body: JSON.stringify({paused})});
    $('pauseBtn').textContent = paused ? 'resume' : 'pause';
  } catch(e){ toast(e.message, 'err'); }
}

/* views */
function setView(v){
  S.view = v;
  $$('.view').forEach(e => e.style.display = e.dataset.view === v ? '' : 'none');
  $$('#nav a').forEach(a => a.classList.toggle('active', a.dataset.v === v));
  const showHero = v === 'download';
  $('hero').style.display = showHero ? '' : 'none';
  $('commandShell').style.display = showHero ? '' : 'none';
  $('presetShell').style.display = showHero ? '' : 'none';
  if (v === 'history') loadHistory();
  if (v === 'stats') loadStats();
  if (v === 'favorites') loadFavorites();
  if (v === 'library') loadLibrary();
  if (v === 'subscriptions') loadSubscriptions();
  if (v === 'queue') renderQueueView();
  announce(v + ' view');
  location.hash = '#/' + v;
}
$('nav').addEventListener('click', e => {
  const a = e.target.closest('a'); if (!a) return;
  e.preventDefault();
  setView(a.dataset.v);
});

/* command bar */
async function cmdGo(){
  const raw = $('urlInput').value.trim();
  if (!raw) return toast('paste a URL', 'err');
  const urls = raw.match(/https?:\/\/[^\s<>"']+/g) || [];
  if (urls.length > 1){ await batchQueue(urls); return; }
  const single = urls[0] || raw;
  if (single.startsWith('?') || (!/^https?:/i.test(single) && single.length < 120)){
    $('searchInput').value = raw.replace(/^\?/, '');
    setView('search');
    return doSearch();
  }
  $('fetchBtn').disabled = true;
  $('fetchBtn').textContent = '…';
  $('results').innerHTML = `<div class="skel">
    <div class="bar-line"><div class="shim"></div></div>
    <div class="bar-line"><div class="shim"></div></div>
    <div class="bar-line"><div class="shim"></div></div></div>`;
  try {
    const data = await api('/api/info', {
      method:'POST', headers:{'Content-Type':'application/json'},
      body: JSON.stringify({url: single}),
    });
    S.url = single; S.data = data; S.filter = 'all'; S.selRow = -1; S.compare = -1;
    if (data.type === 'playlist') renderPlaylist(data);
    else renderVideo(data);
    announce('video loaded: ' + (data.title || ''));
  } catch(e){ $('results').innerHTML = ''; toast(e.message, 'err'); }
  finally { $('fetchBtn').disabled = false; $('fetchBtn').textContent = 'Fetch'; }
}
async function batchQueue(urls){
  try {
    const { job_id, count } = await api('/api/paste', {
      method:'POST', headers:{'Content-Type':'application/json'},
      body: JSON.stringify({urls, preset:'720',
        organize_by: S.settings.organize_by,
        skip_dupes: S.settings.skip_dupes !== '0'}),
    });
    toast(`queued ${count} urls`, 'ok');
    $('urlInput').value = '';
    openModal(job_id);
  } catch(e){ toast(e.message, 'err'); }
}

function renderVideo(d){
  const r = $('results');
  const hasChapters = d.chapters && d.chapters.length > 0;
  const hasSubs = d.subtitles && (
    Object.keys(d.subtitles.manual || {}).length > 0 ||
    Object.keys(d.subtitles.auto || {}).length > 0);
  const dupe = d.already_have ? '<span class="tag ok">already downloaded</span>' : '';

  const chips = [];
  if (d.upload_date) chips.push(`<span class="tag">${esc(d.upload_date)}</span>`);
  if (d.view_count) chips.push(`<span class="tag">${d.view_count.toLocaleString()} views</span>`);
  if (d.like_count) chips.push(`<span class="tag">${d.like_count.toLocaleString()} likes</span>`);
  if (dupe) chips.push(dupe);

  r.innerHTML = `
    <div class="sect">
      <div class="sect-head"><span class="num">01</span><h2>Video</h2>
        <span class="right">${d.formats.length} formats</span></div>
      <div class="vid">
        <div class="thumb" id="thumb" style="background-image:url('${esc(d.thumbnail||'')}')" role="button" tabindex="0" aria-label="Play preview">
          <div class="play-btn"></div>
          ${d.duration ? `<span class="badge-dur">${fmtDur(d.duration)}</span>` : ''}
        </div>
        <div class="info">
          <h3>${esc(d.title)}</h3>
          <div class="meta">
            ${d.uploader ? `<span>by <b>${esc(d.uploader)}</b></span>` : ''}
            ${d.duration ? `<span><b>${fmtDur(d.duration)}</b></span>` : ''}
          </div>
          <div class="tag-row">${chips.join('')}</div>
          <div class="btns">
            <button class="btn primary" data-preset="best">Best</button>
            <button class="btn" data-preset="1080">1080p</button>
            <button class="btn" data-preset="720">720p</button>
            <button class="btn" data-preset="480">480p</button>
            <button class="btn" data-preset="mp3">MP3</button>
            <button class="btn" data-preset="flac">FLAC</button>
            <button class="btn" data-preset="opus">OPUS</button>
            <button class="btn tooltip icon" id="favBtn" data-tip="Favorite">${d.is_favorite ? '★' : '☆'}</button>
            <button class="btn tooltip ghost icon" id="transcriptBtn" data-tip="Transcript">T</button>
            <button class="btn tooltip ghost icon" id="similarBtn" data-tip="Similar">≈</button>
            <label class="switch tooltip" data-tip="Download subtitles"><input type="checkbox" id="subToggle"><span class="box"></span><span class="txt">Subs</span></label>
            <label class="switch tooltip" data-tip="Save .srt/.vtt next to file"><input type="checkbox" id="subFiles"><span class="box"></span><span class="txt">Sidecar</span></label>
            <label class="switch tooltip" data-tip="Normalize loudness"><input type="checkbox" id="loudnormToggle"><span class="box"></span><span class="txt">Loud</span></label>
            <label class="switch tooltip" data-tip="Skip sponsor segments"><input type="checkbox" id="sponsorToggle"><span class="box"></span><span class="txt">SB</span></label>
            <label class="switch tooltip" data-tip="Save yt-dlp info.json"><input type="checkbox" id="infoJsonToggle"><span class="box"></span><span class="txt">JSON</span></label>
          </div>
        </div>
      </div>
    </div>

    ${(hasChapters || hasSubs) ? `
      <div class="sect">
        <div class="sect-head"><span class="num">02</span><h2>Trim & subtitle</h2></div>
        <div class="preview-grid">
          ${hasChapters ? `
            <div class="prev-card">
              <h4>Chapters</h4>
              <div class="sub">uncheck to skip · or use range below</div>
              <div class="ch-list" id="chList">
                ${d.chapters.map((c,i)=>`
                  <label class="ch-row">
                    <input type="checkbox" data-i="${i}" checked>
                    <span class="idx">${String(i+1).padStart(2,'0')}</span>
                    <span class="nm">${esc(c.title)}</span>
                    <span class="tm">${fmtDur(c.start_time)}</span>
                  </label>
                `).join('')}
              </div>
            </div>` : ''}
          ${hasSubs ? `
            <div class="prev-card">
              <h4>Subtitles</h4>
              <div class="sub">pick languages · format shown</div>
              <div class="chips" id="subChips">
                ${Object.keys(d.subtitles.manual||{}).sort().map(l => {
                  const exts = d.subtitles.manual[l].map(t => t.ext).filter(Boolean);
                  const ext = exts[0] || '';
                  return `<span class="lang-chip ${l.startsWith('en')?'on':''}" data-code="${esc(l)}" role="checkbox" tabindex="0">${esc(l)}${ext?`<span class="ext">${esc(ext)}</span>`:''}</span>`;
                }).join('')}
                ${Object.keys(d.subtitles.auto||{}).sort().slice(0,24).map(l => {
                  const exts = d.subtitles.auto[l].map(t => t.ext).filter(Boolean);
                  const ext = exts[0] || '';
                  return `<span class="lang-chip auto ${l.startsWith('en')?'on':''}" data-code="${esc(l)}" role="checkbox" tabindex="0">${esc(l)}${ext?`<span class="ext">${esc(ext)}</span>`:''}</span>`;
                }).join('')}
              </div>
            </div>` : ''}
        </div>
        <div class="trim-wrap">
          <span>Trim range</span>
          <input id="trimStart" placeholder="start s" type="text" aria-label="Trim start seconds">
          <span>→</span>
          <input id="trimEnd" placeholder="end s" type="text" aria-label="Trim end seconds">
          <span>Format</span>
          <select id="subFormat" aria-label="Subtitle format">
            <option value="best">best</option>
            <option value="srt">srt</option>
            <option value="vtt">vtt</option>
            <option value="ass">ass</option>
          </select>
          <button class="btn ghost" onclick="clearTrim()">Clear</button>
          <span style="color:var(--ink-3);font-size:10.5px">blank = full video</span>
        </div>
      </div>
    ` : ''}

    <div class="sect">
      <div class="sect-head"><span class="num">${(hasChapters||hasSubs)?'03':'02'}</span><h2>Formats</h2>
        <span class="right">↑↓ move · ⏎ download · c compare · click header to sort</span></div>
      <div style="display:flex;gap:8px;margin-bottom:16px;font-family:var(--mono);font-size:11px;flex-wrap:wrap">
        <span class="pill on" data-f="all">all</span>
        <span class="pill" data-f="combined">combined</span>
        <span class="pill" data-f="video">video only</span>
        <span class="pill" data-f="audio">audio only</span>
      </div>
      <div id="cmpPanel"></div>
      <table class="formats">
        <thead><tr>
          <th data-sort="format_id">id</th><th data-sort="kind">kind</th>
          <th data-sort="resolution">quality</th>
          <th class="col-fps" data-sort="fps">fps</th>
          <th class="col-vc" data-sort="vcodec">video</th>
          <th class="col-ac" data-sort="acodec">audio</th>
          <th class="col-br" data-sort="tbr">bitrate</th>
          <th class="num" data-sort="filesize">size</th><th></th>
        </tr></thead>
        <tbody id="fmtBody"></tbody>
      </table>
    </div>
  `;
  const thumb = $('thumb');
  const openPreview = () => {
    if (!d.id) return;
    thumb.innerHTML = `<iframe src="https://www.youtube.com/embed/${esc(d.id)}?autoplay=1" allow="autoplay; encrypted-media" allowfullscreen title="preview"></iframe>`;
  };
  thumb.onclick = openPreview;
  thumb.onkeydown = e => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); openPreview(); } };

  r.querySelectorAll('[data-preset]').forEach(b => b.onclick = () => downloadPreset(b.dataset.preset));
  $('favBtn').onclick = async e => {
    try {
      const rr = await api('/api/favorites/toggle', {
        method:'POST', headers:{'Content-Type':'application/json'},
        body: JSON.stringify({id:d.id, url:d.url, title:d.title,
          uploader:d.uploader, duration:d.duration, thumbnail:d.thumbnail}),
      });
      e.target.textContent = rr.favorite ? '★' : '☆';
      d.is_favorite = rr.favorite;
    } catch(err){ toast(err.message, 'err'); }
  };
  $('transcriptBtn').onclick = () => openTranscript(d.url);
  $('similarBtn').onclick = () => {
    $('searchInput').value = (d.title || '').slice(0, 80);
    setView('search');
    doSearch();
  };
  const subChips = $('subChips');
  if (subChips){
    const toggle = c => {
      c.classList.toggle('on');
      const any = document.querySelectorAll('#subChips .lang-chip.on').length > 0;
      if ($('subToggle')) $('subToggle').checked = any;
    };
    subChips.addEventListener('click', e => {
      const c = e.target.closest('.lang-chip'); if (!c) return; toggle(c);
    });
    subChips.addEventListener('keydown', e => {
      if (e.key === ' ' || e.key === 'Enter'){
        const c = e.target.closest('.lang-chip'); if (!c) return;
        e.preventDefault(); toggle(c);
      }
    });
  }
  r.querySelectorAll('.pill[data-f]').forEach(p => {
    p.onclick = () => {
      r.querySelectorAll('.pill[data-f]').forEach(x => x.classList.remove('on'));
      p.classList.add('on');
      S.filter = p.dataset.f; S.selRow = -1; S.compare = -1; paintFormats();
    };
  });
  r.querySelectorAll('th[data-sort]').forEach(th => {
    th.onclick = () => {
      if (S.sortKey === th.dataset.sort) S.sortAsc = !S.sortAsc;
      else { S.sortKey = th.dataset.sort; S.sortAsc = true; }
      paintFormats();
    };
  });
  paintFormats();
}

function paintFormats(){
  let rows = (S.data.formats || []).filter(f => S.filter === 'all' ? true : f.kind === S.filter);
  if (S.sortKey){
    const k = S.sortKey, asc = S.sortAsc ? 1 : -1;
    rows = [...rows].sort((a,b) => {
      const av = a[k] ?? 0, bv = b[k] ?? 0;
      if (typeof av === 'string' && typeof bv === 'string') return av.localeCompare(bv) * asc;
      return (av - bv) * asc;
    });
  }
  const tb = $('fmtBody'); if (!tb) return;
  if (!rows.length){
    tb.innerHTML = '<tr><td colspan="9" class="empty">no formats</td></tr>';
    return;
  }
  const maxBr = Math.max(...rows.map(f => f.tbr || f.vbr || f.abr || 0), 1);
  tb.innerHTML = rows.map((f,i) => {
    const q = f.kind === 'audio' ? (f.abr ? Math.round(f.abr)+' kbps' : 'audio') : (f.resolution || '—');
    const br = f.tbr || f.vbr || f.abr || 0;
    const brPct = Math.min(100, br / maxBr * 100);
    const hdr = f.hdr ? '<span class="hdr-tag">HDR</span>' : '';
    const lang = f.language ? `<span class="lang-tag">${esc(f.language)}</span>` : '';
    const kl = f.kind === 'combined' ? 'V+A' : f.kind === 'video' ? 'VID' : 'AUD';
    const vc = f.vcodec ? `<span class="${codecClass(f.vcodec)}">${esc(f.vcodec)}</span>` : '—';
    const ac = f.acodec ? `<span class="${codecClass(f.acodec)}">${esc(f.acodec)}</span>` : '—';
    const cls = i === S.selRow ? 'pick' : (i === S.compare ? 'compare' : '');
    return `<tr data-i="${i}" class="${cls}">
      <td class="lead">${esc(f.format_id)}</td>
      <td><span class="kind ${f.kind}">${kl}</span></td>
      <td class="lead">${esc(q)}${hdr}${lang}</td>
      <td class="col-fps">${f.fps ? Math.round(f.fps) : '—'}</td>
      <td class="col-vc">${vc}</td>
      <td class="col-ac">${ac}</td>
      <td class="col-br"><span class="br-bar"><i style="width:${brPct}%"></i></span>${br?Math.round(br)+'k':'—'}</td>
      <td class="num">${esc(f.filesize_str || '—')}</td>
      <td style="text-align:right">
        <button class="btn primary" data-fid="${esc(f.format_id)}" data-kind="${f.kind}">Get</button>
      </td>
    </tr>`;
  }).join('');
  tb.querySelectorAll('[data-fid]').forEach(b => {
    b.onclick = ev => { ev.stopPropagation(); downloadFormat(b.dataset.fid, b.dataset.kind); };
  });
  tb.querySelectorAll('tr[data-i]').forEach(tr => {
    tr.onclick = () => { S.selRow = parseInt(tr.dataset.i, 10); paintFormats(); };
  });
  document.querySelectorAll('th[data-sort]').forEach(th => {
    th.classList.toggle('sorted', th.dataset.sort === S.sortKey);
    th.classList.toggle('asc', th.dataset.sort === S.sortKey && S.sortAsc);
  });
  paintCompare();
}
function paintCompare(){
  const p = $('cmpPanel'); if (!p) return;
  if (S.compare < 0 || S.selRow < 0 || S.compare === S.selRow){ p.innerHTML = ''; return; }
  const rows = (S.data.formats || []).filter(f => S.filter === 'all' ? true : f.kind === S.filter);
  const a = rows[S.compare], b = rows[S.selRow];
  if (!a || !b){ p.innerHTML = ''; return; }
  p.innerHTML = `
    <div style="border:1px solid var(--ink);padding:16px;margin-bottom:16px;font-family:var(--mono);font-size:11.5px">
      <div style="display:flex;gap:20px;color:var(--ink-3);margin-bottom:10px;letter-spacing:.14em;text-transform:uppercase;font-size:10px">
        <span>compare</span><span style="flex:1"></span>
        <span onclick="S.compare=-1;paintFormats()" style="cursor:pointer;color:var(--ink)">close ✕</span>
      </div>
      <table style="width:100%;border-collapse:collapse">
        <tr><td style="width:100px;color:var(--ink-3)">id</td>
          <td><b>${esc(a.format_id)}</b></td><td><b>${esc(b.format_id)}</b></td></tr>
        <tr><td style="color:var(--ink-3)">quality</td>
          <td>${esc(a.resolution||'—')}</td><td>${esc(b.resolution||'—')}</td></tr>
        <tr><td style="color:var(--ink-3)">codec</td>
          <td>${esc(a.vcodec||'—')}</td><td>${esc(b.vcodec||'—')}</td></tr>
        <tr><td style="color:var(--ink-3)">bitrate</td>
          <td>${Math.round(a.tbr||a.vbr||a.abr||0)}k</td>
          <td>${Math.round(b.tbr||b.vbr||b.abr||0)}k</td></tr>
        <tr><td style="color:var(--ink-3)">size</td>
          <td>${esc(a.filesize_str||'—')}</td><td>${esc(b.filesize_str||'—')}</td></tr>
      </table>
    </div>`;
}

function selectedChapterRanges(){
  const rows = $$('#chList .ch-row'); if (!rows.length) return null;
  const all = S.data.chapters || []; const ranges = []; let allChecked = true;
  rows.forEach(row => {
    const cb = row.querySelector('input');
    if (!cb.checked) allChecked = false;
    else { const c = all[parseInt(cb.dataset.i,10)]; if (c) ranges.push({start_time:c.start_time, end_time:c.end_time}); }
  });
  if (allChecked) return null;
  return ranges.length ? ranges : [{start_time:0, end_time:0.1}];
}
function selectedSubLangs(){ return [...$$('#subChips .lang-chip.on')].map(c => c.dataset.code); }
function clearTrim(){ $('trimStart').value=''; $('trimEnd').value=''; }

function readSettings(){
  return {
    subtitles: $('subToggle')?.checked || false,
    sub_files: $('subFiles')?.checked || false,
    loudnorm: $('loudnormToggle')?.checked || false,
    sponsorblock: $('sponsorToggle')?.checked || false,
    write_info_json: $('infoJsonToggle')?.checked || false,
    subtitle_format: $('subFormat')?.value || 'best',
    organize_by: S.settings.organize_by || null,
    skip_dupes: S.settings.skip_dupes !== '0',
    cookies_from_browser: S.settings.cookies_from_browser || null,
    cookies_file: S.settings.cookies_file || null,
    rate_limit: S.settings.rate_limit || null,
    proxy: S.settings.proxy || null,
    filename_template: S.settings.filename_template || null,
    sponsorblock_categories: S.settings.sponsorblock_categories
      ? S.settings.sponsorblock_categories.split(",").filter(Boolean)
      : ["sponsor","selfpromo","interaction"],
  };
}
async function downloadPreset(preset){
  const langs = selectedSubLangs();
  const s = readSettings();
  const payload = { url: S.url, preset,
    subtitles: s.subtitles || langs.length > 0,
    subtitle_langs: langs.length ? langs : ['en.*'],
    sub_files: s.sub_files, subtitle_format: s.subtitle_format,
    loudnorm: s.loudnorm, sponsorblock: s.sponsorblock,
    write_info_json: s.write_info_json,
    sponsorblock_categories: s.sponsorblock_categories,
    organize_by: s.organize_by, filename_template: s.filename_template,
    cookies_from_browser: s.cookies_from_browser, cookies_file: s.cookies_file,
    rate_limit: s.rate_limit, proxy: s.proxy, keep: true,
  };
  if (['mp3','flac','opus'].includes(preset)){ payload.audio_format = preset; payload.preset = null; }
  const r = selectedChapterRanges(); if (r) payload.chapter_ranges = r;
  const ts = parseFloat($('trimStart')?.value), te = parseFloat($('trimEnd')?.value);
  if (!isNaN(ts) && !isNaN(te) && te > ts){ payload.trim_start = ts; payload.trim_end = te; }
  await startDownload(payload);
}
async function downloadFormat(fid, kind){
  const langs = selectedSubLangs();
  const s = readSettings();
  await startDownload({
    url: S.url, format_id: fid, kind,
    subtitles: s.subtitles || langs.length > 0,
    subtitle_langs: langs.length ? langs : ['en.*'],
    sub_files: s.sub_files, subtitle_format: s.subtitle_format,
    sponsorblock: s.sponsorblock, write_info_json: s.write_info_json,
    cookies_from_browser: s.cookies_from_browser, cookies_file: s.cookies_file,
    keep: true,
  });
}
async function startDownload(payload){
  try {
    const { job_id } = await api('/api/download', {
      method:'POST', headers:{'Content-Type':'application/json'},
      body: JSON.stringify(payload),
    });
    openModal(job_id);
  } catch(e){ toast(e.message, 'err'); }
}

/* presets */
async function loadPresets(){
  try {
    const { items } = await api('/api/presets');
    S.presets = items;
    const box = $('presetList');
    box.innerHTML = `
      <span class="pill" onclick="applyBuiltin('best')">best</span>
      <span class="pill" onclick="applyBuiltin('1080')">1080p</span>
      <span class="pill" onclick="applyBuiltin('720')">720p</span>
      <span class="pill" onclick="applyBuiltin('mp3')">mp3</span>
      <span class="pill" onclick="applyBuiltin('flac')">flac</span>
      ${items.map(p => `<span class="pill" onclick="applyPreset('${p.id}')" title="saved preset">
        ${esc(p.name)} <span class="x" onclick="event.stopPropagation();delPreset('${p.id}')">✕</span></span>`).join('')}`;
  } catch(_){}
}
function applyBuiltin(name){ if (!S.url) return toast('fetch a video first', 'err'); downloadPreset(name); }
async function applyPreset(pid){
  const p = S.presets.find(x => x.id === pid);
  if (!p || !S.url) return toast('fetch a video first', 'err');
  await startDownload(Object.assign({}, p.payload, {url: S.url}));
}
async function savePreset(){
  const name = await ask('Save preset', '<p>Name this preset so you can reapply it later.</p>', 'my preset');
  if (!name) return;
  const s = readSettings();
  const langs = selectedSubLangs();
  const payload = {
    preset: '720',
    subtitles: s.subtitles || langs.length > 0,
    subtitle_langs: langs.length ? langs : ['en.*'],
    sub_files: s.sub_files, subtitle_format: s.subtitle_format,
    loudnorm: s.loudnorm, sponsorblock: s.sponsorblock,
    write_info_json: s.write_info_json,
    sponsorblock_categories: s.sponsorblock_categories,
    organize_by: s.organize_by, filename_template: s.filename_template,
    cookies_from_browser: s.cookies_from_browser, cookies_file: s.cookies_file,
    rate_limit: s.rate_limit, proxy: s.proxy,
  };
  try { await api('/api/presets', {method:'POST', headers:{'Content-Type':'application/json'},
    body: JSON.stringify({name, payload})}); loadPresets(); }
  catch(e){ toast(e.message, 'err'); }
}
async function delPreset(pid){
  try { await api(`/api/presets/${pid}`, {method:'DELETE'}); loadPresets(); }
  catch(e){ toast(e.message, 'err'); }
}

/* profiles */
async function loadProfiles(){
  try {
    const { items } = await api('/api/profiles');
    S.profiles = items;
    const sel = $('profileSelect');
    const cur = sel.value;
    sel.innerHTML = `<option value="">— default —</option>` +
      items.map(p => `<option value="${p.id}">${esc(p.name)}</option>`).join('');
    sel.value = cur;
  } catch(_){}
}
async function applyProfile(pid){
  const p = S.profiles.find(x => x.id === pid);
  if (!p) return;
  try {
    await api('/api/settings', {method:'POST', headers:{'Content-Type':'application/json'},
      body: JSON.stringify(p.config)});
    S.settings = Object.assign(S.settings, p.config);
    toast('profile: ' + p.name, 'ok');
  } catch(e){ toast(e.message, 'err'); }
}

/* playlist */
function renderPlaylist(d){
  const r = $('results');
  r.innerHTML = `
    <div class="sect">
      <div class="sect-head"><span class="num">01</span><h2>Playlist</h2>
        <span class="right">${d.count} items</span></div>
      <div class="vid" style="grid-template-columns:1fr">
        <div class="info">
          <h3>${esc(d.title)}</h3>
          <div class="meta">
            ${d.uploader ? `<span>by <b>${esc(d.uploader)}</b></span>` : ''}
            <span><b>${d.count}</b> videos</span>
          </div>
          <div class="btns">
            <button class="btn" id="plAll">All</button>
            <button class="btn ghost" id="plNone">None</button>
            <button class="btn primary" onclick="downloadZip('720')">Zip · 720p</button>
            <button class="btn" onclick="downloadZip('best')">Zip · Best</button>
            <button class="btn" onclick="downloadZip('mp3')">Zip · MP3</button>
          </div>
        </div>
      </div>
    </div>
    <div class="sect">
      <div class="sect-head"><span class="num">02</span><h2>Items</h2>
        <span class="right"><input id="plRange" placeholder="range e.g. 1-10,15" style="font-family:var(--mono);font-size:11px;background:transparent;border:0;border-bottom:1px solid var(--rule);outline:0;color:var(--ink);width:160px;padding:2px 0" aria-label="Playlist range"></span></div>
      <div id="plList"></div>
    </div>`;
  const list = $('plList');
  list.innerHTML = d.entries.map((e,i) => `
    <div class="h-item" style="grid-template-columns:22px 60px 1fr auto">
      <input type="checkbox" data-url="${esc(e.url||'')}" checked style="accent-color:var(--red);width:15px;height:15px" aria-label="Select ${esc(e.title||'')}">
      <div style="width:60px;aspect-ratio:16/9;background:#1A1815 center/cover;border:1px solid var(--rule);background-image:url('${esc(e.thumbnail||'')}')"></div>
      <div>
        <div class="t">${String(i+1).padStart(2,'0')} · ${esc(e.title||'')}</div>
        <div class="m">${e.duration?fmtDur(e.duration):''}</div>
      </div>
      <button class="btn ghost" data-open="${esc(e.url||'')}">Inspect</button>
    </div>`).join('');
  $('plAll').onclick = () => list.querySelectorAll('input[type=checkbox]').forEach(c=>c.checked=true);
  $('plNone').onclick = () => list.querySelectorAll('input[type=checkbox]').forEach(c=>c.checked=false);
  list.querySelectorAll('[data-open]').forEach(b => {
    b.onclick = () => { $('urlInput').value = b.dataset.open; cmdGo(); };
  });
}
async function downloadZip(preset){
  const urls = [...document.querySelectorAll('input[data-url]:checked')]
    .map(c => c.dataset.url).filter(Boolean);
  if (!urls.length) return toast('select items', 'err');
  const s = readSettings();
  try {
    const { job_id } = await api('/api/playlist', {
      method:'POST', headers:{'Content-Type':'application/json'},
      body: JSON.stringify({urls, preset,
        audio_format: preset==='mp3'?'mp3':null,
        subtitles: s.subtitles, sponsorblock: s.sponsorblock,
        cookies_from_browser: s.cookies_from_browser, cookies_file: s.cookies_file,
        playlist_items: $('plRange')?.value.trim() || null}),
    });
    openModal(job_id);
  } catch(e){ toast(e.message, 'err'); }
}

/* search */
async function doSearch(){
  const q = $('searchInput').value.trim();
  if (!q) return;
  $('searchResults').innerHTML = '<div class="empty">searching…</div>';
  try {
    const data = await api('/api/search', {method:'POST', headers:{'Content-Type':'application/json'},
      body: JSON.stringify({q, limit: 30})});
    S.searchItems = data.items; S.searchSel = new Set();
    $('sResults').textContent = data.items.length + ' results';
    renderSearchResults();
  } catch(e){ $('searchResults').innerHTML = ''; toast(e.message, 'err'); }
}
function renderSearchResults(){
  const r = $('searchResults');
  if (!S.searchItems.length){ r.innerHTML = '<div class="empty">no results</div>'; return; }
  r.innerHTML = `<div class="search-grid">${S.searchItems.map((it,i)=>`
    <div class="card" data-i="${i}">
      <div class="thumb" style="background-image:url('${esc(it.thumbnail||'')}')">
        <span class="row-check ${S.searchSel.has(i)?'on':''}" data-check="${i}" role="checkbox" aria-checked="${S.searchSel.has(i)}" tabindex="0">✓</span>
        <span class="row-star ${S.favorites.has(it.id)?'on':''}" data-star="${i}" role="button" aria-label="Favorite" tabindex="0">${S.favorites.has(it.id)?'★':'☆'}</span>
        ${it.duration ? `<span class="badge-dur">${fmtDur(it.duration)}</span>` : ''}
      </div>
      <div class="title">${esc(it.title)}</div>
      <div class="byline">${esc(it.uploader||'')}${it.view_count ? ' · '+it.view_count.toLocaleString()+' views' : ''}${it.already_have ? ' · ✓' : ''}</div>
    </div>`).join('')}</div>`;
  r.querySelectorAll('.card').forEach(el => {
    el.onclick = ev => {
      if (ev.target.closest('.row-check') || ev.target.closest('.row-star')) return;
      setView('download');
      $('urlInput').value = S.searchItems[parseInt(el.dataset.i,10)].url;
      cmdGo();
    };
  });
  r.querySelectorAll('[data-check]').forEach(el => {
    el.onclick = ev => { ev.stopPropagation();
      const i = parseInt(el.dataset.check,10);
      if (S.searchSel.has(i)) S.searchSel.delete(i); else S.searchSel.add(i);
      updateBulk(); renderSearchResults(); };
  });
  r.querySelectorAll('[data-star]').forEach(el => {
    el.onclick = async ev => { ev.stopPropagation();
      const it = S.searchItems[parseInt(el.dataset.star,10)];
      try {
        const rr = await api('/api/favorites/toggle', {method:'POST', headers:{'Content-Type':'application/json'},
          body: JSON.stringify({id:it.id, url:it.url, title:it.title, uploader:it.uploader, duration:it.duration, thumbnail:it.thumbnail})});
        if (rr.favorite) S.favorites.add(it.id); else S.favorites.delete(it.id);
        renderSearchResults();
      } catch(err){ toast(err.message, 'err'); } };
  });
}
function updateBulk(){
  const n = S.searchSel.size;
  $('bulkN').textContent = n;
  $('bulkBar').classList.toggle('on', n > 0);
}
function bulkClear(){ S.searchSel = new Set(); updateBulk(); renderSearchResults(); }
async function bulkQueue(){
  const urls = [...S.searchSel].map(i => S.searchItems[i].url).filter(Boolean);
  if (!urls.length) return;
  try {
    const { job_id, count } = await api('/api/paste', {method:'POST', headers:{'Content-Type':'application/json'},
      body: JSON.stringify({urls, preset: '720'})});
    toast(`queued ${count}`, 'ok');
    S.searchSel = new Set(); updateBulk(); renderSearchResults();
    openModal(job_id);
  } catch(e){ toast(e.message, 'err'); }
}

/* channel */
async function loadChannel(){
  const url = $('channelInput').value.trim();
  if (!url) return toast('paste a channel URL', 'err');
  $('channelResults').innerHTML = '<div class="empty">loading…</div>';
  try {
    const data = await api('/api/channel', {method:'POST', headers:{'Content-Type':'application/json'},
      body: JSON.stringify({url, limit: 60})});
    S.channelItems = data.entries; S.channelFiltered = data.entries; S.channelUrl = data.channel_url;
    $('chRight').textContent = data.count + ' videos';
    renderChannel();
  } catch(e){ $('channelResults').innerHTML = ''; toast(e.message, 'err'); }
}
function filterChannel(){
  const rx = $('chFilter').value.trim();
  if (!rx){ S.channelFiltered = S.channelItems; renderChannel(); return; }
  let re;
  try { re = new RegExp(rx, 'i'); }
  catch(_){ toast('bad regex', 'err'); return; }
  S.channelFiltered = S.channelItems.filter(e => re.test(e.title || ''));
  renderChannel();
}
function renderChannel(){
  const r = $('channelResults');
  const items = S.channelFiltered;
  if (!items.length){ r.innerHTML = '<div class="empty">no videos</div>'; return; }
  r.innerHTML = items.map((e,i) => `
    <div class="h-item" style="grid-template-columns:60px 1fr auto">
      <div style="width:60px;aspect-ratio:16/9;background:#1A1815 center/cover;border:1px solid var(--rule);background-image:url('${esc(e.thumbnail||'')}')"></div>
      <div>
        <div class="t">${esc(e.title||'')}</div>
        <div class="m">${e.duration?fmtDur(e.duration):''}${e.view_count?' · '+e.view_count.toLocaleString()+' views':''}</div>
      </div>
      <button class="btn ghost" data-grab="${esc(e.url||'')}">Grab</button>
    </div>`).join('');
  r.querySelectorAll('[data-grab]').forEach(b => {
    b.onclick = () => { S.url = b.dataset.grab; setView('download');
      $('urlInput').value = b.dataset.grab; cmdGo(); };
  });
}
async function queueChannel(){ await channelQueue(S.channelFiltered.map(e => e.url).filter(Boolean)); }
async function queueChannelRegex(){
  const rx = $('chFilter').value.trim();
  if (!rx) return queueChannel();
  filterChannel();
  await channelQueue(S.channelFiltered.map(e => e.url).filter(Boolean));
}
async function channelQueue(urls){
  if (!urls.length) return toast('no urls', 'err');
  const s = readSettings();
  try {
    const { job_id, count } = await api('/api/paste', {method:'POST', headers:{'Content-Type':'application/json'},
      body: JSON.stringify({urls, preset: '720', organize_by: s.organize_by, skip_dupes: s.skip_dupes})});
    toast(`queued ${count}`, 'ok');
    openModal(job_id);
  } catch(e){ toast(e.message, 'err'); }
}
async function subscribeChannel(){
  if (!S.channelUrl) return toast('load a channel first', 'err');
  try {
    const r = await api('/api/subscriptions', {method:'POST', headers:{'Content-Type':'application/json'},
      body: JSON.stringify({url: S.channelUrl, kind: 'channel', preset: '720'})});
    toast('subscribed: ' + r.title, 'ok');
    setView('subscriptions');
  } catch(e){ toast(e.message, 'err'); }
}

/* library */
async function loadLibrary(){
  const q = $('libQ')?.value.trim(); const sort = $('libSort')?.value; const tag = $('libTag')?.value.trim();
  const params = new URLSearchParams();
  if (q) params.set('q', q); if (sort) params.set('sort', sort); if (tag) params.set('tag', tag);
  try {
    const { items } = await api('/api/library?' + params.toString());
    const r = $('libraryResults');
    $('libRight').textContent = items.length + ' files · ' + fmtSize(items.reduce((a,x)=>a+(x.file_size||0),0));
    if (!items.length){ r.innerHTML = `<div class="empty">
      <svg width="64" height="64" viewBox="0 0 64 64" fill="none" stroke="currentColor" stroke-width="2">
        <rect x="8" y="14" width="48" height="36"/><path d="M28 26l12 8-12 8z"/></svg>
      library is empty — download something with "keep" enabled</div>`; return; }
    r.innerHTML = `<div class="library-grid">${items.map(it => {
      let tags = []; try { tags = JSON.parse(it.tags || '[]'); } catch(_){}
      const rating = it.rating ? '★'.repeat(it.rating) : '';
      return `<div class="lib-card" data-jid="${it.job_id}">
        <div class="thumb" style="background-image:url('${esc(it.thumbnail||'')}')">
          ${it.watched ? '<span class="watched-badge">watched</span>' : ''}
          ${rating ? `<span class="rating-badge">${rating}</span>` : ''}
        </div>
        <div class="t">${esc(it.title||'')}</div>
        <div class="m">
          <span>${it.uploader?esc(it.uploader)+' · ':''}${it.file_size?fmtSize(it.file_size):''}</span>
          <span>${fmtRel(it.created_at)}</span>
        </div>
        ${tags.length ? `<div class="tags">${tags.map(t=>`<span class="tag-chip">${esc(t)}</span>`).join('')}</div>` : ''}
      </div>`;
    }).join('')}</div>`;
    r.querySelectorAll('.lib-card').forEach(el => {
      el.onclick = () => openDetail(el.dataset.jid);
    });
  } catch(e){ toast(e.message, 'err'); }
}
let libTimer = null;
function debouncedLibrary(){ clearTimeout(libTimer); libTimer = setTimeout(loadLibrary, 250); }

async function openDetail(jid){
  try {
    const it = await api(`/api/status/${jid}`);
    let bookmarks = []; try { bookmarks = JSON.parse(it.bookmarks || '[]'); } catch(_){}
    let tags = []; try { tags = JSON.parse(it.tags || '[]'); } catch(_){}
    const rating = it.rating || 0;
    $('detailId').textContent = jid.slice(0,8);
    $('detailBody').innerHTML = `
      <div style="display:grid;grid-template-columns:minmax(0,320px) 1fr;gap:24px">
        <div>
          <div class="thumb" style="background-image:url('${esc(it.thumbnail||'')}');cursor:pointer"
            onclick="window.open(apiPath('/api/stream/${jit(jid)}'),'_blank')">
            <div class="play-btn"></div>
          </div>
          <div class="btns" style="margin-top:14px">
            <button class="btn primary" onclick="window.location.href=apiPath('/api/file/${jit(jid)}')">Download</button>
            <button class="btn ghost" onclick="window.open(apiPath('/api/stream/${jit(jid)}'),'_blank')">Stream</button>
          </div>
        </div>
        <div>
          <h3 style="font-family:var(--serif);font-size:22px;margin:0 0 10px;letter-spacing:-.02em;line-height:1.2">${esc(it.title||'')}</h3>
          <div class="meta" style="font-family:var(--mono);font-size:11.5px;color:var(--ink-3);margin-bottom:18px;display:flex;gap:14px;flex-wrap:wrap">
            ${it.uploader?`<span>${esc(it.uploader)}</span>`:''}
            ${it.file_size?`<span>${fmtSize(it.file_size)}</span>`:''}
            ${it.format_label?`<span>${esc(it.format_label)}</span>`:''}
            <span>${new Date(it.created_at*1000).toLocaleString()}</span>
          </div>

          <label style="font-family:var(--mono);font-size:10px;letter-spacing:.14em;color:var(--ink-3);text-transform:uppercase">Rating</label>
          <div class="rating-row" id="dRating">
            ${[1,2,3,4,5].map(n=>`<span class="star ${n<=rating?'on':''}" data-n="${n}" role="button" tabindex="0">★</span>`).join('')}
            ${rating ? `<span style="font-family:var(--mono);font-size:11px;color:var(--ink-3);margin-left:10px;align-self:center;cursor:pointer" onclick="patchMeta('${jit(jid)}',{rating:0}).then(()=>openDetail('${jit(jid)}'))">clear</span>` : ''}
          </div>

          <label style="font-family:var(--mono);font-size:10px;letter-spacing:.14em;color:var(--ink-3);text-transform:uppercase">Tags</label>
          <div class="tag-input" id="dTags">
            ${tags.map(t=>`<span class="t">${esc(t)} <span class="rm" data-t="${esc(t)}">×</span></span>`).join('')}
            <input id="dTagNew" placeholder="add tag…" aria-label="Add tag">
          </div>

          <label style="font-family:var(--mono);font-size:10px;letter-spacing:.14em;color:var(--ink-3);text-transform:uppercase">Notes</label>
          <textarea class="notes-area" id="dNotes" placeholder="notes…">${esc(it.notes||'')}</textarea>

          <div style="display:flex;gap:8px;margin-top:14px;flex-wrap:wrap">
            <button class="btn ghost" onclick="patchMeta('${jit(jid)}',{watched:${it.watched?'false':'true'}}).then(()=>openDetail('${jit(jid)}'))">
              ${it.watched?'Mark unwatched':'Mark watched'}
            </button>
            <button class="btn ghost" onclick="patchMeta('${jit(jid)}',{keep:${it.keep?'false':'true'}}).then(()=>openDetail('${jit(jid)}'))">
              ${it.keep?'Unpin':'Pin'}
            </button>
            <button class="btn ghost" onclick="showTranscriptFor('${esc(it.url)}')">Transcript</button>
            <button class="btn ghost" onclick="deleteFromDetail('${jit(jid)}')">Delete</button>
          </div>
        </div>
      </div>`;
    $('detailWrap').classList.add('show');

    document.querySelectorAll('#dRating .star').forEach(s => {
      s.onclick = () => patchMeta(jit(jid), {rating: parseInt(s.dataset.n,10)}).then(()=>openDetail(jit(jid)));
    });
    document.querySelectorAll('#dTags .rm').forEach(rm => {
      rm.onclick = () => {
        const t = rm.dataset.t;
        const next = tags.filter(x => x !== t);
        patchMeta(jit(jid), {tags: next}).then(()=>openDetail(jit(jid)));
      };
    });
    const tagNew = $('dTagNew');
    tagNew.addEventListener('keydown', e => {
      if (e.key === 'Enter' && tagNew.value.trim()){
        const next = [...tags, tagNew.value.trim()];
        patchMeta(jit(jid), {tags: next}).then(()=>openDetail(jit(jid)));
      }
    });
    const notes = $('dNotes');
    let notesTimer;
    notes.oninput = () => { clearTimeout(notesTimer); notesTimer = setTimeout(() => {
      patchMeta(jit(jid), {notes: notes.value}, true);
    }, 800); };
  } catch(e){ toast(e.message, 'err'); }
}
function jit(x){ return String(x).replace(/'/g, "\\'"); }
async function patchMeta(jid, fields, silent){
  try {
    await api(`/api/history/${jid}/patch`, {method:'POST', headers:{'Content-Type':'application/json'},
      body: JSON.stringify(fields)});
    if (!silent) toast('saved', 'ok');
    if (S.view === 'library') loadLibrary();
  } catch(e){ toast(e.message, 'err'); }
}
function closeDetail(){ $('detailWrap').classList.remove('show'); }
async function deleteFromDetail(jid){
  const ok = await ask('Delete?', '<p>Removes the entry from your library. Files are kept if pinned.</p>');
  if (!ok) return;
  try {
    const r = await api(`/api/history/${jid}`, {method:'DELETE'});
    closeDetail(); loadLibrary();
    toast('deleted', 'ok', r.undo_id);
  } catch(e){ toast(e.message, 'err'); }
}

/* subscriptions */
async function loadSubscriptions(){
  try {
    const { items } = await api('/api/subscriptions');
    const box = $('subList');
    $('subRight').textContent = items.length + ' subs';
    if (!items.length){ box.innerHTML = '<div class="empty">no subscriptions</div>'; return; }
    box.innerHTML = items.map(s => `
      <div class="h-item">
        <div>
          <div class="t">${esc(s.title||s.url)}</div>
          <div class="m">
            <span>${esc(s.kind||'')}</span>
            <span>preset ${esc(s.preset||'')}</span>
            <span>every ${Math.round((s.check_every||3600)/60)}m</span>
            ${s.last_check?`<span>last checked ${fmtRel(s.last_check)} ago</span>`:'<span>never checked</span>'}
            ${s.enabled?'':'<span style="color:var(--warn)">paused</span>'}
          </div>
        </div>
        <div class="acts">
          <button class="btn ghost" onclick="checkSub('${s.id}')">Check now</button>
          <button class="btn ghost" onclick="toggleSub('${s.id}', ${s.enabled?0:1})">${s.enabled?'Pause':'Resume'}</button>
          <button class="btn ghost" onclick="unsub('${s.id}')">✕</button>
        </div>
      </div>`).join('');
  } catch(e){ toast(e.message, 'err'); }
}
async function addSubscription(){
  const url = $('subInput').value.trim();
  if (!url) return toast('paste a URL', 'err');
  try {
    const r = await api('/api/subscriptions', {method:'POST', headers:{'Content-Type':'application/json'},
      body: JSON.stringify({url, kind:'channel', preset: '720'})});
    toast('subscribed: ' + r.title, 'ok');
    $('subInput').value = '';
    loadSubscriptions();
  } catch(e){ toast(e.message, 'err'); }
}
async function checkSub(sid){
  try { await api(`/api/subscriptions/${sid}/check`, {method:'POST'}); toast('checking…'); }
  catch(e){ toast(e.message, 'err'); }
}
async function toggleSub(sid, enabled){
  try { await api(`/api/subscriptions/${sid}/toggle`, {method:'POST', headers:{'Content-Type':'application/json'},
    body: JSON.stringify({enabled: !!enabled})}); loadSubscriptions(); }
  catch(e){ toast(e.message, 'err'); }
}
async function unsub(sid){
  const ok = await ask('Unsubscribe?', '<p>Removes this subscription. Already downloaded files are kept.</p>');
  if (!ok) return;
  try { await api(`/api/subscriptions/${sid}`, {method:'DELETE'}); loadSubscriptions(); }
  catch(e){ toast(e.message, 'err'); }
}

/* queue */
function renderJobs(){
  let active = 0;
  S.jobs.forEach(j => { if (j.status === 'running' || j.status === 'queued') active++; });
  $('navQN').textContent = active > 0 ? '('+active+')' : '';
  let cur = null;
  S.jobs.forEach(j => {
    if (!cur && j.status === 'running') cur = j;
    if (!cur && j.status === 'queued') cur = j;
  });
  $('kCurrent').innerHTML = cur ? `
    <div class="q-mini">
      <div class="t">${esc(cur.title || cur.url || '')}</div>
      <div class="s"><span class="st ${cur.status}">${cur.status}</span>
        <span>${esc(cur.label || '')}</span></div>
      <div class="bar"><i style="width:${(cur.progress||0).toFixed(0)}%"></i></div>
    </div>` : '<div style="font-family:var(--mono);font-size:11px;color:var(--ink-3)">idle</div>';
  if (S.view === 'queue') renderQueueView();
}
function renderQueueView(){
  const list = [...S.jobs.values()]
    .filter(j => j.status === 'running' || j.status === 'queued')
    .sort((a,b) => (a.created_at||0) - (b.created_at||0));
  const box = $('queueList');
  $('qRight').textContent = list.length + ' active';
  if (!list.length){ box.innerHTML = '<div class="empty">queue is empty</div>'; return; }
  box.innerHTML = list.map(j => {
    const pct = Math.round(j.progress || 0);
    const r = 24, c = 2*Math.PI*r;
    const off = c - (pct/100)*c;
    const cls = j.status==='done'?'done':j.status==='error'?'err':'';
    return `<div class="q-item" draggable="true" data-jid="${j.job_id}">
      <span class="grab" aria-hidden="true">⋮⋮</span>
      <div class="ring ${cls}">
        <svg width="56" height="56" viewBox="0 0 56 56" aria-hidden="true">
          <circle class="track" cx="28" cy="28" r="${r}"></circle>
          <circle class="arc" cx="28" cy="28" r="${r}"
            stroke-dasharray="${c}" stroke-dashoffset="${off}"></circle>
        </svg>
        <div class="pct">${pct}%</div>
      </div>
      <div class="q-body">
        <div class="t">${esc(j.title || j.url || 'queued…')}</div>
        <div class="m">
          <span>${esc(j.label || '')}</span>
          ${j.speed ? `<b>${fmtSpeed(j.speed)}</b>` : ''}
          ${j.eta ? `<span>eta ${Math.round(j.eta)}s</span>` : ''}
          ${j.frag_index ? `<span>frag ${j.frag_index}/${j.frag_count}</span>` : ''}
        </div>
      </div>
      <div class="q-act">
        <button class="btn ghost tooltip" data-priority="${j.job_id}" data-tip="Move to front">↑</button>
        <button class="btn ghost" data-cancel="${j.job_id}">Cancel</button>
      </div>
    </div>`;
  }).join('');
  box.querySelectorAll('[data-cancel]').forEach(b => {
    b.onclick = async () => {
      try { await api(`/api/cancel/${b.dataset.cancel}`, {method:'POST'}); }
      catch(e){ toast(e.message, 'err'); }
    };
  });
  box.querySelectorAll('[data-priority]').forEach(b => {
    b.onclick = async () => {
      try { await api(`/api/queue/priority/${b.dataset.priority}`, {method:'POST'}); }
      catch(e){ toast(e.message, 'err'); }
    };
  });
  // drag reorder
  let dragEl = null;
  box.querySelectorAll('.q-item').forEach(el => {
    el.addEventListener('dragstart', e => { dragEl = el; el.classList.add('dragging'); });
    el.addEventListener('dragend', () => { if (dragEl) dragEl.classList.remove('dragging'); dragEl = null; box.querySelectorAll('.drop-target').forEach(x=>x.classList.remove('drop-target')); });
    el.addEventListener('dragover', e => { e.preventDefault(); el.classList.add('drop-target'); });
    el.addEventListener('dragleave', () => el.classList.remove('drop-target'));
    el.addEventListener('drop', e => {
      e.preventDefault(); el.classList.remove('drop-target');
      if (!dragEl || dragEl === el) return;
      const items = [...box.querySelectorAll('.q-item')];
      const from = items.indexOf(dragEl), to = items.indexOf(el);
      if (from < 0 || to < 0) return;
      if (from < to) el.after(dragEl); else el.before(dragEl);
      // visual only — no server priority for real reorder beyond ↑
    });
  });
}

/* favorites */
async function loadFavorites(){
  try {
    const { items } = await api('/api/favorites');
    S.favorites = new Set(items.map(i => i.video_id));
    const g = $('favoritesGrid');
    if (!items.length){ g.innerHTML = '<div class="empty">no favorites yet</div>'; return; }
    g.innerHTML = `<div class="fav-grid">${items.map(f => `
      <div class="fav-card">
        <div class="thumb" style="background-image:url('${esc(f.thumbnail||'')}')"></div>
        <div class="t">${esc(f.title||'')}</div>
        <div class="m">${esc(f.uploader||'')}${f.duration?' · '+fmtDur(f.duration):''}</div>
        <div class="row">
          <button class="btn primary" data-dl="${esc(f.url)}">Get</button>
          <button class="btn ghost" data-open="${esc(f.url)}">Inspect</button>
          <button class="btn ghost" data-unfav="${f.video_id}">✕</button>
        </div>
      </div>`).join('')}</div>`;
    g.querySelectorAll('[data-dl]').forEach(b => b.onclick = () => {
      S.url = b.dataset.dl; setView('download'); downloadPreset('best');
    });
    g.querySelectorAll('[data-open]').forEach(b => b.onclick = () => {
      setView('download'); $('urlInput').value = b.dataset.open; cmdGo();
    });
    g.querySelectorAll('[data-unfav]').forEach(b => b.onclick = async () => {
      try { await api(`/api/favorites/${b.dataset.unfav}`, {method:'DELETE'}); loadFavorites(); }
      catch(e){ toast(e.message, 'err'); }
    });
  } catch(e){ toast(e.message, 'err'); }
}

/* history */
let histT = null;
function debouncedHistory(){ clearTimeout(histT); histT = setTimeout(loadHistory, 200); }
function toggleBatchMode(){
  S.histBatch = !S.histBatch;
  $('batchModeBtn').classList.toggle('on', S.histBatch);
  $('histBulkBar').classList.toggle('on', S.histBatch);
  if (!S.histBatch) S.histSelected.clear();
  loadHistory();
}
function histBulkClear(){ S.histSelected.clear(); loadHistory(); updateHistBulk(); }
function updateHistBulk(){ $('histBulkN').textContent = S.histSelected.size; }
async function histBulkKeep(keep){
  if (!S.histSelected.size) return toast('nothing selected', 'err');
  try {
    await api('/api/batch/keep', {method:'POST', headers:{'Content-Type':'application/json'},
      body: JSON.stringify({ids: [...S.histSelected], keep})});
    toast(`${keep?'pinned':'unpinned'} ${S.histSelected.size}`, 'ok');
    S.histSelected.clear(); loadHistory(); updateHistBulk();
  } catch(e){ toast(e.message, 'err'); }
}
async function histBulkDelete(){
  if (!S.histSelected.size) return toast('nothing selected', 'err');
  const ok = await ask(`Delete ${S.histSelected.size} entries?`, '<p>Removes the entries. Pinned files are kept on disk.</p>');
  if (!ok) return;
  try {
    const r = await api('/api/batch/delete', {method:'POST', headers:{'Content-Type':'application/json'},
      body: JSON.stringify({ids: [...S.histSelected]})});
    toast(`deleted ${r.count}`, 'ok');
    S.histSelected.clear(); loadHistory(); updateHistBulk();
  } catch(e){ toast(e.message, 'err'); }
}
async function loadHistory(){
  const body = $('historyBody');
  const q = $('histQ')?.value.trim(); const s = $('histS')?.value; const sort = $('histSort')?.value;
  try {
    const params = new URLSearchParams({limit:'100'});
    if (q) params.set('q', q); if (s) params.set('status', s); if (sort) params.set('sort', sort);
    const { items } = await api('/api/history?' + params.toString());
    if (!items.length){ body.innerHTML = '<div class="empty">empty</div>'; return; }
    const groups = {};
    items.forEach(j => { const k = fmtDay(j.created_at); (groups[k]=groups[k]||[]).push(j); });
    body.innerHTML = Object.keys(groups).map(day => `
      <div class="day-head"><span>${esc(day)}</span>
        <span>${groups[day].length} · ${fmtSize(groups[day].reduce((a,x)=>a+(x.file_size||0),0))}</span></div>
      ${groups[day].map(j => {
        const st = j.status || 'queued';
        const cls = st==='done'?'ok':st==='error'?'err':st==='cancelled'?'cn':'';
        const rating = j.rating ? '★'.repeat(j.rating) : '';
        const sel = S.histSelected.has(j.job_id);
        return `<div class="h-item ${sel?'selected':''}" data-jid="${j.job_id}">
          <div>
            <div class="t">${S.histBatch ? `<input type="checkbox" ${sel?'checked':''} data-hchk="${j.job_id}" style="margin-right:8px;accent-color:var(--navy)">` : ''}${esc(j.title || j.url || '')}</div>
            <div class="m">
              <span class="st ${cls}">${st.toUpperCase()}</span>
              ${j.format_label ? `<span>${esc(j.format_label)}</span>` : ''}
              ${j.file_size ? `<span>${fmtSize(j.file_size)}</span>` : ''}
              ${rating ? `<span class="rating">${rating}</span>` : ''}
              ${j.keep ? `<span class="keep">pinned</span>` : ''}
              <span>${fmtRel(j.created_at)}</span>
            </div>
          </div>
          <div class="acts">
            ${st==='done' ? `<button class="btn primary" data-get="${j.job_id}">Get</button>` : ''}
            ${st==='done' ? `<button class="btn ghost" data-stream="${j.job_id}">Play</button>` : ''}
            ${st==='done' ? `<button class="btn ghost" data-pin="${j.job_id}" data-keep="${j.keep?0:1}" title="pin">${j.keep?'★':'☆'}</button>` : ''}
            <button class="btn ghost" data-retry="${j.job_id}">Retry</button>
            <button class="btn ghost" data-del="${j.job_id}">✕</button>
          </div>
        </div>`;
      }).join('')}
    `).join('');
    body.querySelectorAll('[data-hchk]').forEach(cb => {
      cb.onchange = () => {
        const jid = cb.dataset.hchk;
        if (cb.checked) S.histSelected.add(jid); else S.histSelected.delete(jid);
        updateHistBulk(); loadHistory();
      };
    });
    body.querySelectorAll('[data-get]').forEach(b => b.onclick = () => {
      window.location.href = apiPath(`/api/file/${b.dataset.get}`);
    });
    body.querySelectorAll('[data-stream]').forEach(b => b.onclick = () => {
      window.open(apiPath(`/api/stream/${b.dataset.stream}`), '_blank');
    });
    body.querySelectorAll('[data-pin]').forEach(b => b.onclick = async () => {
      try {
        await api(`/api/keep/${b.dataset.pin}`, {method:'POST', headers:{'Content-Type':'application/json'},
          body: JSON.stringify({keep: b.dataset.keep === '1'})});
        loadHistory();
      } catch(e){ toast(e.message, 'err'); }
    });
    body.querySelectorAll('[data-retry]').forEach(b => b.onclick = async () => {
      try { await api(`/api/history/${b.dataset.retry}/retry`, {method:'POST'});
        toast('retrying', 'ok'); }
      catch(e){ toast(e.message, 'err'); }
    });
    body.querySelectorAll('[data-del]').forEach(b => b.onclick = async () => {
      const ok = await ask('Delete entry?', '<p>Removes the history record. The file, if pinned, stays.</p>');
      if (!ok) return;
      try {
        const r = await api(`/api/history/${b.dataset.del}`, {method:'DELETE'});
        loadHistory();
        toast('deleted', 'ok', r.undo_id);
      } catch(e){ toast(e.message, 'err'); }
    });
  } catch(e){ body.innerHTML = `<div class="empty">${esc(e.message)}</div>`; }
}
async function exportHistory(){
  try {
    const r = await fetch(apiPath('/api/export/history'));
    const blob = await r.blob();
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url; a.download = 'grabtube-history.json'; a.click();
    URL.revokeObjectURL(url);
  } catch(e){ toast(e.message, 'err'); }
}
async function importHistory(){
  const inp = document.createElement('input');
  inp.type = 'file'; inp.accept = 'application/json';
  inp.onchange = async () => {
    const f = inp.files[0]; if (!f) return;
    try {
      const data = JSON.parse(await f.text());
      const r = await api('/api/import/history', {method:'POST', headers:{'Content-Type':'application/json'},
        body: JSON.stringify(data)});
      toast(`imported ${r.imported}`, 'ok');
      loadHistory();
    } catch(e){ toast(e.message, 'err'); }
  };
  inp.click();
}

/* stats */
async function loadStats(){
  try {
    const st = await api('/api/stats');
    animateNum($('sCount'), st.total_count);
    $('sSize').textContent = fmtSize(st.total_size);
    $('sSession').textContent = fmtSize(S.session.bytes || 0);
    const days = st.by_day.length;
    $('sAvg').textContent = days ? (st.total_count / days).toFixed(1) : '0';
    const chart = $('stChart'), x = $('stChartX');
    if (!st.by_day.length){ chart.innerHTML = '<div class="empty">no data yet</div>'; x.innerHTML = ''; }
    else {
      const max = Math.max(...st.by_day.map(d => d.s), 1);
      chart.innerHTML = st.by_day.map(d => `
        <div class="bar-col"><div class="bar" style="height:${(d.s/max)*100}%"
          data-v="${fmtSize(d.s)} · ${d.c} files"></div></div>`).join('');
      x.innerHTML = st.by_day.map(d => `<div class="lbl">${d.d.slice(5)}</div>`).join('');
      $('sDays').textContent = days + ' days';
    }
    const kb = $('stKinds');
    if (!st.by_kind.length){ kb.innerHTML = '<div class="empty">no data</div>'; }
    else {
      const max = Math.max(...st.by_kind.map(k => k.c), 1);
      kb.innerHTML = st.by_kind.map(k => `
        <div class="kind-row">
          <span class="n">${esc(k.k || '—')}</span>
          <span class="track"><i style="width:${(k.c/max)*100}%"></i></span>
          <span>${k.c}</span>
        </div>`).join('');
    }
    // heatmap
    const hm = st.heatmap || [];
    if (hm.length){
      const days = ['Sun','Mon','Tue','Wed','Thu','Fri','Sat'];
      const grid = Array.from({length:7}, () => Array(24).fill(0));
      hm.forEach(h => {
        const [d, hr] = h.w.split(' ');
        grid[parseInt(d,10)][parseInt(hr,10)] = h.c;
      });
      const max = Math.max(...grid.flat(), 1);
      const el = $('heatmap');
      el.innerHTML = `<div></div>${Array.from({length:24},(_,i)=>`<div class="hour">${i}</div>`).join('')}` +
        days.map((dn, di) => `<div class="lbl">${dn}</div>` +
          grid[di].map(c => {
            let lvl = 0;
            if (c > 0) lvl = c/max < 0.25 ? 1 : c/max < 0.5 ? 2 : c/max < 0.75 ? 3 : 4;
            return `<div class="cell" data-v="${lvl}" title="${c} downloads"></div>`;
          }).join('')
        ).join('');
    }
    // tags
    if (st.tags && st.tags.length){
      $('tagsSect').style.display = '';
      const max = Math.max(...st.tags.map(t => t[1]), 1);
      $('stTags').innerHTML = st.tags.map(([tg, c]) => `
        <div class="kind-row">
          <span class="n" style="cursor:pointer" onclick="filterByTag('${esc(tg)}')">${esc(tg)}</span>
          <span class="track"><i style="width:${(c/max)*100}%"></i></span>
          <span>${c}</span>
        </div>`).join('');
    } else $('tagsSect').style.display = 'none';
  } catch(e){ toast(e.message, 'err'); }
}
function filterByTag(tg){
  setView('library');
  $('libTag').value = tg;
  loadLibrary();
}
function animateNum(el, target){
  const start = performance.now(), dur = 700;
  const from = parseInt(el.textContent.replace(/[^\d]/g,'')) || 0;
  function step(now){
    const t = Math.min(1, (now - start) / dur);
    const eased = 1 - Math.pow(1 - t, 3);
    el.textContent = Math.round(from + (target - from) * eased).toLocaleString();
    if (t < 1) requestAnimationFrame(step);
  }
  requestAnimationFrame(step);
}

/* modal */
function openModal(jid){
  S.current = jid; S.speeds = [];
  $('mJob').textContent = jid.slice(0, 8);
  $('mStage').textContent = 'starting…';
  $('mTitle').textContent = '';
  $('mFill').style.width = '0%';
  $('mSpeed').textContent = '—'; $('mEta').textContent = '—';
  $('mDone').textContent = '—'; $('mFrag').textContent = '—';
  $('mLog').innerHTML = '';
  $('mCancel').disabled = false;
  $('mStream').style.display = 'none';
  $('modalWrap').classList.add('show');
  setTimeout(() => $('mCancel').focus(), 80);
  paintSpark();
}
function closeModal(){ $('modalWrap').classList.remove('show'); }
function updateModal(j){
  const pct = Math.max(0, Math.min(100, j.progress || 0));
  $('mFill').style.width = pct.toFixed(1) + '%';
  $('mMeter').setAttribute('aria-valuenow', pct.toFixed(0));
  $('mStage').textContent = (j.stage || 'working') + ' · ' + pct.toFixed(1) + '%';
  if (j.title) $('mTitle').textContent = j.title;
  $('mSpeed').textContent = j.speed ? fmtSpeed(j.speed) : '—';
  $('mEta').textContent = j.eta ? Math.round(j.eta) + 's' : '—';
  $('mDone').textContent = j.downloaded ? fmtSize(j.downloaded) : '—';
  $('mFrag').textContent = (j.frag_index != null && j.frag_count) ? `${j.frag_index}/${j.frag_count}` : '—';
  if (j.speed){ S.speeds.push(j.speed); if (S.speeds.length > 60) S.speeds.shift(); paintSpark(); }
  if (j.status === 'done') $('mStream').style.display = '';
}
function paintSpark(){
  const line = $('spLine'), area = $('spArea');
  if (S.speeds.length < 2){ line.setAttribute('d',''); area.setAttribute('d',''); return; }
  const W = 400, H = 56, N = S.speeds.length;
  const max = Math.max(...S.speeds, 1);
  const pts = S.speeds.map((v,i) => [i/(N-1)*W, H - (v/max)*(H-6) - 3]);
  const d = 'M' + pts.map(p => `${p[0].toFixed(1)} ${p[1].toFixed(1)}`).join(' L');
  line.setAttribute('d', d);
  area.setAttribute('d', d + ` L${W} ${H} L0 ${H} Z`);
}
function appendModalLog(line, level){
  const box = $('mLog');
  const d = document.createElement('div');
  d.className = 'ln ' + (level||'info');
  d.innerHTML = `<span class="ts">${fmtTime()}</span><span class="tx">${esc(line)}</span>`;
  box.appendChild(d);
  while (box.children.length > 200) box.firstChild.remove();
  box.scrollTop = box.scrollHeight;
}
async function cancelCurrent(){
  if (!S.current) return;
  $('mCancel').disabled = true;
  try { await api(`/api/cancel/${S.current}`, {method:'POST'}); }
  catch(e){ toast(e.message, 'err'); $('mCancel').disabled = false; }
}
function streamCurrent(){ if (!S.current) return; window.open(apiPath(`/api/stream/${S.current}`), '_blank'); }

/* transcript */
async function openTranscript(url){
  $('transcriptTitle').textContent = '';
  $('transcriptBody').textContent = 'loading…';
  $('transcriptWrap').classList.add('show');
  try {
    const d = await api('/api/transcript', {method:'POST', headers:{'Content-Type':'application/json'},
      body: JSON.stringify({url})});
    $('transcriptTitle').textContent = d.title || '';
    $('transcriptBody').textContent = d.has_transcript ? d.text : 'no transcript available for this video';
  } catch(e){ $('transcriptBody').textContent = 'failed: ' + e.message; }
}
function closeTranscript(){ $('transcriptWrap').classList.remove('show'); }
function showTranscriptFor(url){ openTranscript(url); }
async function copyTranscript(){
  try { await navigator.clipboard.writeText($('transcriptBody').textContent); toast('copied', 'ok'); }
  catch(e){ toast('copy failed', 'err'); }
}
function downloadTranscript(){
  const blob = new Blob([$('transcriptBody').textContent], {type:'text/plain'});
  const a = document.createElement('a');
  a.href = URL.createObjectURL(blob);
  a.download = ($('transcriptTitle').textContent || 'transcript') + '.txt';
  a.click();
}

/* bookmarklet */
function showBookmarklet(){
  const origin = location.origin + BASE_PATH;
  const code = `javascript:(()=>{const u=location.href;fetch('${origin}/api/download',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({url:u,preset:'720',keep:true})}).then(r=>r.json()).then(d=>alert('GrabTube queued: '+d.job_id)).catch(e=>alert('GrabTube error: '+e.message));})();`;
  const html = `
    <p>Drag this button to your bookmarks bar. Click it on any YouTube video page to queue a download without opening the app.</p>
    <div style="text-align:center;margin:20px 0">
      <a class="bookmarklet-drag" href='${code.replace(/'/g,"&#39;")}'>→ GrabTube</a>
    </div>
    <label>Or copy the bookmarklet code:</label>
    <div class="bookmarklet-box">${esc(code)}</div>
    <p style="font-size:12px;color:var(--ink-3)">Requires this app to be running locally. Change preset and options in the code if you want defaults different from 720p + keep.</p>`;
  $('dlgTitle').textContent = 'Bookmarklet';
  $('dlgMsg').innerHTML = html;
  $('dlgInput').style.display = 'none';
  $('dlgOk').textContent = 'Done';
  $('dialogWrap').classList.add('show');
  _dlgResolve = () => {};
}

/* settings */
async function openSettings(){
  const s = S.settings;
  const html = `
    <label>Cookies from browser</label>
    <select id="dlgCookies">
      ${['','chrome','firefox','edge','safari','brave','chromium'].map(v =>
        `<option value="${v}" ${s.cookies_from_browser===v?'selected':''}>${v||'— none —'}</option>`).join('')}
    </select>
    <label>Cookies file</label>
    <input id="dlgCookiesFile" type="text" value="${esc(s.cookies_file||'')}">
    <label>Rate limit</label>
    <input id="dlgRate" type="text" value="${esc(s.rate_limit||'')}" placeholder="e.g. 5M">
    <label>Proxy</label>
    <input id="dlgProxy" type="text" value="${esc(s.proxy||'')}" placeholder="socks5://…">
    <label>Filename template</label>
    <input id="dlgTmpl" type="text" value="${esc(s.filename_template||'%(title).120s.%(ext)s')}">
    <label>Organize by</label>
    <select id="dlgOrg">
      <option value="">flat (temp)</option>
      <option value="uploader" ${s.organize_by==='uploader'?'selected':''}>uploader/</option>
      <option value="date" ${s.organize_by==='date'?'selected':''}>yyyy/yyyy-mm/</option>
      <option value="both" ${s.organize_by==='both'?'selected':''}>uploader/yyyy/</option>
    </select>
    <label>SponsorBlock categories</label>
    <input id="dlgSB" type="text" value="${esc(s.sponsorblock_categories||'sponsor,selfpromo,interaction')}">
    <label>Webhook URL</label>
    <input id="dlgWH" type="text" value="${esc(s.webhook_url||'')}" placeholder="POST on completion">
    <label>Custom yt-dlp args</label>
    <input id="dlgExtra" type="text" value="${esc(s.extra_args||'')}" placeholder="--no-check-certificate">
    <label style="display:flex;gap:8px;align-items:center;margin-top:12px">
      <input id="dlgSkip" type="checkbox" ${s.skip_dupes!=='0'?'checked':''} style="width:auto;margin:0">
      skip duplicate downloads
    </label>
    <label style="display:flex;gap:8px;align-items:center;margin-top:8px">
      <input id="dlgUpd" type="checkbox" ${s.auto_update_ytdlp==='1'?'checked':''} style="width:auto;margin:0">
      auto-update yt-dlp on startup
    </label>
    <label style="display:flex;gap:8px;align-items:center;margin-top:8px">
      <input id="dlgSound" type="checkbox" ${s.notify_sound==='1'?'checked':''} style="width:auto;margin:0">
      sound on complete
    </label>
    <label style="display:flex;gap:8px;align-items:center;margin-top:8px">
      <input id="dlgNotify" type="checkbox" ${s.notify_desktop==='1'?'checked':''} style="width:auto;margin:0">
      desktop notifications
    </label>
    <label style="display:flex;gap:8px;align-items:center;margin-top:8px">
      <input id="dlgSaveProf" type="checkbox" style="width:auto;margin:0">
      save these as a profile named
      <input id="dlgProfName" type="text" placeholder="profile name" style="width:auto;margin:0;flex:1;padding:2px 6px;font-size:11px">
    </label>`;
  $('dlgTitle').textContent = 'Settings';
  $('dlgMsg').innerHTML = html;
  $('dlgInput').style.display = 'none';
  $('dlgOk').textContent = 'Save';
  $('dialogWrap').classList.add('show');
  _dlgResolve = async (ok) => {
    if (!ok) return;
    const payload = {
      cookies_from_browser: $('dlgCookies').value,
      cookies_file: $('dlgCookiesFile').value,
      rate_limit: $('dlgRate').value,
      proxy: $('dlgProxy').value,
      filename_template: $('dlgTmpl').value,
      organize_by: $('dlgOrg').value,
      sponsorblock_categories: $('dlgSB').value,
      webhook_url: $('dlgWH').value,
      extra_args: $('dlgExtra').value,
      skip_dupes: $('dlgSkip').checked ? '1' : '0',
      auto_update_ytdlp: $('dlgUpd').checked ? '1' : '0',
      notify_sound: $('dlgSound').checked ? '1' : '0',
      notify_desktop: $('dlgNotify').checked ? '1' : '0',
    };
    try {
      await api('/api/settings', {method:'POST', headers:{'Content-Type':'application/json'},
        body: JSON.stringify(payload)});
      S.settings = payload;
      toast('settings saved', 'ok');
      if ($('dlgSaveProf').checked && $('dlgProfName').value.trim()){
        await api('/api/profiles', {method:'POST', headers:{'Content-Type':'application/json'},
          body: JSON.stringify({name: $('dlgProfName').value.trim(), config: payload})});
        loadProfiles();
      }
      if ($('dlgNotify').checked && window.Notification && Notification.permission === 'default'){
        Notification.requestPermission();
      }
    } catch(e){ toast(e.message, 'err'); }
  };
}
async function loadSettings(){
  try { S.settings = await api('/api/settings'); } catch(_){ S.settings = {}; }
}

/* palette */
const CMDS = [
  {k:'download', label:'Go to Download', run:()=>setView('download')},
  {k:'search', label:'Go to Search', run:()=>{setView('search'); setTimeout(()=>$('searchInput').focus(),40);}},
  {k:'channel', label:'Go to Channel', run:()=>setView('channel')},
  {k:'library', label:'Go to Library', run:()=>setView('library')},
  {k:'subs', label:'Go to Subscriptions', run:()=>setView('subscriptions')},
  {k:'queue', label:'Go to Queue', run:()=>setView('queue')},
  {k:'favorites', label:'Go to Favorites', run:()=>setView('favorites')},
  {k:'history', label:'Go to History', run:()=>setView('history')},
  {k:'stats', label:'Go to Stats', run:()=>setView('stats')},
  {k:'focus', label:'Focus URL input', run:()=>{setView('download'); setTimeout(()=>{$('urlInput').focus();$('urlInput').select();},40);}},
  {k:'best', label:'Download best', run:()=>downloadPreset('best')},
  {k:'1080', label:'Download 1080p', run:()=>downloadPreset('1080')},
  {k:'720', label:'Download 720p', run:()=>downloadPreset('720')},
  {k:'mp3', label:'Download MP3', run:()=>downloadPreset('mp3')},
  {k:'flac', label:'Download FLAC', run:()=>downloadPreset('flac')},
  {k:'cancel', label:'Cancel current', run:()=>cancelCurrent()},
  {k:'pause', label:'Pause/resume queue', run:()=>togglePause()},
  {k:'console', label:'Toggle console', run:()=>toggleConsole()},
  {k:'theme', label:'Cycle theme', run:()=>cycleTheme()},
  {k:'density', label:'Cycle density', run:()=>cycleDensity()},
  {k:'focusmode', label:'Toggle focus mode', run:()=>toggleFocus()},
  {k:'bookmarklet', label:'Show bookmarklet', run:()=>showBookmarklet()},
  {k:'settings', label:'Open settings', run:()=>openSettings()},
  {k:'close', label:'Close modals', run:()=>{closeModal();closeDetail();closeTranscript();}},
];
function openPalette(){
  $('paletteWrap').classList.add('show');
  $('paletteInput').value = ''; $('paletteInput').focus();
  paintPalette('');
}
function closePalette(){ $('paletteWrap').classList.remove('show'); }
function paintPalette(q){
  const r = $('paletteResults');
  const items = CMDS.filter(c => !q || c.k.includes(q.toLowerCase()) || c.label.toLowerCase().includes(q.toLowerCase()));
  if (!items.length){ r.innerHTML = '<div class="empty">no commands match</div>'; return; }
  r.innerHTML = items.map((c,i)=>`<div class="item${i===0?' sel':''}" data-k="${esc(c.k)}" role="option">
    <span style="color:var(--red);font-family:var(--serif);font-style:italic">›</span>
    <span>${esc(c.label)}</span><span class="k">${esc(c.k)}</span>
  </div>`).join('');
  r.querySelectorAll('.item').forEach(el => {
    el.onclick = () => { const c = CMDS.find(x=>x.k===el.dataset.k); closePalette(); c && c.run(); };
  });
}
$('paletteInput').addEventListener('input', e => paintPalette(e.target.value));
$('paletteInput').addEventListener('keydown', e => {
  const items = $('paletteResults').querySelectorAll('.item'); if (!items.length) return;
  const sel = [...items].findIndex(x=>x.classList.contains('sel'));
  if (e.key === 'ArrowDown'){ e.preventDefault(); items[sel].classList.remove('sel');
    items[(sel+1)%items.length].classList.add('sel');
    items[(sel+1)%items.length].scrollIntoView({block:'nearest'}); }
  if (e.key === 'ArrowUp'){ e.preventDefault(); items[sel].classList.remove('sel');
    items[(sel-1+items.length)%items.length].classList.add('sel');
    items[(sel-1+items.length)%items.length].scrollIntoView({block:'nearest'}); }
  if (e.key === 'Enter'){ const cur = $('paletteResults').querySelector('.item.sel');
    if (cur){ const c = CMDS.find(x=>x.k===cur.dataset.k); closePalette(); c && c.run(); } }
});

/* keyboard shortcuts */
document.addEventListener('keydown', e => {
  const inField = ['INPUT','SELECT','TEXTAREA'].includes(document.activeElement.tagName);
  if ((e.metaKey || e.ctrlKey) && e.key.toLowerCase() === 'k'){ e.preventDefault(); openPalette(); return; }
  if (e.key === 'Escape'){
    if ($('dialogWrap').classList.contains('show')){ closeDialog(false); return; }
    if ($('paletteWrap').classList.contains('show')){ closePalette(); return; }
    if ($('previewWrap').classList.contains('show')){ closePreview(); return; }
    if ($('transcriptWrap').classList.contains('show')){ closeTranscript(); return; }
    if ($('detailWrap').classList.contains('show')){ closeDetail(); return; }
    if ($('modalWrap').classList.contains('show')){ closeModal(); return; }
    if (inField){ document.activeElement.blur(); return; }
    return;
  }
  if (inField) return;
  if (e.key === '/'){ e.preventDefault(); setView('download');
    setTimeout(() => { $('urlInput').focus(); $('urlInput').select(); }, 40); return; }
  if (e.key === 'd'){ cycleTheme(); return; }
  if (e.key === 'c' && e.shiftKey){ toggleConsole(); return; }
  if (e.key === '?' ){ openPalette(); return; }
  if (S.view !== 'download') return;
  if (!S.data || S.data.type !== 'video') return;
  const rows = (S.data.formats || []).filter(f => S.filter === 'all' ? true : f.kind === S.filter);
  if (!rows.length) return;
  if (e.key === 'ArrowDown'){ e.preventDefault();
    S.selRow = Math.min(rows.length-1, (S.selRow < 0 ? -1 : S.selRow) + 1); paintFormats();
  } else if (e.key === 'ArrowUp'){ e.preventDefault();
    S.selRow = Math.max(0, (S.selRow < 0 ? rows.length : S.selRow) - 1); paintFormats();
  } else if (e.key === 'Enter' && S.selRow >= 0){ e.preventDefault();
    const f = rows[S.selRow]; downloadFormat(f.format_id, f.kind);
  } else if (e.key === 'c' && S.selRow >= 0){ e.preventDefault();
    S.compare = (S.compare === S.selRow) ? -1 : S.selRow; paintFormats();
  }
});

/* autocomplete for URL bar */
let autoTimer = null;
$('urlInput').addEventListener('input', () => {
  clearTimeout(autoTimer);
  autoTimer = setTimeout(runAutocomplete, 120);
});
$('urlInput').addEventListener('blur', () => setTimeout(() => $('urlAutocomplete').classList.remove('on'), 200));
async function runAutocomplete(){
  const q = $('urlInput').value.trim();
  const box = $('urlAutocomplete');
  if (!q || /^https?:\/\//i.test(q)){ box.classList.remove('on'); return; }
  try {
    const { items } = await api('/api/history?limit=8&q=' + encodeURIComponent(q));
    if (!items.length){ box.classList.remove('on'); return; }
    box.innerHTML = items.map(it => `<div class="row" data-url="${esc(it.url||'')}">
      <span class="sym">›</span>
      <span>${esc((it.title||it.url||'').slice(0,70))}</span>
      <span class="meta">${it.status}</span>
    </div>`).join('');
    box.querySelectorAll('.row').forEach(r => {
      r.onmousedown = (e) => { e.preventDefault(); $('urlInput').value = r.dataset.url; box.classList.remove('on'); cmdGo(); };
    });
    box.classList.add('on');
  } catch(_){ box.classList.remove('on'); }
}
$('urlInput').addEventListener('keydown', e => {
  if (e.key === 'Enter') { $('urlAutocomplete').classList.remove('on'); cmdGo(); }
  if (e.key === 'Escape'){ $('urlAutocomplete').classList.remove('on'); }
});
$('searchInput')?.addEventListener('keydown', e => { if (e.key === 'Enter') doSearch(); });
$('channelInput')?.addEventListener('keydown', e => { if (e.key === 'Enter') loadChannel(); });
$('subInput')?.addEventListener('keydown', e => { if (e.key === 'Enter') addSubscription(); });

/* drag-drop */
let dragDepth = 0;
document.addEventListener('dragenter', e => { e.preventDefault(); dragDepth++;
  if ($('cmdFrame')) $('cmdFrame').classList.add('drop'); });
document.addEventListener('dragover', e => e.preventDefault());
document.addEventListener('dragleave', () => { dragDepth--; if (dragDepth <= 0){ dragDepth = 0;
  if ($('cmdFrame')) $('cmdFrame').classList.remove('drop'); } });
document.addEventListener('drop', e => {
  e.preventDefault(); dragDepth = 0;
  if ($('cmdFrame')) $('cmdFrame').classList.remove('drop');
  const txt = e.dataTransfer.getData('text/plain') || e.dataTransfer.getData('text/uri-list');
  if (txt){
    setView('download');
    $('urlInput').value = txt.trim();
    cmdGo();
  }
});

/* preview modal (kept for future use; currently the thumbnail swaps to embed) */
function closePreview(){ $('previewWrap').classList.remove('show'); }

/* recent */
async function refreshRecent(){
  try {
    const { items } = await api('/api/history?limit=6');
    const done = items.filter(x => x.status === 'done').slice(0,3);
    $('kRecent').innerHTML = done.map(j => `
      <div class="q-mini" style="cursor:pointer" onclick="window.location.href=apiPath('/api/file/${j.job_id}')">
        <div class="t">${esc(j.title || j.url || '')}</div>
        <div class="s"><span class="st done">done</span>
          <span>${j.file_size?fmtSize(j.file_size):''} · ${fmtRel(j.created_at)}</span></div>
      </div>`).join('') || '<div style="font-family:var(--mono);font-size:11px;color:var(--ink-3)">nothing yet</div>';
  } catch(_){}
}

/* init */
(async () => {
  S.theme = localStorage.getItem('gt_theme') || 'paper';
  S.density = localStorage.getItem('gt_density') || 'cozy';
  S.scale = parseFloat(localStorage.getItem('gt_scale') || '1');
  S.focusMode = localStorage.getItem('gt_focus') === '1';
  applyTheme(); applyDensity(); applyScale();
  if (S.focusMode) toggleFocus();
  await loadSettings();
  const h = location.hash.replace('#/','') || 'download';
  setView(h);
  pollHealth();
  connectWS();
  loadPresets();
  loadProfiles();
  refreshRecent();
  setInterval(refreshRecent, 4000);
  window.apiPath = apiPath;
})();
</script>
</body>
</html>
"""


@app.get("/", response_class=HTMLResponse)
async def index(): return HTML


def main():
    global ALLOW_ANY_URL, MAX_CONCURRENT, SEM, BASE_PATH
    ap = argparse.ArgumentParser(description="GrabTube")
    ap.add_argument("--host", default=os.environ.get("GRABTUBE_HOST","127.0.0.1"))
    ap.add_argument("--port", type=int, default=int(os.environ.get("GRABTUBE_PORT","8000")))
    ap.add_argument("--allow-any-url", action="store_true", default=ALLOW_ANY_URL)
    ap.add_argument("--max-concurrent", type=int, default=MAX_CONCURRENT)
    ap.add_argument("--base-path", default=BASE_PATH)
    args = ap.parse_args()
    ALLOW_ANY_URL = args.allow_any_url
    MAX_CONCURRENT = max(1, args.max_concurrent)
    SEM = threading.Semaphore(MAX_CONCURRENT)
    BASE_PATH = (args.base_path or "").rstrip("/")
    has_ws = False
    for m in ("websockets","wsproto"):
        try: __import__(m); has_ws = True; break
        except ImportError: pass
    if not shutil.which("ffmpeg"):
        print("  !  ffmpeg not in PATH — merging/conversion/subtitles will fail.\n")
    if not has_ws:
        print("  !  no websocket lib — falling back to polling.")
        print("     pip install websockets\n")
    if not ALLOW_ANY_URL:
        print("  ·  allowed: " + ", ".join(sorted(ALLOWED_HOSTS)))
        print("     --allow-any-url for any site\n")
    path = BASE_PATH + "/" if BASE_PATH else ""
    print(f"  ›  http://{args.host}:{args.port}{path}\n")
    if BASE_PATH:
        # mount at subpath via a middleware-free approach: routes still match
        # but frontend uses BASE_PATH for assets — user is responsible for
        # reverse-proxy stripping the prefix or keeping it.
        pass
    uvicorn.run(app, host=args.host, port=args.port, log_level="warning")


if __name__ == "__main__":
    main()