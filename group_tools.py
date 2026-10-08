# -*- coding: utf-8 -*-
"""
group_tools.py
---------------------------------------------------------
ماژول ایزوله برای ۲ قابلیت مدیریت گروه:

    1) آمار فعالیت اعضا (۱، ۷ و ۳۰ روز گذشته)
       - شمارش پیام هر عضو در حافظه و ذخیره‌ی دسته‌ای در Supabase / SQLite
       - دستور دستی /stats یا «آمار»، «امار 1»، «آمار 30» و... (فقط ادمین‌ها)
       - ارسال خودکار روزانه (اختیاری، با STATS_AUTO_TIME)

    2) عضویت اجباری کانال
       - پیام کاربری که عضو کانال نیست حذف می‌شود
       - یک پیام هشدار با دکمه‌ی «عضویت در کانال» و «عضو شدم» می‌آید
       - ادمین‌های گروه و پیام‌های کانال/ادمین ناشناس معاف‌اند

اتصال به main.py فقط ۴ خط است (انتهای همین فایل را ببینید).

متغیرهای محیطی (همه اختیاری):
    FORCE_JOIN_ENABLED   = 1 / 0                     (پیش‌فرض 1)
    FORCE_JOIN_MODE      = enforce / log             (پیش‌فرض enforce؛ log = فقط ثبت، بدون حذف و هشدار)
    FORCE_JOIN_WARMUP    = 0                         (اختیاری: ثانیه‌های اول بعد از استارت فقط ثبت شود، بعد اجبار)
    MEMBER_CACHE_TTL     = 21600                     (اعتبار تأیید عضویت به ثانیه؛ بعدش یک بار دوباره چک می‌شود)
    MEMBERS_TABLE        = channel_members           (جدول Supabase؛ با SUPABASE_URL و SUPABASE_KEY)
    FORCE_JOIN_CHANNEL   = @SuperExNews_Iran         (یوزرنیم یا آیدی عددی کانال)
    FORCE_JOIN_URL       = https://t.me/...          (فقط اگر کانال خصوصی است)
    GROUP_IDS            = -1001234,-1005678         (خالی = همه‌ی گروه‌هایی که ربات در آن‌هاست)
    GROUP_STATS_DB       = group_stats.sqlite3       (روی Render مسیر دیسک پایدار بدهید)
    STATS_TOP_N          = 50
    MEMBER_CACHE_TTL     = 21600                     (شبکه‌ی ایمنی؛ لازم نیست دست بزنید)
    STATS_AUTO_TIME      = 23:00                     (به وقت تهران؛ خالی = ارسال خودکار خاموش)
---------------------------------------------------------
"""

import asyncio
import html
import logging
import os
import re
import sqlite3
import threading
import time
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Set, Tuple

import requests
import premium_emoji as pe
from aiogram import Bot, Dispatcher, Router, types, F
from aiogram.exceptions import TelegramBadRequest
from aiogram.filters import Command
from aiogram.filters.callback_data import CallbackData
from aiogram.dispatcher.middlewares.base import BaseMiddleware
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

logger = logging.getLogger("group_tools")

group_router = Router(name="group_tools_router")

# ===========================================================
# Config
# ===========================================================
TEHRAN_TZ = timezone(timedelta(hours=3, minutes=30))  # ایران ساعت تابستانی ندارد

FORCE_JOIN_ENABLED = os.getenv("FORCE_JOIN_ENABLED", "1").strip() not in ("0", "false", "False", "")
FORCE_JOIN_MODE = os.getenv("FORCE_JOIN_MODE", "enforce").strip().lower()      # enforce | log
FORCE_JOIN_WARMUP = int(os.getenv("FORCE_JOIN_WARMUP", "0"))                   # ثانیه
SUPABASE_URL = os.getenv("SUPABASE_URL", "").strip().rstrip("/")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "").strip()
MEMBERS_TABLE = os.getenv("MEMBERS_TABLE", "channel_members").strip()
_raw_channel = os.getenv("FORCE_JOIN_CHANNEL", "@SuperExNews_Iran").strip()
FORCE_CHANNEL = int(_raw_channel) if _raw_channel.lstrip("-").isdigit() else _raw_channel
CHANNEL_KEY = str(FORCE_CHANNEL).lower()   # داخل جدول ثبت می‌شود تا با عوض شدن کانال، دیتای کانال قبلی باور نشود
FORCE_CHANNEL_URL = os.getenv("FORCE_JOIN_URL") or (
    f"https://t.me/{_raw_channel.lstrip('@')}" if not _raw_channel.lstrip("-").isdigit() else "https://t.me/SuperExNews_Iran"
)

_raw_groups = os.getenv("GROUP_IDS", "").strip()
ALLOWED_GROUP_IDS: Set[int] = {int(x) for x in _raw_groups.split(",") if x.strip().lstrip("-").isdigit()}

DB_PATH = os.getenv("GROUP_STATS_DB", "group_stats.sqlite3")
STATS_TOP_N = int(os.getenv("STATS_TOP_N", "50"))
STATS_AUTO_TIME = os.getenv("STATS_AUTO_TIME", "").strip()  # "23:00"
STATS_WINDOW_DAYS = 7

FLUSH_INTERVAL = 10          # ثانیه: هر چند ثانیه شمارنده‌ها در دیتابیس ذخیره شوند
MEMBER_CACHE_TTL = int(os.getenv("MEMBER_CACHE_TTL", "21600"))  # فقط شبکه‌ی ایمنی (۶ ساعت)؛ خروج از کانال با رویداد chat_member فوراً از کش پاک می‌شود
ADMIN_CACHE_TTL = 300
WARN_COOLDOWN = 30           # هر کاربر حداکثر هر ۳۰ ثانیه یک هشدار می‌گیرد
WARN_DELETE_AFTER = 25       # پیام هشدار بعد از این مدت پاک می‌شود

# پیام‌های سرویسی تلگرام (ورود/خروج، پین، ...) که نه شمرده می‌شوند و نه محدودیت می‌خورند
_SERVICE_TYPES = {
    "new_chat_members", "left_chat_member", "new_chat_title", "new_chat_photo",
    "delete_chat_photo", "group_chat_created", "supergroup_chat_created",
    "channel_chat_created", "message_auto_delete_timer_changed", "migrate_to_chat_id",
    "migrate_from_chat_id", "pinned_message", "forum_topic_created", "forum_topic_edited",
    "forum_topic_closed", "forum_topic_reopened", "general_forum_topic_hidden",
    "general_forum_topic_unhidden", "video_chat_scheduled", "video_chat_started",
    "video_chat_ended", "video_chat_participants_invited", "write_access_allowed",
    "boost_added", "chat_background_set",
}
_TELEGRAM_SERVICE_IDS = {777000, 1087968824}  # Telegram / GroupAnonymousBot


def _chat_allowed(chat_id: int) -> bool:
    return not ALLOWED_GROUP_IDS or chat_id in ALLOWED_GROUP_IDS


# ===========================================================
# Persian date helpers (بدون نیاز به کتابخانه‌ی اضافه)
# ===========================================================
_FA_DIGITS = str.maketrans("0123456789", "۰۱۲۳۴۵۶۷۸۹")
_FA_MONTHS = ["فروردین", "اردیبهشت", "خرداد", "تیر", "مرداد", "شهریور",
              "مهر", "آبان", "آذر", "دی", "بهمن", "اسفند"]
_FA_WEEKDAYS = ["دوشنبه", "سه‌شنبه", "چهارشنبه", "پنجشنبه", "جمعه", "شنبه", "یکشنبه"]  # weekday(): Mon=0


def fa_digits(text: str) -> str:
    return str(text).translate(_FA_DIGITS)


def gregorian_to_jalali(gy: int, gm: int, gd: int) -> Tuple[int, int, int]:
    g_d_m = [0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334]
    gy2 = gy + 1 if gm > 2 else gy
    days = (355666 + (365 * gy) + ((gy2 + 3) // 4) - ((gy2 + 99) // 100)
            + ((gy2 + 399) // 400) + gd + g_d_m[gm - 1])
    jy = -1595 + (33 * (days // 12053))
    days %= 12053
    jy += 4 * (days // 1461)
    days %= 1461
    if days > 365:
        jy += (days - 1) // 365
        days = (days - 1) % 365
    if days < 186:
        jm = 1 + (days // 31)
        jd = 1 + (days % 31)
    else:
        jm = 7 + ((days - 186) // 30)
        jd = 1 + ((days - 186) % 30)
    return jy, jm, jd


def _now_tehran() -> datetime:
    return datetime.now(TEHRAN_TZ)


# ===========================================================
# Supabase & SQLite Helpers
# ===========================================================
def _use_supabase() -> bool:
    return bool(SUPABASE_URL and SUPABASE_KEY)

def _supabase_headers(prefer: Optional[str] = None) -> dict:
    headers = {"apikey": SUPABASE_KEY, "Content-Type": "application/json"}
    if SUPABASE_KEY.startswith("eyJ"):
        headers["Authorization"] = f"Bearer {SUPABASE_KEY}"
    if prefer:
        headers["Prefer"] = prefer
    return headers

_db_lock = threading.Lock()
_conn: Optional[sqlite3.Connection] = None


def _get_conn() -> sqlite3.Connection:
    global _conn
    if _conn is None:
        _conn = sqlite3.connect(DB_PATH, check_same_thread=False)
        _conn.execute("PRAGMA journal_mode=WAL")
        _conn.execute("""CREATE TABLE IF NOT EXISTS msg_counts (
            chat_id INTEGER NOT NULL, user_id INTEGER NOT NULL, day TEXT NOT NULL,
            cnt INTEGER NOT NULL DEFAULT 0, PRIMARY KEY (chat_id, user_id, day))""")
        _conn.execute("""CREATE TABLE IF NOT EXISTS users (
            chat_id INTEGER NOT NULL, user_id INTEGER NOT NULL, name TEXT,
            is_channel INTEGER NOT NULL DEFAULT 0, PRIMARY KEY (chat_id, user_id))""")
        _conn.execute("CREATE TABLE IF NOT EXISTS meta (k TEXT PRIMARY KEY, v TEXT)")
        _conn.execute("""CREATE TABLE IF NOT EXISTS channel_members (
            user_id INTEGER PRIMARY KEY, is_member INTEGER NOT NULL, full_name TEXT,
            username TEXT, source TEXT, checked_at TEXT, channel TEXT)""")
        try:                                          # دیتابیس‌های محلیِ قدیمی بدون ستون channel
            _conn.execute("ALTER TABLE channel_members ADD COLUMN channel TEXT")
        except sqlite3.OperationalError:
            pass
        _conn.commit()
    return _conn


def _flush_sync(counts: Dict[Tuple[int, int, str], int], names: Dict[Tuple[int, int], Tuple[str, int]]) -> None:
    if _use_supabase():
        # آپدیت نام کاربری در Supabase (جدول group_users)
        if names:
            rows_users = [{"chat_id": c, "user_id": u, "name": nm, "is_channel": bool(ch)} for (c, u), (nm, ch) in names.items()]
            for i in range(0, len(rows_users), 500):
                requests.post(f"{SUPABASE_URL}/rest/v1/group_users?on_conflict=chat_id,user_id",
                              json=rows_users[i:i + 500], headers=_supabase_headers("resolution=merge-duplicates,return=minimal"))
        
        # آپدیت تعداد پیام‌ها در Supabase (جدول msg_counts)
        if counts:
            for (c, u, d), n in counts.items():
                url = f"{SUPABASE_URL}/rest/v1/msg_counts"
                resp = requests.get(url, params={"chat_id": f"eq.{c}", "user_id": f"eq.{u}", "day": f"eq.{d}", "select": "cnt"}, headers=_supabase_headers())
                old_cnt = 0
                if resp.status_code == 200 and resp.json():
                    old_cnt = resp.json()[0].get("cnt", 0)
                new_cnt = old_cnt + n
                requests.post(f"{url}?on_conflict=chat_id,user_id,day", 
                              json={"chat_id": c, "user_id": u, "day": d, "cnt": new_cnt}, 
                              headers=_supabase_headers("resolution=merge-duplicates,return=minimal"))
        return

    # روش دیتابیس محلی (SQLite)
    with _db_lock:
        conn = _get_conn()
        conn.executemany(
            "INSERT INTO msg_counts (chat_id, user_id, day, cnt) VALUES (?,?,?,?) "
            "ON CONFLICT(chat_id, user_id, day) DO UPDATE SET cnt = cnt + excluded.cnt",
            [(c, u, d, n) for (c, u, d), n in counts.items()],
        )
        conn.executemany(
            "INSERT INTO users (chat_id, user_id, name, is_channel) VALUES (?,?,?,?) "
            "ON CONFLICT(chat_id, user_id) DO UPDATE SET name = excluded.name, is_channel = excluded.is_channel",
            [(c, u, nm, ch) for (c, u), (nm, ch) in names.items()],
        )
        conn.commit()


def _query_top_sync(chat_id: int, since_day: str, limit: int) -> List[Tuple[int, int, str, int]]:
    if _use_supabase():
        counts_url = f"{SUPABASE_URL}/rest/v1/msg_counts"
        all_counts = []
        offset = 0
        while True:
            resp = requests.get(counts_url, params={"chat_id": f"eq.{chat_id}", "day": f"gte.{since_day}", "select": "user_id,cnt", "limit": "1000", "offset": str(offset)}, headers=_supabase_headers())
            if resp.status_code != 200:
                break
            data = resp.json()
            all_counts.extend(data)
            if len(data) < 1000:
                break
            offset += 1000

        user_totals = {}
        for row in all_counts:
            u = int(row["user_id"])
            user_totals[u] = user_totals.get(u, 0) + int(row["cnt"])

        if not user_totals:
            return []

        top_users = sorted(user_totals.items(), key=lambda x: x[1], reverse=True)[:limit]
        top_user_ids = [u for u, _ in top_users]

        users_url = f"{SUPABASE_URL}/rest/v1/group_users"
        u_ids_str = ",".join(map(str, top_user_ids))
        user_info = {}
        if top_user_ids:
            resp_users = requests.get(users_url, params={"chat_id": f"eq.{chat_id}", "user_id": f"in.({u_ids_str})", "select": "user_id,name,is_channel"}, headers=_supabase_headers())
            if resp_users.status_code == 200:
                for r in resp_users.json():
                    user_info[int(r["user_id"])] = (r.get("name") or "", 1 if r.get("is_channel") else 0)

        result = []
        for u, total in top_users:
            name, is_chan = user_info.get(u, ("", 0))
            result.append((u, total, name, is_chan))
        return result

    # روش دیتابیس محلی (SQLite)
    with _db_lock:
        cur = _get_conn().execute(
            "SELECT m.user_id, SUM(m.cnt) AS total, COALESCE(u.name, ''), COALESCE(u.is_channel, 0) "
            "FROM msg_counts m LEFT JOIN users u ON u.chat_id = m.chat_id AND u.user_id = m.user_id "
            "WHERE m.chat_id = ? AND m.day >= ? GROUP BY m.user_id "
            "ORDER BY total DESC, m.user_id LIMIT ?",
            (chat_id, since_day, limit),
        )
        return cur.fetchall()


def _active_chats_sync(since_day: str) -> List[int]:
    if _use_supabase():
        url = f"{SUPABASE_URL}/rest/v1/msg_counts"
        chat_ids = set()
        offset = 0
        while True:
            resp = requests.get(url, params={"day": f"gte.{since_day}", "select": "chat_id", "limit": "1000", "offset": str(offset)}, headers=_supabase_headers())
            if resp.status_code != 200:
                break
            data = resp.json()
            for r in data:
                chat_ids.add(int(r["chat_id"]))
            if len(data) < 1000:
                break
            offset += 1000
        return list(chat_ids)

    # روش دیتابیس محلی (SQLite)
    with _db_lock:
        cur = _get_conn().execute("SELECT DISTINCT chat_id FROM msg_counts WHERE day >= ?", (since_day,))
        return [r[0] for r in cur.fetchall()]


def _prune_sync(before_day: str) -> None:
    if _use_supabase():
        url = f"{SUPABASE_URL}/rest/v1/msg_counts"
        requests.delete(url, params={"day": f"lt.{before_day}"}, headers=_supabase_headers())
        return

    # روش دیتابیس محلی (SQLite)
    with _db_lock:
        conn = _get_conn()
        conn.execute("DELETE FROM msg_counts WHERE day < ?", (before_day,))
        conn.commit()


def _meta_get_sync(key: str) -> Optional[str]:
    if _use_supabase():
        url = f"{SUPABASE_URL}/rest/v1/group_meta"
        resp = requests.get(url, params={"k": f"eq.{key}", "select": "v"}, headers=_supabase_headers())
        if resp.status_code == 200:
            data = resp.json()
            if data:
                return data[0].get("v")
        return None

    # روش دیتابیس محلی (SQLite)
    with _db_lock:
        row = _get_conn().execute("SELECT v FROM meta WHERE k = ?", (key,)).fetchone()
        return row[0] if row else None


def _meta_set_sync(key: str, value: str) -> None:
    if _use_supabase():
        url = f"{SUPABASE_URL}/rest/v1/group_meta"
        requests.post(f"{url}?on_conflict=k", json={"k": key, "v": value}, headers=_supabase_headers("resolution=merge-duplicates,return=minimal"))
        return

    # روش دیتابیس محلی (SQLite)
    with _db_lock:
        conn = _get_conn()
        conn.execute("INSERT INTO meta (k, v) VALUES (?, ?) ON CONFLICT(k) DO UPDATE SET v = excluded.v", (key, value))
        conn.commit()


# ===========================================================
# 1) آمار فعالیت
# ===========================================================
_buf_counts: Dict[Tuple[int, int, str], int] = {}
_buf_names: Dict[Tuple[int, int], Tuple[str, int]] = {}


def _record(chat_id: int, user_id: int, name: str, is_channel: bool) -> None:
    day = _now_tehran().strftime("%Y-%m-%d")
    key = (chat_id, user_id, day)
    _buf_counts[key] = _buf_counts.get(key, 0) + 1
    _buf_names[(chat_id, user_id)] = (name[:60], 1 if is_channel else 0)


# ----- جدول وضعیت عضویت کاربران در کانال (Supabase؛ در نبودش SQLite محلی) -----
_started_at = time.time()
_enforce_announced = False
_cache_loaded = False
_last_load_try = 0.0
_selfchecked = False
_force_channel_id: Optional[int] = None
_member_buf: Dict[int, dict] = {}                 # user_id -> آخرین ردیف برای نوشتن
_member_last: Dict[int, Tuple[bool, float]] = {}  # برای جلوگیری از نوشتن تکراریِ یک وضعیت


def _parse_ts(value: str) -> float:
    """زمان ISO را به timestamp تبدیل می‌کند (۰ اگر نامعتبر)."""
    try:
        text = re.sub(r"(\.\d+)", lambda m: (m.group(1) + "000000")[:7], str(value).replace("Z", "+00:00"))
        dt = datetime.fromisoformat(text)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.timestamp()
    except (ValueError, TypeError):
        return 0.0


def _remember_member(user_id: int, full_name: str, username: Optional[str], is_member: bool, source: str) -> None:
    """وضعیت عضویت را در بافر می‌گذارد (نوشتن واقعی دسته‌ای و در پس‌زمینه است)."""
    now = time.time()
    last = _member_last.get(user_id)
    if source == "check" and last and last[0] == is_member and now - last[1] < 300:
        return
    _member_last[user_id] = (is_member, now)
    _member_buf[user_id] = {
        "user_id": user_id,
        "is_member": is_member,
        "full_name": (full_name or "")[:80],
        "username": username or "",
        "source": source,
        "channel": CHANNEL_KEY,
        "checked_at": datetime.now(timezone.utc).isoformat(),
    }


def _members_write_sync(rows: List[dict]) -> None:
    if _use_supabase():
        for i in range(0, len(rows), 500):
            resp = requests.post(
                f"{SUPABASE_URL}/rest/v1/{MEMBERS_TABLE}?on_conflict=user_id",
                json=rows[i:i + 500], headers=_supabase_headers("resolution=merge-duplicates,return=minimal"),
                timeout=(10, 20))
            if resp.status_code >= 300:
                raise RuntimeError(f"Supabase HTTP {resp.status_code}: {resp.text[:200]}")
        return
    with _db_lock:                                   # جایگزین محلی
        conn = _get_conn()
        conn.executemany(
            "INSERT INTO channel_members (user_id, is_member, full_name, username, source, checked_at, channel) "
            "VALUES (:user_id, :is_member, :full_name, :username, :source, :checked_at, :channel) "
            "ON CONFLICT(user_id) DO UPDATE SET is_member=excluded.is_member, full_name=excluded.full_name, "
            "username=excluded.username, source=excluded.source, checked_at=excluded.checked_at, "
            "channel=excluded.channel",
            [{**r, "is_member": 1 if r["is_member"] else 0} for r in rows],
        )
        conn.commit()


def _members_load_sync(since_iso: str) -> List[Tuple[int, str]]:
    """عضوهای تأییدشده‌ی همین کانال که تازه‌تر از since_iso بررسی شده‌اند."""
    if _use_supabase():
        out: List[Tuple[int, str]] = []
        offset = 0
        while True:
            resp = requests.get(
                f"{SUPABASE_URL}/rest/v1/{MEMBERS_TABLE}",
                params={"select": "user_id,checked_at", "is_member": "eq.true", "channel": f"eq.{CHANNEL_KEY}",
                        "checked_at": f"gte.{since_iso}", "order": "user_id.asc",
                        "limit": "1000", "offset": str(offset)},
                headers=_supabase_headers(), timeout=(10, 30))
            if resp.status_code >= 300:
                raise RuntimeError(f"Supabase HTTP {resp.status_code}: {resp.text[:200]}")
            page = resp.json()
            out.extend((int(r["user_id"]), str(r["checked_at"])) for r in page)
            if len(page) < 1000 or offset >= 500000:
                return out
            offset += 1000
    with _db_lock:
        rows = _get_conn().execute(
            "SELECT user_id, checked_at FROM channel_members WHERE is_member = 1 AND channel = ? AND checked_at >= ?",
            (CHANNEL_KEY, since_iso)).fetchall()
    return [(int(u), str(t)) for u, t in rows]


async def load_member_cache() -> None:
    """
    بعد از هر استارت، عضوهای تازه‌تأییدشده را از دیتابیس در حافظه می‌گذارد.
    فقط «عضو»ها را باور می‌کنیم؛ «غیرعضو» همیشه زنده از تلگرام چک می‌شود (ممکن است
    در زمان خاموشی ربات عضو شده باشد و نباید به اشتباه اخطار بگیرد).
    """
    global _cache_loaded
    since = datetime.fromtimestamp(time.time() - MEMBER_CACHE_TTL, timezone.utc).isoformat()
    rows = await asyncio.to_thread(_members_load_sync, since)
    now = time.time()
    loaded = 0
    for user_id, checked_at in rows:
        expiry = _parse_ts(checked_at) + MEMBER_CACHE_TTL
        # اگر در همین پروسه خبر تازه‌تری (رویداد یا چک زنده) داریم، دیتابیس را نادیده بگیر
        if expiry > now and user_id not in _fresh_at and _member_cache.get(user_id, 0) < expiry:
            _member_cache[user_id] = expiry
            loaded += 1
    _cache_loaded = True
    logger.info(f"[group_tools] {loaded} عضو تأییدشده از دیتابیس بارگذاری شد")


async def flush_members() -> None:
    global _member_buf
    if not _member_buf:
        return
    rows, _member_buf = list(_member_buf.values()), {}
    try:
        await asyncio.to_thread(_members_write_sync, rows)
    except Exception as e:                           # گروه نباید به خاطر دیتابیس خراب شود
        _log_throttled(f"[group_tools] ذخیره‌ی جدول اعضا ناموفق (بعداً دوباره تلاش می‌شود): {e}", key="members_write")
        for r in rows:
            _member_buf.setdefault(r["user_id"], r)


def enforcement_active() -> bool:
    """اجبار فقط در حالت enforce و بعد از پایان دوره‌ی (اختیاریِ) گرم‌کردن فعال است."""
    return FORCE_JOIN_MODE == "enforce" and (time.time() - _started_at) >= FORCE_JOIN_WARMUP


async def flush_buffers() -> None:
    """بافر حافظه را (بدون بلاک کردن event loop) در SQLite می‌نویسد."""
    global _buf_counts, _buf_names
    if not _buf_counts and not _buf_names:
        return
    counts, names = _buf_counts, _buf_names
    _buf_counts, _buf_names = {}, {}
    try:
        await asyncio.to_thread(_flush_sync, counts, names)
    except Exception as e:
        logger.error(f"[group_tools] flush failed, will retry: {e}")
        for k, n in counts.items():          # برگرداندن به بافر تا از دست نرود
            _buf_counts[k] = _buf_counts.get(k, 0) + n
        for k, v in names.items():
            _buf_names.setdefault(k, v)


def _build_stats_text(rows: List[Tuple[int, int, str, int]], days: int, premium: bool = True) -> str:
    prem = pe.active(premium)
    now = _now_tehran()
    jy, jm, jd = gregorian_to_jalali(now.year, now.month, now.day)
    date_str = fa_digits(f"{jd:02d} {_FA_MONTHS[jm - 1]} {jy}")
    time_str = fa_digits(now.strftime("%H:%M:%S"))

    # تعداد روز با ارقام طلایی؛ بدون پریمیوم: عدد فارسی ساده
    title_day = "امروز" if days == 1 else f"{pe.digits(days, 'gold', prem, persian=True)} روز گذشته"
    title = f" فعالیت  {title_day}"
    header = (f"{pe.e('flower_l')} <b>{title}</b> {pe.e('flower_r')}" if prem else f"➖ <b>{title}</b>")

    head = [
        header,
        "",
        f"• {_FA_WEEKDAYS[now.weekday()]}: {date_str}",
        f"• ساعت : {time_str}",
        "",
        "• فعال ترین اعضا به ترتیب:",
    ]
    text = "\n".join(head) + "\n"
    entities = pe.entity_count(text)
    
    for i, (user_id, total, name, is_channel) in enumerate(rows, 1):
        safe = html.escape((name or "کاربر").strip()[:28])
        label = f"<b>{safe}</b>" if is_channel else f"<a href='tg://user?id={user_id}'>{safe}</a>"
        
        rank = pe.digits(i, "gold" if i <= 3 else "silver", prem) if i <= 10 else str(i)
        
        # تغییر ۲: اضافه کردن فاصله (\n) اضافه برای فاصله‌گذاری بین اعضا
        line = f"\nنفر {rank} {label} با {total} پیام\n"
        
        line_entities = pe.entity_count(line)
        if len(text) + len(line) > 3900 or entities + line_entities > 95:   # سقف ۴۰۹۶ کاراکتر و ۱۰۰ entity تلگرام
            break
        text += line
        entities += line_entities
        
    return text.strip()


async def build_stats_for_chat(chat_id: int, days: int = STATS_WINDOW_DAYS, premium: bool = True) -> Optional[str]:
    await flush_buffers()
    since = (_now_tehran() - timedelta(days=days - 1)).strftime("%Y-%m-%d")
    rows = await asyncio.to_thread(_query_top_sync, chat_id, since, STATS_TOP_N)
    if not rows:
        return None
    return _build_stats_text(rows, days, premium)


_no_premium_chats: Set[int] = set()   # چت‌هایی که ایموجی پریمیوم را نپذیرفته‌اند (تا ری‌استارت)


async def _deliver_stats(send, chat_id: int, days: int) -> bool:
    """
    آمار را می‌سازد و می‌فرستد. اگر تلگرام ایموجی پریمیوم را نپذیرفت، همان آمار
    با ایموجی معمولی دوباره ارسال می‌شود. send: تابعی که متن را می‌گیرد و می‌فرستد.
    """
    prem = pe.active() and chat_id not in _no_premium_chats
    text = await build_stats_for_chat(chat_id, days, premium=prem)
    if not text:
        return False
    try:
        await send(text)
    except TelegramBadRequest as e:
        if not prem:
            raise
        logger.warning(f"[group_tools] ایموجی پریمیوم در {chat_id} پذیرفته نشد ({e}) → ارسال با ایموجی معمولی")
        text = await build_stats_for_chat(chat_id, days, premium=False)
        if text:
            await send(text)
            _no_premium_chats.add(chat_id)   # فقط وقتی نسخه‌ی معمولی رسید یعنی مشکل از ایموجی بوده
    return True


_admin_cache: Dict[Tuple[int, int], Tuple[float, bool]] = {}


async def _is_group_admin(bot: Bot, chat_id: int, user_id: int) -> bool:
    key = (chat_id, user_id)
    hit = _admin_cache.get(key)
    if hit and hit[0] > time.time():
        return hit[1]
    try:
        cm = await bot.get_chat_member(chat_id, user_id)
        status = getattr(cm.status, "value", cm.status)
        result = status in ("creator", "administrator")
    except Exception as e:
        logger.warning(f"[group_tools] admin check failed: {e}")
        return False
    _admin_cache[key] = (time.time() + ADMIN_CACHE_TTL, result)
    return result


@group_router.message(Command("stats"))
@group_router.message(F.text.regexp(r"^(آمار|امار)\s*(\d+)?$"))
async def handle_stats_command(message: types.Message):
    chat = message.chat
    if chat.type not in ("group", "supergroup") or not _chat_allowed(chat.id):
        return
    # ادمین ناشناس (sender_chat == خود گروه) یا ادمین عادی
    is_admin = (message.sender_chat is not None and message.sender_chat.id == chat.id) or (
        message.from_user is not None and await _is_group_admin(message.bot, chat.id, message.from_user.id)
    )
    if not is_admin:
        return  # Silent
        
    days = STATS_WINDOW_DAYS
    if message.text:
        text_clean = message.text.strip()
        if text_clean.startswith("/stats"):
            parts = text_clean.split()
            if len(parts) > 1 and parts[1].isdigit():
                days = int(parts[1])
        else:
            match = re.match(r"^(?:آمار|امار)\s*(\d+)?$", text_clean)
            if match and match.group(1):
                days = int(match.group(1))

    await _deliver_stats(
        lambda t: message.answer(t, parse_mode="HTML", disable_web_page_preview=True), chat.id, days)


@group_router.message(Command("emojitest"))
async def handle_emoji_test(message: types.Message):
    """همه‌ی ایموجی‌های پریمیوم را نشان می‌دهد تا شناسه‌ها را چشمی تأیید کنید (در گروه فقط ادمین‌ها)."""
    chat = message.chat
    if chat.type in ("group", "supergroup"):
        is_admin = (message.sender_chat is not None and message.sender_chat.id == chat.id) or (
            message.from_user is not None and await _is_group_admin(message.bot, chat.id, message.from_user.id)
        )
        if not is_admin:
            return
    try:
        await message.answer(pe.test_text(), parse_mode="HTML")
    except TelegramBadRequest as e:
        await message.answer(f"❌ تلگرام ایموجی پریمیوم را نپذیرفت:\n<code>{html.escape(str(e))}</code>", parse_mode="HTML")


@group_router.message(Command("id"))
async def handle_chat_id_command(message: types.Message):
    """آیدی عددی گروه را نشان می‌دهد (برای پر کردن LISTING_CHAT_ID). فقط ادمین‌ها."""
    chat = message.chat
    if chat.type not in ("group", "supergroup"):
        return
    is_admin = (message.sender_chat is not None and message.sender_chat.id == chat.id) or (
        message.from_user is not None and await _is_group_admin(message.bot, chat.id, message.from_user.id)
    )
    if is_admin:
        await message.answer(f"🆔 آیدی این گروه:\n<code>{chat.id}</code>", parse_mode="HTML")


async def _maybe_auto_post(bot: Bot) -> None:
    if not STATS_AUTO_TIME:
        return
    try:
        hh, mm = (int(x) for x in STATS_AUTO_TIME.split(":"))
    except ValueError:
        return
    now = _now_tehran()
    if (now.hour, now.minute) < (hh, mm):
        return
    today = now.strftime("%Y-%m-%d")
    if await asyncio.to_thread(_meta_get_sync, "last_auto_post") == today:
        return
    await asyncio.to_thread(_meta_set_sync, "last_auto_post", today)   # اول ثبت می‌کنیم تا دوبار نرود

    await flush_buffers()
    since = (now - timedelta(days=STATS_WINDOW_DAYS - 1)).strftime("%Y-%m-%d")
    for chat_id in await asyncio.to_thread(_active_chats_sync, since):
        if not _chat_allowed(chat_id):
            continue
        try:
            await _deliver_stats(
                lambda t, c=chat_id: bot.send_message(c, t, parse_mode="HTML", disable_web_page_preview=True),
                chat_id, STATS_WINDOW_DAYS)
        except Exception as e:
            logger.warning(f"[group_tools] auto-post to {chat_id} failed: {e}")


# ===========================================================
# 2) عضویت اجباری کانال
# ===========================================================
class JoinCheckCallback(CallbackData, prefix="fj"):
    user_id: int


_member_cache: Dict[int, float] = {}                 # user_id -> زمان انقضای تأیید (فقط عضوها)
_fresh_at: Dict[int, float] = {}                     # آخرین لحظه‌ای که از منبع معتبر (رویداد/چک زنده) خبر داریم
_exempt_cache: Dict[Tuple[int, int], float] = {}     # (chat_id, user_id) -> expiry (ادمین‌ها)
_warn_ts: Dict[Tuple[int, int], float] = {}
_bg_tasks: Set[asyncio.Task] = set()
_last_log: Dict[str, float] = {}


def _log_throttled(msg: str, key: str = "default", every: int = 300) -> None:
    """هر نوع خطا حداکثر هر ۵ دقیقه یک بار در لاگ می‌آید."""
    if time.time() - _last_log.get(key, 0) > every:
        _last_log[key] = time.time()
        logger.error(msg)


def _status_is_member(cm) -> bool:
    status = getattr(cm.status, "value", cm.status)
    return status in ("creator", "administrator", "member") or (
        status == "restricted" and bool(getattr(cm, "is_member", False))
    )


def _is_force_channel(chat) -> bool:
    """آیا این چت همان کانال عضویت اجباری است؟"""
    if _force_channel_id is not None and chat.id == _force_channel_id:
        return True
    if isinstance(FORCE_CHANNEL, int):
        return chat.id == FORCE_CHANNEL
    return (chat.username or "").lower() == FORCE_CHANNEL.lstrip("@").lower()


@group_router.chat_member()
async def on_channel_member_update(event: types.ChatMemberUpdated):
    """
    تلگرام خودش هر بار کسی وارد/خارج کانال شود این رویداد را می‌‌فرستد (بدون هیچ فشار اضافه).
    ربات باید ادمین کانال باشد و "chat_member" در allowed_updates باشد.
    رویداد معتبرترین منبع است: هم حافظه و هم دیتابیس را فوراً به‌روز می‌کند.
    """
    if not FORCE_JOIN_ENABLED or not _is_force_channel(event.chat):
        return
    member_user = event.new_chat_member.user
    if member_user.is_bot:
        return
    user_id = member_user.id
    is_member = _status_is_member(event.new_chat_member)
    now = time.time()
    _fresh_at[user_id] = now
    _remember_member(user_id, member_user.full_name, member_user.username, is_member, "event")
    if is_member:
        _member_cache[user_id] = now + MEMBER_CACHE_TTL
        logger.info(f"[group_tools] {user_id} عضو کانال شد")
    else:
        _member_cache.pop(user_id, None)
        _exempt_cache_clear_user(user_id)
        logger.info(f"[group_tools] {user_id} از کانال خارج شد")


def _exempt_cache_clear_user(user_id: int) -> None:
    for key in [k for k in _exempt_cache if k[1] == user_id]:
        _exempt_cache.pop(key, None)


async def is_channel_member(bot: Bot, user_id: int, use_cache: bool = True,
                            user: Optional[types.User] = None) -> Optional[bool]:
    """True/False، یا None اگر استعلام ممکن نبود (در این حالت کاربر را رد نمی‌‌کنیم)."""
    started = time.time()
    if use_cache and _member_cache.get(user_id, 0) > started:
        return True
    try:
        cm = await bot.get_chat_member(FORCE_CHANNEL, user_id)
    except Exception as e:
        _log_throttled(
            f"[group_tools] get_chat_member on {FORCE_CHANNEL} failed: {e} "
            f"(ربات باید ادمین کانال باشد؛ تا آن موقع محدودیت اعمال نمی‌شود)", key="chat_member")
        return None

    # اگر در حین همین استعلام، رویداد ورود/خروج رسیده، رویداد تازه‌تر است و جواب استعلام کهنه است
    if _fresh_at.get(user_id, 0) >= started:
        return _member_cache.get(user_id, 0) > time.time()

    is_member = _status_is_member(cm)
    now = time.time()
    _fresh_at[user_id] = now
    if user is not None:
        _remember_member(user_id, user.full_name, user.username, is_member, "check")
    if is_member:
        _member_cache[user_id] = now + MEMBER_CACHE_TTL
    else:
        _member_cache.pop(user_id, None)
    return is_member


def _spawn(coro) -> None:
    task = asyncio.create_task(coro)
    _bg_tasks.add(task)
    task.add_done_callback(_bg_tasks.discard)


async def _delete_later(msg: types.Message, delay: int) -> None:
    await asyncio.sleep(delay)
    try:
        await msg.delete()
    except Exception:
        pass


async def _enforce_join(message: types.Message, user: types.User) -> None:
    try:
        await message.delete()
    except Exception as e:
        _log_throttled(f"[group_tools] cannot delete message (ربات باید ادمین با دسترسی حذف پیام باشد): {e}", key="delete")

    key = (message.chat.id, user.id)
    now = time.time()
    if now - _warn_ts.get(key, 0) < WARN_COOLDOWN:
        return
    _warn_ts[key] = now

    chat_id = message.chat.id
    prem = pe.active() and chat_id not in _no_premium_chats
    try:
        sent = await message.answer(**_warning_kwargs(user, prem))
    except TelegramBadRequest as e:
        if not prem:
            logger.warning(f"[group_tools] warning send failed: {e}")
            return
        logger.warning(f"[group_tools] ایموجی پریمیوم در {chat_id} پذیرفته نشد ({e}) → ارسال با ایموجی معمولی")
        try:
            sent = await message.answer(**_warning_kwargs(user, False))
        except Exception as e2:
            logger.warning(f"[group_tools] warning send failed: {e2}")
            return
        _no_premium_chats.add(chat_id)
    except Exception as e:
        logger.warning(f"[group_tools] warning send failed: {e}")
        return
    _spawn(_delete_later(sent, WARN_DELETE_AFTER))


def _warning_kwargs(user: types.User, premium: bool) -> dict:
    """متن و دکمه‌های هشدار عضویت اجباری (premium=False: همان ظاهر ساده‌ی قبلی)."""
    name = html.escape(user.full_name[:30])
    body = (
        f"جناب <a href='tg://user?id={user.id}'>{name}</a>، برای ارسال پیام در گروه "
        f"باید ابتدا در کانال رسمی ما عضو شوید.\n\n"
        f"بعد از عضویت روی دکمه «عضو شدم» بزنید."
    )
    callback = JoinCheckCallback(user_id=user.id).pack()
    if premium:
        text = f"{pe.e('flower_l')} <b>عضویت اجباری</b> {pe.e('flower_r')}\n\n{body}"
        buttons = [
            InlineKeyboardButton(text="عضویت در کانال", url=FORCE_CHANNEL_URL,
                                 icon_custom_emoji_id=pe.icon_id("arrow_btn")),
            InlineKeyboardButton(text="عضو شدم", callback_data=callback,
                                 icon_custom_emoji_id=pe.icon_id("lock")),
        ]
    else:
        text = body
        buttons = [
            InlineKeyboardButton(text="عضویت در کانال 📢", url=FORCE_CHANNEL_URL),
            InlineKeyboardButton(text="✅ عضو شدم", callback_data=callback),
        ]
    return {"text": text, "parse_mode": "HTML", "reply_markup": InlineKeyboardMarkup(inline_keyboard=[buttons])}


async def _passes_force_join(message: types.Message, user: types.User) -> bool:
    bot = message.bot
    chat_id = message.chat.id
    now = time.time()

    if _exempt_cache.get((chat_id, user.id), 0) > now:
        return True

    member = await is_channel_member(bot, user.id, user=user)   # هر نفر یک بار چک و در جدول ثبت می‌شود
    if member is not False:          # True یا None (خطا => fail-open)
        return True

    if not enforcement_active():     # دقیقه‌ی گرم‌کردن / حالت log: فقط ثبت، بدون حذف و هشدار
        return True

    if await _is_group_admin(bot, chat_id, user.id):
        _exempt_cache[(chat_id, user.id)] = now + ADMIN_CACHE_TTL
        return True

    await _enforce_join(message, user)
    return False


@group_router.callback_query(JoinCheckCallback.filter())
async def on_join_check(query: types.CallbackQuery, callback_data: JoinCheckCallback):
    if query.from_user.id != callback_data.user_id:
        await query.answer("این دکمه مخصوص شما نیست.", show_alert=True)
        return
    member = await is_channel_member(query.bot, query.from_user.id, use_cache=False, user=query.from_user)
    if member is False:
        await query.answer("هنوز عضو کانال نشده‌اید ❌", show_alert=True)
        return
    await query.answer("✅ عضویت شما تأیید شد، می‌توانید پیام بدهید.", show_alert=True)
    try:
        await query.message.delete()
    except Exception:
        pass


# ===========================================================
# Middleware: برای هر پیام گروه، اول عضویت چک می‌شود، بعد شمارش
# (Middleware است تا بقیه‌ی هندلرهای ربات مثل قیمت/چارت دست‌‌نخورده کار کنند)
# ===========================================================
def _is_service_message(message: types.Message) -> bool:
    ct = getattr(message.content_type, "value", message.content_type)
    return ct in _SERVICE_TYPES


async def _process_group_message(message: types.Message) -> bool:
    """False یعنی پیام مسدود شد و نباید به هندلرهای دیگر برسد."""
    chat = message.chat
    if chat.type not in ("group", "supergroup") or not _chat_allowed(chat.id):
        return True
    if _is_service_message(message):
        return True

    if message.sender_chat is not None:
        # پیام از طرف کانال یا ادمین ناشناس؛ شمارش می‌شود ولی محدودیتی ندارد
        sc = message.sender_chat
        _record(chat.id, sc.id, sc.title or "کانال", True)
        return True

    user = message.from_user
    if user is None or user.is_bot or user.id in _TELEGRAM_SERVICE_IDS:
        return True

    if FORCE_JOIN_ENABLED and not await _passes_force_join(message, user):
        return False

    _record(chat.id, user.id, user.full_name or user.first_name or "کاربر", False)
    return True


class GroupGuardMiddleware(BaseMiddleware):
    async def __call__(self, handler, event: types.Message, data):
        try:
            allowed = await _process_group_message(event)
        except Exception as e:
            logger.exception(f"[group_tools] middleware error (fail-open): {e}")
            allowed = True
        if not allowed:
            return None
        return await handler(event, data)


# ===========================================================
# Public API برای main.py
# ===========================================================
def setup_group_tools(dp: Dispatcher) -> None:
    dp.message.outer_middleware(GroupGuardMiddleware())
    dp.include_router(group_router)


def init_group_tools() -> None:
    global _started_at
    _started_at = time.time()
    _get_conn()
    if FORCE_JOIN_ENABLED:
        logger.info(f"[group_tools] عضویت اجباری: حالت={FORCE_JOIN_MODE}، گرم‌کردن={FORCE_JOIN_WARMUP} ثانیه، "
                    f"اعتبار تأیید={MEMBER_CACHE_TTL} ثانیه، "
                    f"دیتابیس={'Supabase' if _use_supabase() else 'SQLite محلی'}")


async def _startup_selfcheck(bot: Bot) -> None:
    """یک بار بعد از استارت: آیا ربات واقعاً ادمین کانال است؟ (نبودنش را در لاگ بلند اعلام می‌کند)"""
    global _force_channel_id
    try:
        me = await bot.me()
        chat = await bot.get_chat(FORCE_CHANNEL)
        _force_channel_id = chat.id
        cm = await bot.get_chat_member(FORCE_CHANNEL, me.id)
        status = getattr(cm.status, "value", cm.status)
        if status in ("administrator", "creator"):
            logger.info(f"[group_tools] کانال «{chat.title}» ({chat.id}) درست است؛ ربات ادمین است")
        else:
            logger.error(f"[group_tools] ⚠️ ربات در کانال «{chat.title}» ادمین نیست (وضعیت: {status}). "
                         f"بدون ادمین بودن عضویت‌ها چک نمی‌‌شود و رویداد ورود/خروج نمی‌رسد.")
    except Exception as e:
        logger.error(f"[group_tools] ⚠️ بررسی کانال {FORCE_CHANNEL} ناموفق: {e}")


async def group_tools_background_loop(bot: Bot) -> None:
    """ذخیره‌ی دسته‌ای دیتا، بارگذاری کش عضویت، ارسال خودکار آمار و تمیزکاری."""
    global _selfchecked, _last_load_try, _enforce_announced
    last_prune = 0.0
    while True:
        try:
            if FORCE_JOIN_ENABLED:
                if not _selfchecked:
                    _selfchecked = True
                    await _startup_selfcheck(bot)
                if not _cache_loaded and time.time() - _last_load_try > 60:
                    _last_load_try = time.time()
                    try:
                        await load_member_cache()
                    except Exception as e:     # دیتابیس موقتاً در دسترس نیست؛ ۱ دقیقه بعد دوباره
                        _log_throttled(f"[group_tools] بارگذاری عضوها از دیتابیس ناموفق: {e}", key="members_load", every=60)

            await flush_buffers()
            await flush_members()
            await _maybe_auto_post(bot)

            if FORCE_JOIN_ENABLED and not _enforce_announced and enforcement_active():
                _enforce_announced = True
                logger.info("[group_tools] حالت اجبار عضویت فعال است")

            now = time.time()
            if now - last_prune > 3600:
                last_prune = now
                for store in (_member_cache, _exempt_cache):
                    for k in [k for k, exp in store.items() if exp < now]:
                        store.pop(k, None)
                for k in [k for k, exp in _admin_cache.items() if exp[0] < now]:
                    _admin_cache.pop(k, None)
                for k in [k for k, ts in _warn_ts.items() if now - ts > 3600]:
                    _warn_ts.pop(k, None)
                for store in (_fresh_at,):
                    for k in [k for k, ts in store.items() if now - ts > 3600]:
                        store.pop(k, None)
                for k in [k for k, v in _member_last.items() if now - v[1] > 3600]:
                    _member_last.pop(k, None)
                
                # نگهداری اطلاعات آمار دیتابیس تا ۴۰۰ روز (برای پشتیبانی از دستوراتی مثل امار 365)
                old = (_now_tehran() - timedelta(days=400)).strftime("%Y-%m-%d")
                await asyncio.to_thread(_prune_sync, old)
        except Exception as e:
            logger.warning(f"[group_tools] background loop error: {e}")
        await asyncio.sleep(FLUSH_INTERVAL)


async def shutdown_group_tools() -> None:
    await flush_buffers()
    await flush_members()
