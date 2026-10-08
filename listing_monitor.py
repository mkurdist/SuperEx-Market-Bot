# -*- coding: utf-8 -*-
"""
listing_monitor.py
---------------------------------------------------------
مانیتور مرکز اطلاعیه‌های SuperEx (نسخه‌ی فارسی) و ارسال خبرها به تلگرام.
نسخه نهایی ناهمگام (Async) با تفکیک کامل ۴ جدول، سیستم صفحه‌بندی (Pagination) 
و وضعیت‌سنجی ضد-تکرار (State Machine).

ویژگی‌های این نسخه:
    ۱) استفاده از ۴ جدول مجزا (seen_listings, seen_announcements, seen_events, seen_updates).
    ۲) دریافت تمام تاریخچه (ورق زدن اتوماتیک صفحات Zendesk تا اولین خبر).
    ۳) ثبت تاریخ واقعی انتشار (published_date) از سایت.
    ۴) ثبت خبر به صورت pending پیش از ارسال، و sent پس از اطمینان از ارسال.
    ۵) رفع قطعی باگ‌های Supabase (هدر User-Agent و ارور JSON خالی).
    ۶) سازگاری کامل با main.py بدون تداخل در Event Loop ها.
---------------------------------------------------------
"""

import argparse
import asyncio
import html
import logging
import os
import re
import signal
import sqlite3
import threading
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Set
from urllib.parse import urljoin, urlparse

import aiohttp
import premium_emoji as pe
from bs4 import BeautifulSoup

logger = logging.getLogger("listing_monitor")


# ===========================================================
# بخش‌های پایش‌شده + ظاهر پیام
# ===========================================================
@dataclass(frozen=True)
class Section:
    key: str        
    title: str      
    hashtag: str    
    url: str        
    emoji: str = ""  
    detailed: bool = False  


_BASE = "https://support.superex.com/hc/fa/sections/"
ALL_SECTIONS = (
    Section("listings", "لیست جدید", "#لیست_جدید", _BASE + "9117167570457", detailed=True),
    Section("announcements", "اطلاعیه", "#اطلاعیه", _BASE + "9117102988825"),
    Section("events", "رویداد", "#رویداد", _BASE + "9117136933657"),
    Section("updates", "به‌روزرسانی و نگهداری", "#آپدیت", _BASE + "9117198982041"),
)

BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/json;q=0.9,*/*;q=0.8",
}

HTTP_TIMEOUT = aiohttp.ClientTimeout(total=20)
STOP_FLAG = False


# ===========================================================
# خطاهای اختصاصی
# ===========================================================
class FetchError(Exception): pass
class ParseError(Exception): pass
class StorageError(Exception): pass
class TelegramError(Exception): pass


# ===========================================================
# تنظیمات
# ===========================================================
def _pick_sections(raw: str) -> tuple:
    wanted = {x.strip() for x in raw.split(",") if x.strip()}
    chosen = tuple(sec for sec in ALL_SECTIONS if not wanted or sec.key in wanted)
    return chosen or ALL_SECTIONS


@dataclass(frozen=True)
class Config:
    bot_token: str
    chat_ids: tuple  
    sections: tuple
    interval: int
    max_per_cycle: int
    preview: bool
    supabase_url: str
    supabase_key: str
    sqlite_path: str

    @classmethod
    def from_env(cls) -> "Config":
        try:
            from dotenv import load_dotenv
            load_dotenv()
        except ImportError:
            pass
        targets = []
        for part in os.getenv("LISTING_CHAT_ID", "").split(","):
            part = part.strip()
            if not part: continue
            chat, _, thread = part.partition(":")
            targets.append((chat.strip(), int(thread) if thread.strip().isdigit() else None))
        return cls(
            bot_token=os.getenv("BOT_TOKEN", "").strip(),
            chat_ids=tuple(targets),
            sections=_pick_sections(os.getenv("LISTING_SECTIONS", "")),
            interval=max(60, int(os.getenv("LISTING_CHECK_INTERVAL", "600"))),
            max_per_cycle=max(1, int(os.getenv("LISTING_MAX_PER_CYCLE", "5"))),
            preview=os.getenv("LISTING_PREVIEW", "0").strip() == "1",
            supabase_url=os.getenv("SUPABASE_URL", "").strip().rstrip("/"),
            supabase_key=os.getenv("SUPABASE_KEY", "").strip(),
            sqlite_path=os.getenv("LISTING_SQLITE_PATH", "listing_seen.sqlite3").strip(),
        )

    def validate(self, need_telegram: bool = True) -> None:
        missing = []
        if need_telegram and not self.bot_token: missing.append("BOT_TOKEN")
        if need_telegram and not self.chat_ids: missing.append("LISTING_CHAT_ID")
        if missing: raise SystemExit(f"متغیرهای محیطی لازم تنظیم نشده‌اند: {', '.join(missing)}")


@dataclass(frozen=True)
class Listing:
    article_id: int
    title: str
    url: str
    section_key: str  
    date_str: str = "" 


# ===========================================================
# توابع تاریخ شمسی
# ===========================================================
def gregorian_to_jalali(gy: int, gm: int, gd: int) -> tuple:
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

def parse_and_format_date(date_str: str) -> str:
    if not date_str: return ""
    try:
        date_part = date_str.split("T")[0]
        y, m, d = map(int, date_part.split("-"))
        jy, jm, jd = gregorian_to_jalali(y, m, d)
        return f"{jy:04d}/{jm:02d}/{jd:02d}"
    except Exception:
        return str(date_str).split("T")[0]


# ===========================================================
# بخش ۱: دریافت و پارس کردن خبرها (با پشتیبانی از صفحه‌بندی کامل)
# ===========================================================
_ARTICLE_ID_RE = re.compile(r"/articles/(\d+)")

def parse_listings_html(html_content: str, section: Section) -> List[Listing]:
    soup = BeautifulSoup(html_content, "html.parser")
    container = soup.select_one("ul.article-list") or soup.select_one("main") or soup

    listings: List[Listing] = []
    seen_ids: Set[int] = set()
    for a in container.find_all("a", href=True):
        match = _ARTICLE_ID_RE.search(a["href"])
        title = a.get_text(" ", strip=True)
        if not match or not title: continue
        
        article_id = int(match.group(1))
        if article_id in seen_ids: continue
        seen_ids.add(article_id)
        
        date_str = ""
        li = a.find_parent("li")
        if li:
            time_tag = li.find("time")
            if time_tag and time_tag.has_attr("datetime"):
                date_str = parse_and_format_date(time_tag["datetime"])

        full = urljoin(section.url, a["href"])
        clean_url = full.split("#")[0].split("?")[0]
        listings.append(Listing(article_id, title, clean_url, section.key, date_str))

    if not listings: raise ParseError("هیچ مقاله‌ای در صفحه HTML پیدا نشد")
    return listings

def _zendesk_api_url(source_url: str) -> Optional[str]:
    m = re.search(r"/hc/([^/]+)/sections/(\d+)", source_url)
    if not m: return None
    parsed = urlparse(source_url)
    # دریافت ۱۰۰ رکورد در هر صفحه برای تسریع فرایند کشیدن تاریخچه کامل
    return (f"{parsed.scheme}://{parsed.netloc}/api/v2/help_center/{m.group(1)}/sections/{m.group(2)}"
            f"/articles.json?sort_by=created_at&sort_order=desc&per_page=100")

async def _fetch_with_retry(session: aiohttp.ClientSession, url: str, is_json=True):
    for attempt in range(1, 4):
        try:
            async with session.get(url, timeout=HTTP_TIMEOUT) as resp:
                if resp.status == 200:
                    return await resp.json() if is_json else await resp.text()
                if resp.status in (429, 500, 502, 503, 504):
                    await asyncio.sleep(1.5 ** attempt)
                    continue
                resp.raise_for_status()
        except Exception as e:
            if attempt == 3: raise FetchError(f"HTTP fetch failed: {e}")
            await asyncio.sleep(1.5 ** attempt)

async def _fetch_via_api(session: aiohttp.ClientSession, section: Section) -> List[Listing]:
    api_url = _zendesk_api_url(section.url)
    if not api_url: raise FetchError("آدرس API قابل ساخت نیست")
    
    listings = []
    next_url = api_url
    
    # حلقه دریافت تمامی صفحات تاریخچه (Pagination)
    while next_url:
        data = await _fetch_with_retry(session, next_url, is_json=True)
        articles = data.get("articles", [])
        
        for a in articles:
            if a.get("id") and a.get("title") and a.get("html_url"):
                dt = parse_and_format_date(str(a.get("created_at", "")))
                listings.append(Listing(
                    int(a["id"]), str(a["title"]).strip(), str(a["html_url"]).split("?")[0], section.key, dt
                ))
        
        next_url = data.get("next_page")
        if next_url:
            await asyncio.sleep(0.5)  # وقفه کوتاه برای جلوگیری از فشار به سرور Zendesk
            
    if not listings: raise ParseError("API مقاله‌ای برنگرداند")
    return listings

async def _fetch_via_html(session: aiohttp.ClientSession, section: Section) -> List[Listing]:
    html_text = await _fetch_with_retry(session, section.url, is_json=False)
    return parse_listings_html(html_text, section)

async def fetch_listings(session: aiohttp.ClientSession, section: Section) -> List[Listing]:
    try:
        return await _fetch_via_api(session, section)
    except Exception as api_error:
        logger.warning(f"[{section.key}] API ناموفق ({type(api_error).__name__}) → تلاش با HTML")
        try:
            return await _fetch_via_html(session, section)
        except Exception as html_error:
            raise FetchError(f"API و HTML هر دو ناموفق بودند.") from html_error


# ===========================================================
# بخش ۲: ذخیره‌سازی ۴ جدولی
# ===========================================================
class Storage(ABC):
    @abstractmethod
    async def is_section_empty(self, section_key: str) -> bool: ...
    @abstractmethod
    async def get_status(self, section_key: str, ids: Iterable[int]) -> Dict[int, str]: ...
    @abstractmethod
    async def upsert_status(self, section_key: str, listings: Iterable[Listing], status: str) -> None: ...


class SupabaseStorage(Storage):
    def __init__(self, session: aiohttp.ClientSession, url: str, key: str):
        self.url = url
        self.session = session
        # User-Agent غیر مرورگر برای دور زدن محدودیت‌های امنیتی Supabase
        self.headers = {
            "apikey": key, 
            "Content-Type": "application/json",
            "User-Agent": "SuperExBot/1.0"
        }
        if key.startswith("eyJ"):
            self.headers["Authorization"] = f"Bearer {key}"

    async def _request(self, table_name: str, method: str, params=None, json=None, prefer=None):
        base_url = f"{self.url}/rest/v1/{table_name}"
        headers = dict(self.headers)
        if prefer: headers["Prefer"] = prefer
        try:
            async with self.session.request(method, base_url, params=params, json=json, headers=headers, timeout=HTTP_TIMEOUT) as resp:
                if resp.status >= 300:
                    text = await resp.text()
                    raise StorageError(f"Supabase HTTP {resp.status} on {table_name}: {text[:200]}")
                
                # رفع باگ بادی خالی (Supabase 204 No Content یا کد 200 خالی)
                if resp.status == 204:
                    return None
                    
                text = await resp.text()
                if not text.strip():
                    return None
                    
                return await resp.json()
        except Exception as e:
            raise StorageError(f"خطای ارتباط با Supabase ({table_name}): {e}")

    async def is_section_empty(self, section_key: str) -> bool:
        table_name = f"seen_{section_key}"
        data = await self._request(table_name, "GET", params={"select": "article_id", "limit": "1"})
        return not data or len(data) == 0

    async def get_status(self, section_key: str, ids: Iterable[int]) -> Dict[int, str]:
        table_name = f"seen_{section_key}"
        id_list = ",".join(str(i) for i in ids)
        if not id_list: return {}
        data = await self._request(table_name, "GET", params={"select": "article_id,status", "article_id": f"in.({id_list})"})
        if not data: return {}
        return {int(r["article_id"]): r.get("status", "sent") for r in data}

    async def upsert_status(self, section_key: str, listings: Iterable[Listing], status: str) -> None:
        table_name = f"seen_{section_key}"
        # ارسال ردیف‌ها به صورت دسته‌های ۱۰۰ تایی برای جلوگیری از فشار به API در زمان ساخت خط پایه (مهم برای صدها رکورد)
        rows = [{"article_id": l.article_id, "title": l.title, "url": l.url, "published_date": l.date_str, "status": status} for l in listings]
        if not rows: return
        
        for i in range(0, len(rows), 100):
            batch = rows[i:i + 100]
            await self._request(table_name, "POST", params={"on_conflict": "article_id"}, json=batch, prefer="resolution=merge-duplicates,return=minimal")


class SqliteStorage(Storage):
    def __init__(self, path: str):
        self.lock = threading.Lock()
        try:
            self.conn = sqlite3.connect(path, check_same_thread=False)
            for sec in ("listings", "announcements", "events", "updates"):
                self.conn.execute(f"CREATE TABLE IF NOT EXISTS seen_{sec} (article_id INTEGER PRIMARY KEY, title TEXT, url TEXT, published_date TEXT, status TEXT DEFAULT 'sent', created_at TEXT DEFAULT CURRENT_TIMESTAMP)")
            self.conn.commit()
        except sqlite3.Error as e:
            raise StorageError(f"باز کردن SQLite ناموفق: {e}")

    async def is_section_empty(self, section_key: str) -> bool:
        def _check():
            with self.lock: return self.conn.execute(f"SELECT 1 FROM seen_{section_key} LIMIT 1").fetchone() is None
        return await asyncio.to_thread(_check)

    async def get_status(self, section_key: str, ids: Iterable[int]) -> Dict[int, str]:
        def _get():
            id_list = list(ids)
            if not id_list: return {}
            marks = ",".join("?" * len(id_list))
            with self.lock: return {r[0]: r[1] for r in self.conn.execute(f"SELECT article_id, status FROM seen_{section_key} WHERE article_id IN ({marks})", id_list).fetchall()}
        return await asyncio.to_thread(_get)

    async def upsert_status(self, section_key: str, listings: Iterable[Listing], status: str) -> None:
        def _upsert():
            with self.lock:
                self.conn.executemany(
                    f"INSERT INTO seen_{section_key} (article_id, title, url, published_date, status) VALUES (?,?,?,?,?) "
                    "ON CONFLICT(article_id) DO UPDATE SET status = excluded.status",
                    [(l.article_id, l.title, l.url, l.date_str, status) for l in listings])
                self.conn.commit()
        await asyncio.to_thread(_upsert)


def build_storage(cfg: Config, session: aiohttp.ClientSession) -> Storage:
    if cfg.supabase_url and cfg.supabase_key:
        return SupabaseStorage(session, cfg.supabase_url, cfg.supabase_key)
    logger.warning("SUPABASE تنظیم نشده؛ از SQLite محلی استفاده می‌شود")
    return SqliteStorage(cfg.sqlite_path)


# ===========================================================
# بخش ۳: ساخت پیام
# ===========================================================
def detect_categories(title: str) -> str:
    cats = []
    if "اسپات" in title: cats.append("اسپات")
    if any(w in title for w in ("فیوچرز", "آتی", "پرپچوال", "دائمی", "USDT-M")): cats.append("فیوچرز دائمی")
    if "index futures" in title.lower(): cats.append("مارجین Index Futures")
    return "، ".join(cats)

def detect_symbols(title: str) -> List[str]:
    found = []
    for pattern in (r"\(([A-Z0-9.\-]{2,12})\)", r"(?<![A-Za-z0-9])([A-Z0-9]{2,12}USDT)(?![A-Za-z0-9])"):
        for sym in re.findall(pattern, title):
            if sym not in found: found.append(sym)
    return found[:6]

def format_message(listing: Listing, section: Section, html_mode: bool = True, premium: bool = True) -> str:
    prem = pe.active(premium and html_mode)
    if html_mode:
        esc = html.escape
        head = f"{section.emoji} <b>{section.title}</b>" if section.emoji else (f"{pe.e('flower_l')} <b>{section.title}</b> {pe.e('flower_r')}" if prem else f"<b>{section.title}</b>")
        link = f'{pe.e("arrow_link", prem)} <a href="{html.escape(listing.url, quote=True)}">مشاهده‌ی جزئیات</a>'
        sym_fmt, date_fmt = lambda x: f"<code>{html.escape(x)}</code>", lambda x: f"{pe.e('date', prem)} <b>تاریخ:</b> {x}"
    else:
        esc = lambda x: x
        plain_emoji = re.sub(r"<[^>]+>", "", section.emoji)
        head = f"{plain_emoji} {section.title}".strip()
        link = f"{pe.PLAIN['arrow_link']} مشاهده‌ی جزئیات:\n{listing.url}"
        sym_fmt, date_fmt = lambda x: x, lambda x: f"{pe.PLAIN['date']} تاریخ: {x}"

    blocks = [head, f"{pe.e('bell', prem)} \u200F{esc(listing.title)}"]
    details = []
    if listing.date_str: details.append(date_fmt(listing.date_str))

    if section.detailed:
        category = detect_categories(listing.title)
        if category: details.append(f"دسته‌بندی: {category}")
        symbols = detect_symbols(listing.title)
        if symbols: details.append(f"\u200Fنماد: " + "، ".join(sym_fmt(x) for x in symbols))

    if details: blocks.append("\n".join(details))
    blocks += [link, pe.hashtag(section.hashtag, prem)]
    return "\n\n".join(blocks)


# ===========================================================
# بخش ۴: ارسال به تلگرام
# ===========================================================
class TelegramSender:
    MAX_ATTEMPTS = 4

    def __init__(self, cfg: Config, session: aiohttp.ClientSession):
        self.cfg = cfg
        self.session = session
        self.url = f"https://api.telegram.org/bot{cfg.bot_token}/sendMessage"
        self._no_premium: Set[str] = set()

    def _safe(self, text: str) -> str:
        return str(text).replace(self.cfg.bot_token, "***") if self.cfg.bot_token else str(text)

    async def send(self, listing: Listing, section: Section) -> None:
        ok = 0
        last_error = None
        for chat_id, thread_id in self.cfg.chat_ids:
            try:
                await self._send_to(listing, section, chat_id, thread_id)
                ok += 1
            except TelegramError as e:
                logger.error(f"ارسال به {chat_id} ناموفق: {e}")
                last_error = e
        if ok == 0:
            raise last_error or TelegramError("هیچ مقصدی تنظیم نشده")

    async def _send_to(self, listing: Listing, section: Section, chat_id: str, thread_id: Optional[int]) -> None:
        mode = "premium" if (pe.ENABLED and chat_id not in self._no_premium) else "html"
        downgraded = False

        payload = {"chat_id": chat_id, "text": format_message(listing, section, html_mode=(mode != "plain"), premium=(mode == "premium")), "parse_mode": "HTML", "disable_web_page_preview": not self.cfg.preview}
        if thread_id: payload["message_thread_id"] = thread_id

        for attempt in range(1, self.MAX_ATTEMPTS + 1):
            if STOP_FLAG: raise TelegramError("پروسه قطع شد")
            try:
                async with self.session.post(self.url, json=payload, timeout=HTTP_TIMEOUT) as resp:
                    if resp.status == 200:
                        if downgraded and mode != "premium": self._no_premium.add(chat_id)
                        return
                    
                    data = await resp.json() if resp.headers.get("content-type") == "application/json" else {}
                    desc = str(data.get("description", await resp.text()))

                    if resp.status == 429:
                        wait = int(data.get("parameters", {}).get("retry_after", 5)) + 1
                        await asyncio.sleep(min(wait, 60))
                        continue
                    if resp.status == 400 and mode == "premium":
                        downgraded, mode, payload["text"] = True, "html", format_message(listing, section, html_mode=True, premium=False)
                        continue
                    if resp.status == 400 and "parse entities" in desc.lower() and "parse_mode" in payload:
                        payload.pop("parse_mode")
                        mode, payload["text"] = "plain", format_message(listing, section, html_mode=False, premium=False)
                        continue
                    if resp.status >= 500:
                        await asyncio.sleep(2 ** attempt)
                        continue
                    raise TelegramError(f"HTTP {resp.status}: {self._safe(desc)}")
            except aiohttp.ClientError:
                await asyncio.sleep(2 ** attempt)
        raise TelegramError("ارسال بعد از چند تلاش ناموفق ماند")


# ===========================================================
# بخش ۵: حلقه‌ی اصلی
# ===========================================================
async def _process_section(section: Section, cfg: Config, http: aiohttp.ClientSession, storage: Storage, sender: TelegramSender) -> int:
    listings = await fetch_listings(http, section)
    statuses = await storage.get_status(section.key, [l.article_id for l in listings])

    # ثبت خط پایه (دریافت و ذخیره در سکوت کل تاریخچه سایت)
    if await storage.is_section_empty(section.key):
        await storage.upsert_status(section.key, listings, "sent")
        logger.info(f"[{section.key}] خط پایه ثبت شد ({len(listings)} خبر تاریخی ذخیره شد؛ چیزی ارسال نشد)")
        return 0

    new_listings = sorted([l for l in listings if statuses.get(l.article_id) != "sent"], key=lambda x: x.article_id)
    if not new_listings: return 0

    if len(new_listings) > cfg.max_per_cycle:
        skipped, new_listings = new_listings[:-cfg.max_per_cycle], new_listings[-cfg.max_per_cycle:]
        await storage.upsert_status(section.key, skipped, "sent")

    sent = 0
    for listing in new_listings:
        if STOP_FLAG: break
        
        # ۱. وضعیت Pending
        await storage.upsert_status(section.key, [listing], "pending")
        
        # ۲. تلاش برای ارسال
        try:
            await sender.send(listing, section)
        except TelegramError as e:
            logger.error(f"[{section.key}] ارسال «{listing.title[:60]}» ناموفق: {e} — دوره‌ی بعد دوباره تلاش می‌شود")
            break
            
        # ۳. ثبت موفقیت‌آمیز Sent
        await storage.upsert_status(section.key, [listing], "sent")
        sent += 1
        await asyncio.sleep(1.5)
        
    if sent > 0: logger.info(f"[{section.key}] {sent} پیام ارسال شد")
    return sent


async def run_once_async(cfg: Config) -> int:
    total = 0
    async with aiohttp.ClientSession(headers=BROWSER_HEADERS) as http:
        storage = build_storage(cfg, http)
        sender = TelegramSender(cfg, http)
        for section in cfg.sections:
            try:
                total += await _process_section(section, cfg, http, storage, sender)
            except (FetchError, ParseError) as e:
                logger.warning(f"[{section.key}] دریافت خبرها ناموفق: {e}")
            except StorageError as e:
                logger.error(f"[{section.key}] خطای دیتابیس در این بخش: {e}")
            except Exception as e:
                logger.error(f"[{section.key}] خطای غیرمنتظره: {e}")
    return total


async def run_forever_async(cfg: Config):
    cfg.validate()
    logger.info(f"مانیتور شروع شد؛ فاصله‌ی چک: {cfg.interval} ثانیه")
    
    while not STOP_FLAG:
        try:
            await run_once_async(cfg)
        except Exception:
            logger.exception("خطای پیش‌بینی‌نشده در حلقه اصلی؛ مانیتورینگ متوقف نمی‌شود.")
            
        for _ in range(cfg.interval):
            if STOP_FLAG: break
            await asyncio.sleep(1)


def _bg_runner(cfg: Optional[Config] = None):
    cfg = cfg or Config.from_env()
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(run_forever_async(cfg))
    finally:
        loop.close()


def start_in_background() -> threading.Thread:
    """اجرا به صورت پس‌زمینه بدون تداخل با Event Loop برنامه‌ی اصلی main.py"""
    thread = threading.Thread(target=_bg_runner, name="listing-monitor", daemon=True)
    thread.start()
    return thread

# ===========================================================
# CLI
# ===========================================================
def main():
    global STOP_FLAG
    parser = argparse.ArgumentParser()
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    
    def signal_handler(*_):
        global STOP_FLAG
        STOP_FLAG = True
    
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, signal_handler)

    if args.once:
        asyncio.run(run_once_async(Config.from_env()))
    else:
        _bg_runner()


if __name__ == "__main__":
    main()
