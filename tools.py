# -*- coding: utf-8 -*-
"""
tools.py
---------------------------------------------------------
ماژول ایزوله برای ۳ قابلیت جدید ربات:
    1) سرویس قیمت طلا، سکه و آبشده  (Gold & Coin Service)
    2) سرویس قیمت دلار و تتر        (Dollar & Tether Service)
    3) ماشین‌حساب تبدیل کریپتو       (Crypto Calculator)

این فایل کاملاً مستقل است و هیچ وابستگی‌ای به main.py ندارد.
تمامی هندلرها روی یک روتر واحد (tools_router) ثبت شده‌اند تا
با یک خط دستور در main.py قابل اتصال باشند:

    from tools import tools_router
    dp.include_router(tools_router)

نکته مهم درباره‌ی ترتیب پردازش پیام‌ها:
در main.py هندلر تشخیص نماد کریپتو (تیکر) روی خود dp ثبت شده و
زودتر از روترهای زیرمجموعه بررسی می‌شود. برای همین، در main.py
یک لیست سیاه (Blocklist) از کلمات رایج انگلیسی + کلیدواژه‌های این
ماژول (gold, dollar, usdt) اضافه شده تا پیام‌های مربوط به این
قابلیت‌ها هرگز به اشتباه توسط هندلر تیکر مصرف نشوند و به این روتر
برسند.

تمام خروجی‌های متنی این فایل با parse_mode="HTML" ارسال می‌شوند.
در گروه‌ها، در صورت خطا یا عدم موفقیت در دریافت داده، پیامی ارسال
نمی‌شود (Silent Fail) تا اسپم ایجاد نشود.
"""

import asyncio
import io
import logging
import re
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from typing import Optional, Dict, Any

import aiohttp
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

from aiogram import Router, types
from aiogram.types import BufferedInputFile

logger = logging.getLogger("tools")

tools_router = Router(name="tools_router")

# Executor مخصوص رندر چارت طلای جهانی - جدا از main.py تا این فایل کاملاً ایزوله بماند
_CHART_EXECUTOR = ThreadPoolExecutor(max_workers=2, thread_name_prefix="gold-chart")

# ===========================================================
# Shared HTTP / formatting helpers
# ===========================================================
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
}

REQUEST_TIMEOUT = aiohttp.ClientTimeout(total=6.0)


async def _fetch_json(
    url: str,
    headers: Optional[dict] = None,
    params: Optional[dict] = None,
) -> Optional[Any]:
    """درخواست GET امن با تایم‌اوت و مدیریت خطا - در صورت هر مشکلی None برمی‌گرداند."""
    try:
        async with aiohttp.ClientSession(timeout=REQUEST_TIMEOUT) as session:
            async with session.get(url, headers=headers or DEFAULT_HEADERS, params=params) as resp:
                if resp.status == 200:
                    return await resp.json(content_type=None)
                logger.warning(f"[tools] {url} -> HTTP {resp.status}")
    except Exception as e:
        logger.warning(f"[tools] fetch failed for {url}: {e}")
    return None


# tgju (و بعضی سرویس‌های دیگر) اعداد را به‌صورت رشته و با جداکننده هزارگان
# برمی‌گردانند (مثلاً "3,780,000") و گاهی با ارقام فارسی/عربی. float() مستقیم
# روی چنین رشته‌ای Exception می‌دهد - همین باعث می‌شد همه‌ی مقادیر N/A شوند.
# این تابع قبل از تبدیل، رشته را پاک‌سازی می‌کند.
_DIGIT_TRANS = str.maketrans(
    "۰۱۲۳۴۵۶۷۸۹" + "٠١٢٣٤٥٦٧٨٩",
    "01234567890123456789",
)


def _clean_numeric(raw: Any) -> Optional[float]:
    if raw is None:
        return None
    if isinstance(raw, (int, float)):
        return float(raw)
    text = str(raw).strip().translate(_DIGIT_TRANS)
    text = re.sub(r"[^\d.\-]", "", text)
    if not text or text in ("-", "."):
        return None
    try:
        return float(text)
    except ValueError:
        return None


def rial_to_toman_str(rial_value: Any) -> str:
    """تبدیل ریال به تومان + جداکننده سه رقمی."""
    val = _clean_numeric(rial_value)
    if val is None:
        return "N/A"
    return f"{val / 10:,.0f}"


def toman_str(toman_value: Any) -> str:
    """فرمت مقدار تومانی (بدون تبدیل) + جداکننده سه رقمی."""
    val = _clean_numeric(toman_value)
    if val is None:
        return "N/A"
    return f"{val:,.0f}"


def format_number(value: Any, decimals: int = 2) -> str:
    val = _clean_numeric(value)
    if val is None:
        return "N/A"
    return f"{val:,.{decimals}f}"


FOOTER = "\n<i>@SuperExFa_bot</i>"


# ===========================================================
# 1) Gold Service — فقط طلای ۱۸ عیار + انس جهانی + چارت طلای جهانی
# ===========================================================
GOLD_KEYWORDS = ["طلا", "سکه", "قیمت طلا", "قیمت سکه", "مظنه", "ابشده", "آبشده", "gold"]

TGJU_MIRRORS = [
    "https://call1.tgju.org/ajax.json",
    "https://call.tgju.org/ajax.json",
    "https://call5.tgju.org/ajax.json",
]

TGJU_HEADERS = {
    **DEFAULT_HEADERS,
    "Referer": "https://www.tgju.org/",
}

# کش کوتاه‌مدت مشترک بین سرویس طلا و دلار (هر دو از یک اندپوینت tgju می‌خوانند)
_TGJU_CACHE: Dict[str, Any] = {"ts": 0.0, "data": None}
_TGJU_CACHE_TTL = 5.0


async def fetch_tgju_data() -> Optional[dict]:
    """تلاش برای دریافت دیتای tgju از آدرس اصلی و در صورت خطا از آدرس‌های پشتیبان."""
    now = time.time()
    if _TGJU_CACHE["data"] and (now - _TGJU_CACHE["ts"]) < _TGJU_CACHE_TTL:
        return _TGJU_CACHE["data"]

    for url in TGJU_MIRRORS:
        data = await _fetch_json(url, headers=TGJU_HEADERS)
        if data and isinstance(data, dict) and data.get("current"):
            _TGJU_CACHE["data"] = data
            _TGJU_CACHE["ts"] = now
            return data
    return None


def is_gold_trigger(message: types.Message) -> bool:
    text = (message.text or "").strip()
    if not text:
        return False
    lowered = text.lower()
    return any(kw.lower() in lowered for kw in GOLD_KEYWORDS)


# -----------------------------------------------------------
# چارت طلای جهانی (XAU/USD)
# -----------------------------------------------------------
# tgju فقط قیمت لحظه‌ای می‌دهد، نه سری تاریخی، پس برای چارت از اندپوینت
# عمومی و رایگان یاهو فاینانس (بدون نیاز به کلید) روی نماد فیوچرز طلا
# (GC=F) استفاده شده است. اگر این سرویس در دسترس نبود، پیام متنی طلا
# همچنان (بدون چارت) ارسال می‌شود.
YAHOO_CHART_URL = "https://query1.finance.yahoo.com/v8/finance/chart/GC=F"

_GOLD_CHART_CACHE: Dict[str, Any] = {"ts": 0.0, "data": None}
_GOLD_CHART_CACHE_TTL = 60.0  # هر ۶۰ ثانیه یک‌بار رفرش


async def _fetch_gold_series(interval: str, range_: str):
    params = {"interval": interval, "range": range_}
    data = await _fetch_json(YAHOO_CHART_URL, headers=DEFAULT_HEADERS, params=params)
    if not data:
        return None
    try:
        result = data["chart"]["result"][0]
        timestamps = result["timestamp"]
        closes = result["indicators"]["quote"][0]["close"]
        points = [(t, c) for t, c in zip(timestamps, closes) if c is not None]
        return points if len(points) >= 2 else None
    except (KeyError, IndexError, TypeError):
        return None


async def fetch_world_gold_series():
    # اول بازه‌ی ۱ روزه با گام ۱۵ دقیقه؛ اگر بازار بسته بود (مثلاً آخر هفته) فال‌بک ۵ روزه
    points = await _fetch_gold_series("15m", "1d")
    if not points:
        points = await _fetch_gold_series("1h", "5d")
    return points


def _render_world_gold_chart_sync(points) -> bytes:
    times = [datetime.fromtimestamp(t, tz=timezone.utc) for t, _ in points]
    prices = [p for _, p in points]

    fig, ax = plt.subplots(figsize=(10, 5), facecolor="#000000")
    ax.set_facecolor("#000000")
    ax.plot(times, prices, color="#FFD700", linewidth=1.8)
    ax.fill_between(times, prices, min(prices), color="#FFD700", alpha=0.08)

    ax.set_title("XAU/USD | World Gold Ounce", color="#e6e6e6", fontsize=13, pad=10)
    ax.set_ylabel("Price (USD)", color="#cfcfcf")
    ax.tick_params(colors="#9a9a9a")
    for spine in ax.spines.values():
        spine.set_color("#555555")
    ax.grid(True, color="#222222", linestyle="--", linewidth=0.5)
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
    fig.autofmt_xdate(rotation=0)

    ax.text(
        0.5, 0.03, "created by @SuperExPrice_bot | @SuperexIR",
        transform=ax.transAxes, ha="center", va="bottom",
        fontsize=9, color="#9a9a9a",
    )

    buf = io.BytesIO()
    try:
        fig.tight_layout()
        fig.savefig(buf, dpi=130, facecolor=fig.get_facecolor(), bbox_inches="tight", pad_inches=0.15)
        return buf.getvalue()
    finally:
        buf.close()
        plt.close(fig)


async def generate_world_gold_chart() -> Optional[bytes]:
    now = time.time()
    if _GOLD_CHART_CACHE["data"] and (now - _GOLD_CHART_CACHE["ts"]) < _GOLD_CHART_CACHE_TTL:
        return _GOLD_CHART_CACHE["data"]

    points = await fetch_world_gold_series()
    if not points:
        return None

    loop = asyncio.get_running_loop()
    image_bytes = await loop.run_in_executor(_CHART_EXECUTOR, _render_world_gold_chart_sync, points)

    _GOLD_CHART_CACHE["data"] = image_bytes
    _GOLD_CHART_CACHE["ts"] = now
    return image_bytes


# -----------------------------------------------------------
# ماشین‌حساب طلا (وزن به گرم -> قیمت تومانی)
# -----------------------------------------------------------
# نمونه ورودی‌های معتبر: "10 گرم طلا"، "1.5 گرم طلا"
# مبنای محاسبه: قیمت هر گرم طلای ۱۸ عیار (geram18) که خودِ tgju همین‌طوری
# می‌دهد (قیمت به‌ازای هر گرم)، پس فقط کافیست در وزن ضرب شود.
#
# نکته‌ی مهم درباره‌ی ترتیب: چون این پیام‌ها خودشان شامل کلمه‌ی "طلا"
# هستند، اگر این هندلر بعد از هندلر عمومی طلا (handle_gold_price) ثبت
# می‌شد، هیچ‌وقت اجرا نمی‌شد (چون is_gold_trigger هم با آن‌ها match
# می‌کند و روتر با اولین match متوقف می‌شود). به همین دلیل این هندلر
# زودتر از هندلر عمومی طلا در فایل ثبت شده است.
GOLD_GRAM_REGEX = re.compile(r"^\s*(\d+(?:\.\d+)?)\s*گرم\s*طلا\s*$")


def is_gold_calc_trigger(message: types.Message) -> bool:
    text = (message.text or "").strip()
    return bool(GOLD_GRAM_REGEX.match(text))


@tools_router.message(is_gold_calc_trigger)
async def handle_gold_gram_calculator(message: types.Message):
    match = GOLD_GRAM_REGEX.match((message.text or "").strip())
    if not match:
        return

    try:
        grams = float(match.group(1))
    except ValueError:
        return
    if grams <= 0:
        return

    data = await fetch_tgju_data()
    geram18_val: Optional[float] = None
    if data:
        item = data.get("current", {}).get("geram18")
        geram18_val = _clean_numeric(item.get("p")) if item else None

    if geram18_val is None:
        if message.chat.type == "private":
            await message.reply(
                "⚠️ در حال حاضر امکان محاسبه قیمت طلا وجود ندارد.",
                parse_mode="HTML",
            )
        return  # Silent Fail در گروه

    price_per_gram_toman = geram18_val / 10
    total_toman = grams * price_per_gram_toman

    lines = [
        "🧮 <b>ماشین‌حساب طلا</b>\n",
        f"وزن: <b>{format_number(grams, 2)}</b> گرم (طلای ۱۸ عیار)",
        f"قیمت هر گرم: <b>{toman_str(price_per_gram_toman)}</b> تومان",
        f"\n💰 مجموع: <b>{toman_str(total_toman)}</b> تومان",
        FOOTER,
    ]
    await message.reply("\n".join(lines), parse_mode="HTML")


@tools_router.message(is_gold_trigger)
async def handle_gold_price(message: types.Message):
    data = await fetch_tgju_data()

    geram18_val: Optional[float] = None
    ons_val: Optional[float] = None

    if data:
        current = data.get("current", {})
        geram18_item = current.get("geram18")
        ons_item = current.get("ons")
        geram18_val = _clean_numeric(geram18_item.get("p")) if geram18_item else None
        ons_val = _clean_numeric(ons_item.get("p")) if ons_item else None

    if geram18_val is None and ons_val is None:
        if message.chat.type == "private":
            await message.reply(
                "⚠️ در حال حاضر امکان دریافت قیمت طلا وجود ندارد.",
                parse_mode="HTML",
            )
        return  # Silent Fail در گروه

    lines = ["🥇 <b>قیمت طلا</b>\n"]
    if geram18_val is not None:
        lines.append(f"• طلای ۱۸ عیار: <b>{geram18_val / 10:,.0f}</b> تومان")
    if ons_val is not None:
        lines.append(f"🌍 انس جهانی طلا: <b>{ons_val:,.2f}</b> دلار")
    lines.append(FOOTER)
    caption = "\n".join(lines)

    chart_bytes = None
    try:
        chart_bytes = await generate_world_gold_chart()
    except Exception as e:
        logger.warning(f"[tools] world gold chart failed: {e}")

    if chart_bytes:
        photo = BufferedInputFile(chart_bytes, filename="world_gold_chart.png")
        await message.reply_photo(photo=photo, caption=caption, parse_mode="HTML")
    else:
        await message.reply(caption, parse_mode="HTML")


# ===========================================================
# 2) Dollar & Tether Service
# ===========================================================
DOLLAR_KEYWORDS = ["دلار", "تتر", "usdt", "dollar", "قیمت دلار", "قیمت تتر", "نرخ دلار"]


async def fetch_tgju_dollar_toman() -> Optional[float]:
    data = await fetch_tgju_data()
    if not data:
        return None
    item = data.get("current", {}).get("price_dollar_rl")
    if not item:
        return None
    val = _clean_numeric(item.get("p"))
    if val is None:
        return None
    return val / 10  # ریال -> تومان


# نوبیتکس عمداً حذف شده: سرور Render قادر به resolve کردن دامنه‌ی
# api.nobitex.ir نیست ("Name or service not known" در لاگ‌ها) و همیشه fail
# می‌شد، پس دیگر در هیچ‌جای این فایل استفاده نمی‌شود.


async def fetch_bitpin_usdt() -> Optional[dict]:
    url = "https://api.bitpin.ir/v1/mkt/markets/"
    data = await _fetch_json(url)
    if not data:
        return None
    try:
        markets = data.get("results", []) if isinstance(data, dict) else data
        for m in markets:
            code = m.get("code") or f"{m.get('currency1')}_{m.get('currency2')}"
            if code == "USDT_IRT":
                price = float(m.get("price"))
                change = m.get("price_change_percent") or m.get("change")
                return {"price_toman": price, "change": change}
    except (TypeError, ValueError, AttributeError):
        return None
    return None


async def fetch_wallex_usdt() -> Optional[dict]:
    url = "https://api.wallex.ir/v1/markets"
    data = await _fetch_json(url)
    if not data:
        return None
    try:
        symbols = data.get("result", {}).get("symbols", {})
        item = symbols.get("USDTTMN")
        if not item:
            return None
        stats = item.get("stats", {})
        price = float(stats.get("lastPrice"))
        change = stats.get("dailyChangePercent") or stats.get("24h_ch")
        return {"price_toman": price, "change": change}
    except (TypeError, ValueError, AttributeError):
        return None


def _format_change(change: Any) -> str:
    val = _clean_numeric(change)
    if val is None:
        return ""
    arrow = "🔺" if val > 0 else ("🔻" if val < 0 else "▪️")
    return f" ({arrow}{abs(val):.2f}%)"


def is_dollar_trigger(message: types.Message) -> bool:
    text = (message.text or "").strip()
    if not text:
        return False
    lowered = text.lower()
    return any(kw.lower() in lowered for kw in DOLLAR_KEYWORDS)


@tools_router.message(is_dollar_trigger)
async def handle_dollar_price(message: types.Message):
    # نوبیتکس حذف شده (به دلیل عدم دسترسی سرور Render)؛ فقط بیت‌پین و والکس
    tgju_dollar, bitpin, wallex = await asyncio.gather(
        fetch_tgju_dollar_toman(),
        fetch_bitpin_usdt(),
        fetch_wallex_usdt(),
    )

    sources = [s for s in (bitpin, wallex) if s and _clean_numeric(s.get("price_toman")) is not None]

    if tgju_dollar is None and not sources:
        if message.chat.type == "private":
            await message.reply(
                "⚠️ در حال حاضر امکان دریافت نرخ دلار و تتر وجود ندارد.",
                parse_mode="HTML",
            )
        return  # Silent Fail در گروه

    lines = ["💵 <b>نرخ لحظه‌ای دلار و تتر</b>\n"]

    if tgju_dollar is not None:
        lines.append(f"🏦 دلار بازار آزاد: <b>{toman_str(tgju_dollar)}</b> تومان\n")

    if sources:
        avg_price = sum(_clean_numeric(s["price_toman"]) for s in sources) / len(sources)
        changes = [_clean_numeric(s.get("change")) for s in sources]
        changes = [c for c in changes if c is not None]
        avg_change_str = ""
        if changes:
            avg_change = sum(changes) / len(changes)
            avg_change_str = _format_change(avg_change)
        lines.append(f"💰 میانگین قیمت تتر: <b>{toman_str(avg_price)}</b> تومان{avg_change_str}")
    else:
        lines.append("💰 میانگین قیمت تتر: <i>در دسترس نیست</i>")

    lines.append(FOOTER)
    await message.reply("\n".join(lines), parse_mode="HTML")


# ===========================================================
# 3) Crypto Calculator
# ===========================================================
CALC_REGEX = re.compile(r"^\s*(\d+(?:\.\d+)?)\s*([a-zA-Z]{2,10})\s*$")

BINANCE_PRICE_URL = "https://data-api.binance.vision/api/v3/ticker/price"
SUPEREX_TICKER_URL = "https://api.superexchang.com/resource/v3/public/currency/new"


async def fetch_binance_usdt_price(symbol: str) -> Optional[float]:
    url = f"{BINANCE_PRICE_URL}?symbol={symbol.upper()}USDT"
    data = await _fetch_json(url)
    if data and isinstance(data, dict) and "price" in data:
        try:
            return float(data["price"])
        except (TypeError, ValueError):
            return None
    return None


def _get_superex_headers() -> dict:
    return {
        "accept": "*/*",
        "accept-language": "en",
        "client": "1",
        "nonce": uuid.uuid4().hex,
        "timestamp": str(int(time.time() * 1000)),
        "token": "",
        "content-type": "application/x-www-form-urlencoded",
    }


async def fetch_superex_usdt_price(symbol: str) -> Optional[float]:
    url = f"{SUPEREX_TICKER_URL}?currency={symbol.lower()}"
    data = await _fetch_json(url, headers=_get_superex_headers())
    if data:
        obj = data.get("data", {}) if isinstance(data, dict) else {}
        price = obj.get("newPrice")
        if price:
            try:
                return float(price)
            except (TypeError, ValueError):
                return None
    return None


async def get_avg_usdt_toman_rate() -> Optional[float]:
    # نوبیتکس حذف شده (به دلیل عدم دسترسی سرور Render)؛ فقط بیت‌پین و والکس
    bitpin, wallex = await asyncio.gather(fetch_bitpin_usdt(), fetch_wallex_usdt())
    rates = [
        _clean_numeric(r["price_toman"])
        for r in (bitpin, wallex)
        if r and _clean_numeric(r.get("price_toman")) is not None
    ]
    if not rates:
        return None
    return sum(rates) / len(rates)


def is_calc_trigger(message: types.Message) -> bool:
    text = message.text or ""
    return bool(CALC_REGEX.match(text.strip()))


@tools_router.message(is_calc_trigger)
async def handle_crypto_calculator(message: types.Message):
    match = CALC_REGEX.match((message.text or "").strip())
    if not match:
        return

    amount_str, symbol = match.group(1), match.group(2).upper()

    try:
        amount = float(amount_str)
    except ValueError:
        return
    if amount <= 0:
        return

    price_usd = await fetch_binance_usdt_price(symbol)
    if price_usd is None:
        price_usd = await fetch_superex_usdt_price(symbol)

    if price_usd is None:
        if message.chat.type == "private":
            await message.reply(f"⚠️ نماد <b>{symbol}</b> یافت نشد.", parse_mode="HTML")
        return  # Silent Fail در گروه

    toman_rate = await get_avg_usdt_toman_rate()
    usd_value = amount * price_usd
    amount_decimals = 8 if amount < 1 else 4

    lines = [
        "🧮 <b>ماشین‌حساب کریپتو</b>\n",
        f"مقدار: <b>{format_number(amount, amount_decimals)}</b> {symbol}",
        f"قیمت واحد: <b>${format_number(price_usd, 4)}</b>",
    ]

    if toman_rate:
        toman_value = usd_value * toman_rate
        lines.append(f"نرخ مبنای تتر: <b>{toman_str(toman_rate)}</b> تومان")
        lines.append(f"\n💵 مجموع دلاری: <b>${format_number(usd_value, 2)}</b>")
        lines.append(f"💰 مجموع تومانی: <b>{toman_str(toman_value)}</b> تومان")
    else:
        lines.append(f"\n💵 مجموع دلاری: <b>${format_number(usd_value, 2)}</b>")
        lines.append("💰 مجموع تومانی: <i>در دسترس نیست</i>")

    lines.append(FOOTER)
    await message.reply("\n".join(lines), parse_mode="HTML")
