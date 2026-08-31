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
import logging
import re
import time
import uuid
from typing import Optional, Dict, Any

import aiohttp
from aiogram import Router, types

logger = logging.getLogger("tools")

tools_router = Router(name="tools_router")

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


def rial_to_toman_str(rial_value: Any) -> str:
    """تبدیل ریال به تومان + جداکننده سه رقمی."""
    try:
        toman = float(rial_value) / 10
        return f"{toman:,.0f}"
    except (TypeError, ValueError):
        return "N/A"


def toman_str(toman_value: Any) -> str:
    """فرمت مقدار تومانی (بدون تبدیل) + جداکننده سه رقمی."""
    try:
        return f"{float(toman_value):,.0f}"
    except (TypeError, ValueError):
        return "N/A"


def format_number(value: Any, decimals: int = 2) -> str:
    try:
        return f"{float(value):,.{decimals}f}"
    except (TypeError, ValueError):
        return "N/A"


FOOTER = "\n<i>@SuperExFa_bot</i>"


# ===========================================================
# 1) Gold & Coin Service
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

GOLD_ITEMS = {
    "geram18": "طلای ۱۸ عیار",
    "geram24": "طلای ۲۴ عیار",
    "mesghal": "مثقال طلا (آبشده)",
    "sekee": "سکه امامی",
    "sekeb": "سکه بهار آزادی",
    "nim": "نیم سکه",
    "rob": "ربع سکه",
    "gerami": "سکه گرمی",
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


@tools_router.message(is_gold_trigger)
async def handle_gold_price(message: types.Message):
    data = await fetch_tgju_data()
    if not data:
        if message.chat.type == "private":
            await message.reply(
                "⚠️ در حال حاضر امکان دریافت قیمت طلا و سکه وجود ندارد.",
                parse_mode="HTML",
            )
        return  # Silent Fail در گروه

    current = data.get("current", {})

    def price_of(key: str) -> Optional[Any]:
        item = current.get(key)
        return item.get("p") if item else None

    lines = ["🥇 <b>قیمت لحظه‌ای طلا و سکه</b>\n"]
    found_any = False

    for key, label in GOLD_ITEMS.items():
        raw = price_of(key)
        if raw is None:
            continue
        found_any = True
        lines.append(f"• {label}: <b>{rial_to_toman_str(raw)}</b> تومان")

    ons_raw = price_of("ons")
    if ons_raw is not None:
        found_any = True
        lines.append(f"\n🌍 انس جهانی طلا: <b>{format_number(ons_raw)}</b> دلار")

    if not found_any:
        if message.chat.type == "private":
            await message.reply(
                "⚠️ در حال حاضر امکان دریافت قیمت طلا و سکه وجود ندارد.",
                parse_mode="HTML",
            )
        return

    lines.append(FOOTER)
    await message.reply("\n".join(lines), parse_mode="HTML")


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
    try:
        return float(item.get("p")) / 10  # ریال -> تومان
    except (TypeError, ValueError):
        return None


async def fetch_nobitex_usdt() -> Optional[dict]:
    url = "https://api.nobitex.ir/market/stats"
    params = {"srcCurrency": "usdt", "dstCurrency": "rls"}
    data = await _fetch_json(url, params=params)
    if not data:
        return None
    try:
        stats = data.get("stats", {}).get("usdt-rls", {})
        latest_rial = float(stats.get("latest"))
        day_change = stats.get("dayChange")
        return {"price_toman": latest_rial / 10, "change": day_change}
    except (TypeError, ValueError, AttributeError):
        return None


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
    if change is None:
        return ""
    try:
        change_val = float(change)
    except (TypeError, ValueError):
        return ""
    arrow = "🔺" if change_val > 0 else ("🔻" if change_val < 0 else "▪️")
    return f" ({arrow}{abs(change_val):.2f}%)"


def _format_exchange_line(name: str, info: Optional[dict]) -> str:
    if not info:
        return f"• {name}: <i>نامشخص</i>"
    price_str = toman_str(info["price_toman"])
    return f"• {name}: <b>{price_str}</b> تومان{_format_change(info.get('change'))}"


def is_dollar_trigger(message: types.Message) -> bool:
    text = (message.text or "").strip()
    if not text:
        return False
    lowered = text.lower()
    return any(kw.lower() in lowered for kw in DOLLAR_KEYWORDS)


@tools_router.message(is_dollar_trigger)
async def handle_dollar_price(message: types.Message):
    tgju_dollar, nobitex, bitpin, wallex = await asyncio.gather(
        fetch_tgju_dollar_toman(),
        fetch_nobitex_usdt(),
        fetch_bitpin_usdt(),
        fetch_wallex_usdt(),
    )

    if not any([tgju_dollar, nobitex, bitpin, wallex]):
        if message.chat.type == "private":
            await message.reply(
                "⚠️ در حال حاضر امکان دریافت نرخ دلار و تتر وجود ندارد.",
                parse_mode="HTML",
            )
        return  # Silent Fail در گروه

    lines = ["💵 <b>نرخ لحظه‌ای دلار و تتر</b>\n"]

    if tgju_dollar is not None:
        lines.append(f"🏦 دلار بازار آزاد تهران: <b>{toman_str(tgju_dollar)}</b> تومان\n")

    lines.append(_format_exchange_line("نوبیتکس (USDT)", nobitex))
    lines.append(_format_exchange_line("بیت‌پین (USDT)", bitpin))
    lines.append(_format_exchange_line("والکس (USDT)", wallex))

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
    nobitex, bitpin, wallex = await asyncio.gather(
        fetch_nobitex_usdt(), fetch_bitpin_usdt(), fetch_wallex_usdt()
    )
    rates = [r["price_toman"] for r in (nobitex, bitpin, wallex) if r and r.get("price_toman")]
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
