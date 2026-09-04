# -*- coding: utf-8 -*-
"""
tools.py
---------------------------------------------------------
ماژول ایزوله برای ۳ قابلیت جدید ربات:
    1) سرویس قیمت طلا، سکه و آبشده  (Gold & Coin Service)
    2) سرویس قیمت دلار و تتر        (Dollar & Tether Service)
    3) ماشین‌حساب تبدیل کریپتو و دلار (Calculator)

تمام خروجی‌های متنی این فایل با parse_mode="HTML" ارسال می‌شوند.
در گروه‌ها، در صورت خطا یا عدم موفقیت در دریافت داده، پیامی ارسال
نمی‌شود (Silent Fail) تا اسپم ایجاد نشود. ربات پیام‌های ریپلای را نادیده می‌گیرد.
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
import pandas as pd
import mplfinance as mpf
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

from aiogram import Router, types
from aiogram.types import BufferedInputFile

logger = logging.getLogger("tools")

tools_router = Router(name="tools_router")

_CHART_EXECUTOR = ThreadPoolExecutor(max_workers=2, thread_name_prefix="gold-chart")

# ===========================================================
# Chart Styles (دقیقاً مشابه کریپتو)
# ===========================================================
_MARKET_COLORS = mpf.make_marketcolors(
    up='#00d964', down='#ff3b3b', edge='inherit', wick='inherit', volume='in', ohlc='i'
)

CHART_STYLE = mpf.make_mpf_style(
    marketcolors=_MARKET_COLORS, base_mpf_style='binance', facecolor='#000000',   
    edgecolor='#555555', figcolor='#000000', gridcolor='#222222', gridstyle='--',
    y_on_right=False,      
    rc={
        'font.family': 'sans-serif', 'axes.titleweight': 'normal', 'axes.titlesize': 13,
        'axes.titlecolor': '#e6e6e6', 'axes.labelcolor': '#cfcfcf', 'xtick.color': '#9a9a9a',
        'ytick.color': '#9a9a9a', 'text.color': '#9a9a9a',        
    }
)

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
    try:
        async with aiohttp.ClientSession(timeout=REQUEST_TIMEOUT) as session:
            async with session.get(url, headers=headers or DEFAULT_HEADERS, params=params) as resp:
                if resp.status == 200:
                    return await resp.json(content_type=None)
                logger.warning(f"[tools] {url} -> HTTP {resp.status}")
    except Exception as e:
        logger.warning(f"[tools] fetch failed for {url}: {e}")
    return None

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

def toman_str(toman_value: Any) -> str:
    val = _clean_numeric(toman_value)
    if val is None:
        return "N/A"
    return f"{val:,.0f}"

def format_number(value: Any, decimals: int = 2) -> str:
    val = _clean_numeric(value)
    if val is None:
        return "N/A"
    return f"{val:,.{decimals}f}"

def get_persian_abbrev(val: float) -> tuple[str, str]:
    val = _clean_numeric(val)
    if val is None:
        return "N/A", ""
    if val >= 1_000_000_000:
        s = f"{val / 1_000_000_000:.2f}"
        return s.rstrip('0').rstrip('.') if '.' in s else s, " میلیارد"
    elif val >= 1_000_000:
        s = f"{val / 1_000_000:.2f}"
        return s.rstrip('0').rstrip('.') if '.' in s else s, " میلیون"
    elif val >= 1_000:
        s = f"{val / 1_000:.2f}"
        return s.rstrip('0').rstrip('.') if '.' in s else s, " هزار"
    else:
        return f"{val:,.0f}", ""

# تعریف ایموجی‌های فوتر
E_CHANNEL_ICON = "<tg-emoji emoji-id='5244940072473599757'>⭐</tg-emoji>"
E_GROUP_ICON = "<tg-emoji emoji-id='5242463950813012253'>👥</tg-emoji>"

FOOTER = (f"\n{E_CHANNEL_ICON} <a href='https://t.me/SuperExNews_Iran'>کانال رسمی اخبار ایران</a>\n"
    f"{E_GROUP_ICON} <a href='https://t.me/SuperexIR'>گروه گفتگو و پشتیبانی</a>")

def normalize_text(text: str) -> str:
    if not text:
        return ""
    text = text.translate(_DIGIT_TRANS)
    text = re.sub(r'[؟?!.،,]', '', text)
    return text.strip().lower()

def is_valid_keyword_trigger(message: types.Message, keywords: set) -> bool:
    if message.reply_to_message is not None:
        return False
    return normalize_text(message.text) in keywords

def is_valid_calc_trigger(message: types.Message, regex_pattern: re.Pattern) -> bool:
    if message.reply_to_message is not None:
        return False
    return bool(regex_pattern.match(normalize_text(message.text)))


# --- متغیرهای سراسری برای ایموجی‌های سه‌بعدی ---
E_USD = "<tg-emoji emoji-id='5197434882321567830'>💲</tg-emoji>"
E_TOMAN = "<tg-emoji emoji-id='5472030678633684592'>💸</tg-emoji>"
E_TOTAL = "<tg-emoji emoji-id='5375296873982604963'>💰</tg-emoji>"
E_HEADER_CONVERT = "<tg-emoji emoji-id='5402186569006210455'>🔄</tg-emoji>"
USDT_EMOJI = "<tg-emoji emoji-id='5203973501878286332'>💵</tg-emoji>"
E_18K = "<tg-emoji emoji-id='5242306076405148889'>💰</tg-emoji>"
E_OUNCE = "<tg-emoji emoji-id='5242751808111125319'>🌍</tg-emoji>"
DEFAULT_CRYPTO_EMOJI = "<tg-emoji emoji-id='5242390476807479264'>🪙</tg-emoji>"

CRYPTO_EMOJIS = {
    "GRAM": "<tg-emoji emoji-id='5321041614443944130'>💎</tg-emoji>",
    "TON": "<tg-emoji emoji-id='5204021979174157518'>💎</tg-emoji>",
    "USDT": "<tg-emoji emoji-id='5203973501878286332'>💵</tg-emoji>",
    "USDC": "<tg-emoji emoji-id='5204298115506517373'>💵</tg-emoji>",
    "BTC": "<tg-emoji emoji-id='5206210561364210906'>🪙</tg-emoji>", 
    "ETH": "<tg-emoji emoji-id='5206384773827670642'>🔷</tg-emoji>",
    "SOL": "<tg-emoji emoji-id='5206338061763362251'>🟣</tg-emoji>",
    "TRX": "<tg-emoji emoji-id='5206292569469760905'>🔴</tg-emoji>",
    "BNB": "<tg-emoji emoji-id='5204418030993422385'>🟡</tg-emoji>",
    "XRP": "<tg-emoji emoji-id='5204173131958204067'>✖️</tg-emoji>",
    "DOGE": "<tg-emoji emoji-id='5204328287651770656'>🐕</tg-emoji>",
    "ADA": "<tg-emoji emoji-id='5206635372284489921'>🔵</tg-emoji>",
    "DAI": "<tg-emoji emoji-id='5208870606409317456'>🟡</tg-emoji>",
    "DOT": "<tg-emoji emoji-id='5208683607828214797'>🩷</tg-emoji>",
    "MATIC": "<tg-emoji emoji-id='5208848070715916390'>💜</tg-emoji>",
    "LTC": "<tg-emoji emoji-id='5208615064445139636'>🥈</tg-emoji>",
    "SHIB": "<tg-emoji emoji-id='5206558148772511929'>🐶</tg-emoji>",
    "STETH": "<tg-emoji emoji-id='5226934633265904016'>💧</tg-emoji>",
    "WBTC": "<tg-emoji emoji-id='5224186386772411289'>🪙</tg-emoji>",
    "BCH": "<tg-emoji emoji-id='5226956374390355937'>🟩</tg-emoji>",
    "LINK": "<tg-emoji emoji-id='5226844752485297224'>🔗</tg-emoji>",
    "TUSD": "<tg-emoji emoji-id='5224659155297518214'>💵</tg-emoji>",
    "LEO": "<tg-emoji emoji-id='5224681630861375987'>🦁</tg-emoji>",
    "AVAX": "<tg-emoji emoji-id='5226653411692262875'>🔺</tg-emoji>",
    "XLM": "<tg-emoji emoji-id='5224278943317638868'>🚀</tg-emoji>",
    "XMR": "<tg-emoji emoji-id='5226686306846781788'>🕵️</tg-emoji>",
    "UNI": "<tg-emoji emoji-id='5226480109761867804'>🦄</tg-emoji>",
    "OKB": "<tg-emoji emoji-id='5224475648524826880'>⬛</tg-emoji>",
    "FIL": "<tg-emoji emoji-id='5224599712950139709'>🗄</tg-emoji>",
    "ETC": "<tg-emoji emoji-id='5224345794483599984'>☘️</tg-emoji>",
    "HBAR": "<tg-emoji emoji-id='5224436285149560605'>🪙</tg-emoji>",
    "ATOM": "<tg-emoji emoji-id='5226961305012811929'>⚛️</tg-emoji>"
}


# ===========================================================
# 1) Gold Service
# ===========================================================
GOLD_KEYWORDS = {"طلا", "سکه", "قیمت طلا", "قیمت سکه", "مظنه", "ابشده", "آبشده", "gold"}

TGJU_MIRRORS = [
    "https://call1.tgju.org/ajax.json",
    "https://call.tgju.org/ajax.json",
    "https://call5.tgju.org/ajax.json",
]

TGJU_HEADERS = {
    **DEFAULT_HEADERS,
    "Referer": "https://www.tgju.org/",
}

_TGJU_CACHE: Dict[str, Any] = {"ts": 0.0, "data": None}
_TGJU_CACHE_TTL = 5.0

async def fetch_tgju_data() -> Optional[dict]:
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

# استفاده از نماد اسپات طلا بجای فیوچرز برای چارت بی‌نقص و ۲۴ ساعته
YAHOO_CHART_URL = "https://query1.finance.yahoo.com/v8/finance/chart/GC=F"
_GOLD_CHART_CACHE: Dict[str, Any] = {"ts": 0.0, "data": None}
_GOLD_CHART_CACHE_TTL = 60.0

async def _fetch_gold_series(interval: str, range_: str):
    params = {"interval": interval, "range": range_}
    data = await _fetch_json(YAHOO_CHART_URL, headers=DEFAULT_HEADERS, params=params)
    if not data: return None
    try:
        result = data["chart"]["result"][0]
        timestamps = result["timestamp"]
        quote = result["indicators"]["quote"][0]
        
        opens = quote["open"]
        highs = quote["high"]
        lows = quote["low"]
        closes = quote["close"]
        volumes = quote.get("volume", [0] * len(timestamps))
        
        parsed_data = []
        for t, o, h, l, c, v in zip(timestamps, opens, highs, lows, closes, volumes):
            if c is not None and o is not None and h is not None and l is not None:
                parsed_data.append({
                    "Date": pd.to_datetime(t, unit="s"),
                    "Open": float(o),
                    "High": float(h),
                    "Low": float(l),
                    "Close": float(c),
                    "Volume": float(v)
                })
        return parsed_data if len(parsed_data) >= 2 else None
    except (KeyError, IndexError, TypeError):
        return None

async def fetch_world_gold_series():
    parsed_data = await _fetch_gold_series("15m", "1d")
    if not parsed_data: parsed_data = await _fetch_gold_series("1h", "5d")
    return parsed_data

def _render_world_gold_chart_sync(parsed_data) -> bytes:
    df = pd.DataFrame(parsed_data)
    df.set_index("Date", inplace=True)
    df.sort_index(inplace=True)

    fig, axlist = mpf.plot(
        df, type='candle', style=CHART_STYLE, volume=False,
        ylabel='Price (USD)', datetime_format='%H:%M',
        xrotation=0, tight_layout=True, returnfig=True, figsize=(10, 6)
    )
    
    ax = axlist[0]
    # تنظیم دقیق فاصله ۱۰ پیکسلی مثل چارت کریپتو
    ax.set_title("XAU/USD | World Gold Ounce", pad=10, fontsize=13, color='#e6e6e6', ha='center')

    # ایجاد فضای خالی در سمت راست
    x_min, x_max = ax.get_xlim()
    ax.set_xlim(x_min, x_max + 2)

    ax.text(
        0.5, 0.03, "created by @SuperExPrice_bot | @SuperexIR",
        transform=ax.transAxes, ha="center", va="bottom",
        fontsize=9.5, color="#9a9a9a", fontweight="normal"
    )

    buf = io.BytesIO()
    try:
        fig.savefig(buf, dpi=130, bbox_inches="tight", pad_inches=0.3, facecolor=fig.get_facecolor(), edgecolor="none")
        return buf.getvalue()
    finally:
        buf.close()
        plt.close(fig)

async def generate_world_gold_chart() -> Optional[bytes]:
    now = time.time()
    if _GOLD_CHART_CACHE["data"] and (now - _GOLD_CHART_CACHE["ts"]) < _GOLD_CHART_CACHE_TTL:
        return _GOLD_CHART_CACHE["data"]
    parsed_data = await fetch_world_gold_series()
    if not parsed_data: return None
    loop = asyncio.get_running_loop()
    image_bytes = await loop.run_in_executor(_CHART_EXECUTOR, _render_world_gold_chart_sync, parsed_data)
    _GOLD_CHART_CACHE["data"] = image_bytes
    _GOLD_CHART_CACHE["ts"] = now
    return image_bytes


@tools_router.message(lambda msg: is_valid_keyword_trigger(msg, GOLD_KEYWORDS))
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
        return  # Silent Fail

    lines = ["🥇 <b>قیمت لحظه‌ای طلا</b>\n"]
    if geram18_val is not None: 
        lines.append(f"{E_18K} طلای ۱۸ عیار : <code>{geram18_val / 10:,.0f}</code> تومان")
    if ons_val is not None: 
        lines.append(f"{E_OUNCE} انس جهانی طلا : <code>{ons_val:,.2f}</code> دلار")
    
    lines.append(FOOTER)
    caption = "\n".join(lines)

    chart_bytes = None
    try: chart_bytes = await generate_world_gold_chart()
    except Exception as e: logger.warning(f"[tools] world gold chart failed: {e}")

    if chart_bytes:
        photo = BufferedInputFile(chart_bytes, filename="world_gold_chart.png")
        await message.reply_photo(photo=photo, caption=caption, parse_mode="HTML", disable_web_page_preview=True)
    else:
        await message.reply(caption, parse_mode="HTML", disable_web_page_preview=True)


# ===========================================================
# 2) Dollar & Tether Service
# ===========================================================
DOLLAR_KEYWORDS = {"دلار", "تتر", "usdt", "dollar", "قیمت دلار", "قیمت تتر", "نرخ دلار"}

async def fetch_tgju_dollar_toman() -> Optional[float]:
    data = await fetch_tgju_data()
    if not data: return None
    item = data.get("current", {}).get("price_dollar_rl")
    if not item: return None
    val = _clean_numeric(item.get("p"))
    return val / 10 if val else None

async def fetch_bitpin_usdt() -> Optional[dict]:
    url = "https://api.bitpin.ir/v1/mkt/markets/"
    data = await _fetch_json(url)
    if not data: return None
    try:
        markets = data.get("results", []) if isinstance(data, dict) else data
        for m in markets:
            code = m.get("code") or f"{m.get('currency1')}_{m.get('currency2')}"
            if code == "USDT_IRT":
                return {"price_toman": float(m.get("price")), "change": m.get("price_change_percent") or m.get("change")}
    except Exception: pass
    return None

async def fetch_wallex_usdt() -> Optional[dict]:
    url = "https://api.wallex.ir/v1/markets"
    data = await _fetch_json(url)
    if not data: return None
    try:
        symbols = data.get("result", {}).get("symbols", {})
        item = symbols.get("USDTTMN")
        if item: return {"price_toman": float(item.get("stats", {}).get("lastPrice")), "change": item.get("stats", {}).get("dailyChangePercent") or item.get("stats", {}).get("24h_ch")}
    except Exception: pass
    return None

def _format_change(change: Any) -> str:
    val = _clean_numeric(change)
    if val is None: return ""
    arrow = "🔺" if val > 0 else ("🔻" if val < 0 else "▪️")
    return f" ({arrow}{abs(val):.2f}%)"

@tools_router.message(lambda msg: is_valid_keyword_trigger(msg, DOLLAR_KEYWORDS))
async def handle_dollar_price(message: types.Message):
    tgju_dollar, bitpin, wallex = await asyncio.gather(
        fetch_tgju_dollar_toman(), fetch_bitpin_usdt(), fetch_wallex_usdt()
    )
    sources = [s for s in (bitpin, wallex) if s and _clean_numeric(s.get("price_toman")) is not None]

    if tgju_dollar is None and not sources:
        return  # Silent Fail

    lines = [f"{USDT_EMOJI} <b>نرخ لحظه‌ای دلار و تتر</b>\n"]
    if tgju_dollar is not None: 
        lines.append(f"🏦 دلار بازار آزاد : <code>{toman_str(tgju_dollar)}</code> تومان\n")

    if sources:
        avg_price = sum(_clean_numeric(s["price_toman"]) for s in sources) / len(sources)
        changes = [c for c in [_clean_numeric(s.get("change")) for s in sources] if c is not None]
        avg_change_str = _format_change(sum(changes) / len(changes)) if changes else ""
        lines.append(f"{USDT_EMOJI} میانگین قیمت تتر : <code>{toman_str(avg_price)}</code> تومان <b>{avg_change_str}</b>")
    else:
        lines.append(f"{USDT_EMOJI} میانگین قیمت تتر : <i>در دسترس نیست</i>")

    lines.append(FOOTER)
    await message.reply("\n".join(lines), parse_mode="HTML", disable_web_page_preview=True)


# ===========================================================
# 3) Calculators (Gold & Dollar/Crypto)
# ===========================================================

GOLD_CALC_REGEX = re.compile(r"^\s*(\d+(?:\.\d+)?)\s*(?:گرم\s*)?(طلا|سکه|آبشده|ابشده)\s*$")
DOLLAR_CALC_REGEX = re.compile(r"^\s*(\d+(?:\.\d+)?)\s*(دلار|تتر|usdt|dollar)\s*$")
CALC_REGEX = re.compile(r"^\s*(\d+(?:\.\d+)?)\s*([a-z]{2,10})\s*$")

async def get_avg_usdt_toman_rate() -> Optional[float]:
    bitpin, wallex = await asyncio.gather(fetch_bitpin_usdt(), fetch_wallex_usdt())
    rates = [_clean_numeric(r["price_toman"]) for r in (bitpin, wallex) if r and _clean_numeric(r.get("price_toman")) is not None]
    return sum(rates) / len(rates) if rates else None

# ----------- ماشین‌حساب طلا -----------
@tools_router.message(lambda msg: is_valid_calc_trigger(msg, GOLD_CALC_REGEX))
async def handle_gold_gram_calculator(message: types.Message):
    match = GOLD_CALC_REGEX.match(normalize_text(message.text))
    if not match: return
    try: grams = float(match.group(1))
    except ValueError: return
    if grams <= 0: return

    data = await fetch_tgju_data()
    geram18_val: Optional[float] = None
    if data:
        item = data.get("current", {}).get("geram18")
        geram18_val = _clean_numeric(item.get("p")) if item else None

    if geram18_val is None:
        return  # Silent Fail

    price_per_gram_toman = geram18_val / 10
    total_toman = grams * price_per_gram_toman
    
    total_num, total_suffix = get_persian_abbrev(total_toman)
    amount_str = f"{grams:,.0f}" if grams.is_integer() else f"{grams:,.2f}".rstrip('0').rstrip('.')

    lines = [
        f"⚖️ وزن : <code>{amount_str}</code> گرم (طلای ۱۸ عیار)",
        f"{E_18K} قیمت هر گرم : <code>{toman_str(price_per_gram_toman)}</code> تومان",
        f"\n{E_TOMAN} مجموع : <code>{total_num}</code>{total_suffix} تومان",
        FOOTER,
    ]
    await message.reply("\n".join(lines), parse_mode="HTML", disable_web_page_preview=True)

# ----------- ماشین‌حساب دلار و تتر -----------
@tools_router.message(lambda msg: is_valid_calc_trigger(msg, DOLLAR_CALC_REGEX))
async def handle_dollar_calculator(message: types.Message):
    match = DOLLAR_CALC_REGEX.match(normalize_text(message.text))
    if not match: return
    try: amount = float(match.group(1))
    except ValueError: return
    if amount <= 0: return

    currency_name = "USDT / دلار"
    if "usdt" in match.group(2) or "تتر" in match.group(2):
        currency_name = "USDT / دلار"

    toman_rate = await get_avg_usdt_toman_rate()
    if not toman_rate:
        return # Silent Fail

    total_toman = amount * toman_rate
    
    amount_str = f"{amount:,.0f}" if amount.is_integer() else f"{amount:,.2f}".rstrip('0').rstrip('.')
    total_num, total_suffix = get_persian_abbrev(total_toman)
    
    lines = [
        f"{E_HEADER_CONVERT} <b>تبدیل دلار به تومان</b>\n",
        f"{E_USD} مقدار : <code>{amount_str}</code> {currency_name}",
        f"{E_USD} قیمت تتر : <code>{toman_str(toman_rate)}</code> تومان",
        f"{E_TOMAN} معادل تومان : <code>{total_num}</code>{total_suffix} تومان",
        FOOTER,
    ]
    await message.reply("\n".join(lines), parse_mode="HTML", disable_web_page_preview=True)

# ----------- ماشین‌حساب سایر کریپتوها -----------
BINANCE_PRICE_URL = "https://data-api.binance.vision/api/v3/ticker/price"
SUPEREX_TICKER_URL = "https://api.superexchang.com/resource/v3/public/currency/new"

async def fetch_binance_usdt_price(symbol: str) -> Optional[float]:
    url = f"{BINANCE_PRICE_URL}?symbol={symbol.upper()}USDT"
    data = await _fetch_json(url)
    if data and isinstance(data, dict) and "price" in data:
        try: return float(data["price"])
        except ValueError: pass
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
        price = (data.get("data", {}) if isinstance(data, dict) else {}).get("newPrice")
        if price:
            try: return float(price)
            except ValueError: pass
    return None

@tools_router.message(lambda msg: is_valid_calc_trigger(msg, CALC_REGEX))
async def handle_crypto_calculator(message: types.Message):
    match = CALC_REGEX.match(normalize_text(message.text))
    if not match: return
    try: amount = float(match.group(1))
    except ValueError: return
    symbol = match.group(2).upper()
    if amount <= 0: return

    price_usd = await fetch_binance_usdt_price(symbol)
    if price_usd is None: price_usd = await fetch_superex_usdt_price(symbol)
    if price_usd is None: return

    toman_rate = await get_avg_usdt_toman_rate()
    usd_value = amount * price_usd
    amount_decimals = 8 if amount < 1 else 4
    
    amount_str = f"{amount:,.0f}" if amount.is_integer() else f"{amount:,.{amount_decimals}f}".rstrip('0').rstrip('.')

    coin_emoji = CRYPTO_EMOJIS.get(symbol, DEFAULT_CRYPTO_EMOJI)

    lines = [
        f"{coin_emoji} مقدار : <code>{amount_str}</code> {symbol}",
        f"{E_USD} قیمت واحد : <code>${format_number(price_usd, 4)}</code>",
    ]

    if toman_rate:
        toman_value = usd_value * toman_rate
        total_num, total_suffix = get_persian_abbrev(toman_value)
        
        lines.append(f"{E_USD} نرخ مبنای تتر : <code>{toman_str(toman_rate)}</code> تومان")
        lines.append(f"\n{E_TOMAN} مجموع دلاری : <code>${format_number(usd_value, 2)}</code>")
        lines.append(f"{E_TOTAL} مجموع تومانی : <code>{total_num}</code>{total_suffix} تومان")
    else:
        lines.append(f"\n{E_TOMAN} مجموع دلاری : <code>${format_number(usd_value, 2)}</code>")
        lines.append(f"{E_TOTAL} مجموع تومانی : <i>در دسترس نیست</i>")

    lines.append(FOOTER)
    await message.reply("\n".join(lines), parse_mode="HTML", disable_web_page_preview=True)
