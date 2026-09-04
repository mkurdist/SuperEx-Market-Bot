import asyncio
import os
import io
import time
import uuid
import logging
import re
import aiohttp
import pandas as pd
import mplfinance as mpf
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from concurrent.futures import ThreadPoolExecutor

from aiogram import Bot, Dispatcher, types, F
from aiohttp import web
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, BufferedInputFile
from aiogram.filters.callback_data import CallbackData
from aiogram.filters import CommandStart
from dotenv import load_dotenv

# ---------------------------------------------------------
# ماژول ایزوله ۳ قابلیت جدید
# ---------------------------------------------------------
from tools import tools_router

# ---------------------------------------------------------
# Configuration & Setup
# ---------------------------------------------------------
load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN")
PORT = int(os.getenv("PORT", 8080))

logging.basicConfig(level=logging.INFO)

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

shared_session: aiohttp.ClientSession = None
USDT_TOMAN_RATE = None

TIMEFRAME_MAP = {
    "1m": "1m",
    "15m": "15m",
    "1h": "1h",
    "4h": "4h",
    "1d": "1d"
}

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

# ---------------------------------------------------------
# Formatting Helpers (برای خلاصه کردن اعداد)
# ---------------------------------------------------------
def format_abbrev(val: float) -> str:
    if val >= 1_000_000_000:
        return f"{val/1_000_000_000:.2f}B"
    if val >= 1_000_000:
        return f"{val/1_000_000:.2f}M"
    if val >= 1_000:
        return f"{val/1_000:.2f}K"
    s = f"{val:,.2f}"
    return s.rstrip('0').rstrip('.') if '.' in s else s

def format_comma(val: float) -> str:
    if val < 1: 
        s = f"{val:,.6f}"
    else:
        s = f"{val:,.2f}"
    return s.rstrip('0').rstrip('.') if '.' in s else s

# ---------------------------------------------------------
# Performance: Chart rendering pool + caches
# ---------------------------------------------------------
CHART_RENDER_EXECUTOR = ThreadPoolExecutor(
    max_workers=max(2, (os.cpu_count() or 2)),
    thread_name_prefix="chart-render"
)

CHART_RENDER_SEMAPHORE = asyncio.Semaphore(int(os.getenv("MAX_CONCURRENT_CHARTS", "4")))

CHART_CACHE_TTL = 30  
CHART_CACHE: dict = {}

PRICE_CACHE_TTL = 10  
PRICE_CACHE: dict = {}

_CACHE_MAX_ENTRIES = 300  

def _cache_get(store: dict, key, ttl: float):
    entry = store.get(key)
    if entry and (time.time() - entry[0]) < ttl:
        return entry[1]
    return None

def _cache_set(store: dict, key, value):
    store[key] = (time.time(), value)
    if len(store) > _CACHE_MAX_ENTRIES:
        oldest_key = min(store, key=lambda k: store[k][0])
        store.pop(oldest_key, None)

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

class ChartCallback(CallbackData, prefix="chart"):
    symbol: str
    timeframe: str

# ---------------------------------------------------------
# Background Task: Fetch Tether/Toman Rate
# ---------------------------------------------------------
async def fetch_bitpin_usdt() -> float | None:
    url = "https://api.bitpin.ir/v1/mkt/markets/"
    try:
        async with shared_session.get(url, timeout=5.0) as resp:
            if resp.status == 200:
                data = await resp.json()
                markets = data.get("results", []) if isinstance(data, dict) else data
                for m in markets:
                    code = m.get("code") or f"{m.get('currency1')}_{m.get('currency2')}"
                    if code == "USDT_IRT":
                        return float(m.get("price"))
    except Exception as e:
        logging.error(f"Bitpin fetch error: {e}")
    return None

async def fetch_wallex_usdt() -> float | None:
    url = "https://api.wallex.ir/v1/markets"
    try:
        async with shared_session.get(url, timeout=5.0) as resp:
            if resp.status == 200:
                data = await resp.json()
                item = data.get("result", {}).get("symbols", {}).get("USDTTMN")
                if item: return float(item.get("stats", {}).get("lastPrice"))
    except Exception as e:
        logging.error(f"Wallex fetch error: {e}")
    return None

async def update_tether_rate_loop():
    global USDT_TOMAN_RATE
    while True:
        try:
            bitpin_rate, wallex_rate = await asyncio.gather(
                fetch_bitpin_usdt(), fetch_wallex_usdt(), return_exceptions=True
            )
            valid_rates = [r for r in (bitpin_rate, wallex_rate) if isinstance(r, float)]
            if valid_rates:
                USDT_TOMAN_RATE = sum(valid_rates) / len(valid_rates)
        except Exception as e:
            logging.error(f"Error in update_tether_rate_loop: {e}")
        await asyncio.sleep(120)

# ---------------------------------------------------------
# API Helper Functions 
# ---------------------------------------------------------
def get_superex_headers() -> dict:
    return {
        "accept": "*/*", "accept-language": "en", "client": "1",
        "nonce": uuid.uuid4().hex, "timestamp": str(int(time.time() * 1000)),
        "token": "", "content-type": "application/x-www-form-urlencoded"
    }

async def fetch_price_data(symbol: str) -> dict:
    base_symbol = symbol.lower().replace("_usdt", "").replace("usdt", "")
    url = f"https://api.superexchang.com/resource/v3/public/currency/new?currency={base_symbol}"
    
    try:
        async with shared_session.get(url, headers=get_superex_headers(), timeout=5.0) as response:
            if response.status == 200:
                res_data = await response.json()
                data_obj = res_data.get("data", {})
                
                if data_obj and data_obj.get("newPrice"):
                    price = float(data_obj.get("newPrice", "0.0"))
                    if price == 0.0: return {"error": "Symbol not found or invalid."}
                        
                    sum_number = float(data_obj.get("sumNumber", "0.0"))
                    volume_usdt = price * sum_number

                    return {
                        "symbol": base_symbol.upper(),
                        "price": str(data_obj.get("newPrice", "0.0")),
                        "change_24h": str(data_obj.get("change", "0.0")),
                        "high": str(data_obj.get("maxPrice", "0.0")),
                        "low": str(data_obj.get("minPrice", "0.0")),
                        "raw_volume": volume_usdt, # دیتای خام برای قالب‌بندی با M و B
                        "source": "SuperEx"
                    }
    except Exception as e:
        logging.error(f"Error fetching ticker for {symbol}: {e}")
            
    return {"error": "Symbol not found on SuperEx."}

async def fetch_binance_kline(symbol: str, timeframe: str) -> list:
    base_symbol = symbol.lower().replace("_usdt", "").replace("usdt", "")
    binance_symbol = f"{base_symbol.upper()}USDT"
    interval = TIMEFRAME_MAP.get(timeframe, "1h")
    url = f"https://data-api.binance.vision/api/v3/klines?symbol={binance_symbol}&interval={interval}&limit=60"
    
    parsed_data = []
    try:
        async with shared_session.get(url, timeout=5.0) as response:
            if response.status == 200:
                items = await response.json()
                if isinstance(items, list) and items:
                    for item in items:
                        t, o, h, l, c, v = item[0], item[1], item[2], item[3], item[4], item[5]
                        parsed_data.append({
                            "Date": pd.to_datetime(int(t), unit="ms"), "Open": float(o),
                            "High": float(h), "Low": float(l), "Close": float(c), "Volume": float(v)
                        })
    except Exception as e:
        logging.error(f"Binance Kline error for {symbol}: {e}")
    return parsed_data

async def fetch_coingecko_market_chart(symbol: str) -> list:
    base_symbol = symbol.lower().replace("_usdt", "").replace("usdt", "")
    search_url = f"https://api.coingecko.com/api/v3/search?query={base_symbol}"
    
    parsed_data = []
    try:
        async with shared_session.get(search_url, timeout=5.0) as resp:
            if resp.status == 200:
                search_data = await resp.json()
                coins = search_data.get("coins", [])
                if coins:
                    coin_id = coins[0].get("id")
                    chart_url = f"https://api.coingecko.com/api/v3/coins/{coin_id}/market_chart?vs_currency=usd&days=1"
                    async with shared_session.get(chart_url, timeout=5.0) as chart_resp:
                        if chart_resp.status == 200:
                            chart_data = await chart_resp.json()
                            prices, volumes = chart_data.get("prices", []), chart_data.get("total_volumes", [])
                            for i, p_item in enumerate(prices):
                                t, price = p_item[0], p_item[1]
                                vol = volumes[i][1] if i < len(volumes) else 0.0
                                parsed_data.append({
                                    "Date": pd.to_datetime(int(t), unit="ms"), "Open": float(price),
                                    "High": float(price), "Low": float(price), "Close": float(price), "Volume": float(vol)
                                })
    except Exception as e:
        logging.error(f"CoinGecko fallback error for {symbol}: {e}")
    return parsed_data

async def fetch_kline_with_fallback(symbol: str, timeframe: str) -> list:
    data = await fetch_binance_kline(symbol, timeframe)
    if data: return data
    data = await fetch_coingecko_market_chart(symbol)
    return data

async def get_price_data_cached(symbol: str) -> dict:
    cache_key = symbol.upper()
    cached = _cache_get(PRICE_CACHE, cache_key, PRICE_CACHE_TTL)
    if cached is not None: return cached

    data = await fetch_price_data(symbol)
    if "error" not in data: _cache_set(PRICE_CACHE, cache_key, data)
    return data

def _render_chart_sync(df: pd.DataFrame, symbol: str, timeframe: str) -> bytes:
    date_format = '%b' if timeframe == "1d" else '%H:%M'
    fig, axlist = mpf.plot(
        df, type='candle', style=CHART_STYLE, volume=False,
        ylabel='Price (USDT)', datetime_format=date_format,
        xrotation=0, tight_layout=True, returnfig=True, figsize=(10, 6)   
    )
    ax = axlist[0]
    ax.set_title(f"{symbol.upper().replace('USDT', '')}/USDT | {timeframe}", pad=10, fontsize=13, color='#e6e6e6', ha='center')

    x_min, x_max = ax.get_xlim()
    ax.set_xlim(x_min, x_max + 2)

    ax.text(0.5, 0.03, "created by @SuperEXPrice_bot | @SuperexIR", transform=ax.transAxes, ha='center', va='bottom', fontsize=9.5, color='#9a9a9a', fontweight='normal')

    buf = io.BytesIO()
    try:
        fig.savefig(buf, dpi=130, bbox_inches='tight', pad_inches=0.3, facecolor=fig.get_facecolor(), edgecolor='none')
        return buf.getvalue()
    finally:
        buf.close()
        plt.close(fig)

async def generate_chart_image(symbol: str, timeframe: str) -> bytes:
    cache_key = (symbol.upper(), timeframe)
    cached = _cache_get(CHART_CACHE, cache_key, CHART_CACHE_TTL)
    if cached is not None: return cached

    parsed_data = await fetch_kline_with_fallback(symbol, timeframe)
    if not parsed_data: raise ValueError("No chart data available.")

    df = pd.DataFrame(parsed_data)
    df.set_index("Date", inplace=True)
    df.sort_index(inplace=True)

    async with CHART_RENDER_SEMAPHORE:
        loop = asyncio.get_running_loop()
        image_bytes = await loop.run_in_executor(CHART_RENDER_EXECUTOR, _render_chart_sync, df, symbol, timeframe)

    _cache_set(CHART_CACHE, cache_key, image_bytes)
    return image_bytes

def get_price_keyboard(symbol: str) -> InlineKeyboardMarkup:
    url_register = "https://app.superex.live/register?invitationCode=VQK2N6DDS"
    url_group = "https://t.me/SuperexIR"
    base_symbol = symbol.upper().replace("_USDT", "").replace("USDT", "")
    
    keyboard = [
        [
            InlineKeyboardButton(text="1m", callback_data=ChartCallback(symbol=base_symbol, timeframe="1m").pack()),
            InlineKeyboardButton(text="15m", callback_data=ChartCallback(symbol=base_symbol, timeframe="15m").pack()),
            InlineKeyboardButton(text="1h", callback_data=ChartCallback(symbol=base_symbol, timeframe="1h").pack()),
            InlineKeyboardButton(text="4h", callback_data=ChartCallback(symbol=base_symbol, timeframe="4h").pack()),
            InlineKeyboardButton(text="1d", callback_data=ChartCallback(symbol=base_symbol, timeframe="1d").pack()),
        ],
        [
            InlineKeyboardButton(text="عضویت در گروه 👥", url=url_group),
            InlineKeyboardButton(text="ثبت نام در صرافی 🏦", url=url_register)
        ]
    ]
    return InlineKeyboardMarkup(inline_keyboard=keyboard)

# ---------------------------------------------------------
# تشخیص هوشمند نماد کریپتو
# ---------------------------------------------------------
COMMON_WORDS_BLOCKLIST = {
    "hi", "hello", "hey", "yo", "sup", "ok", "okay", "yes", "no", "yep", "nope",
    "bye", "please", "thanks", "thank", "welcome", "sorry", "lol", "lmao", "wow",
    "nice", "cool", "good", "bad", "great", "ok?", "test", "help", "info", "menu",
    "admin", "admins", "mod", "mods", "owner", "dev", "team", "support", "staff",
    "report", "spam", "ban", "kick", "mute", "bot", "bots",
    "buy", "sell", "sold", "pump", "dump", "moon", "scam", "fake", "real", "up", "down",
    "long", "short", "hold", "hodl", "new", "old", "price", "chart", "link", "join",
    "group", "channel", "start", "stop", "gold", "dollar", "usdt", "tether",
}

TICKER_REGEX = re.compile(r"^[a-zA-Z]{2,10}$")

def is_valid_ticker_symbol(text: str) -> bool:
    if not text: return False
    cleaned = text.strip()
    if not TICKER_REGEX.match(cleaned): return False
    if cleaned.lower() in COMMON_WORDS_BLOCKLIST: return False
    return True

# ---------------------------------------------------------
# Message Handlers
# ---------------------------------------------------------
@dp.message(CommandStart())
async def send_welcome_and_tutorials(message: types.Message):
    welcome_text = (
        "<b>به دستیار هوشمند SuperEx ایران خوش آمدید! 🌐</b>\n\n"
        "💡 شما می‌توانید نام هر ارز (مثل <code>BTC</code>) یا کلماتی مثل <code>طلا</code> و <code>تتر</code> را برای من بفرستید تا اطلاعات لحظه‌ای آن‌ها را به شما نشان دهم.\n\n"
        "📚 <b>فهرست آموزش‌ها و لینک‌های کاربردی صرافی:</b>\n\n"
        "📲 <b>نصب و راه‌اندازی:</b>\n"
        "🔸 <a href='https://t.me/SuperExNews_Iran/130'>لینک‌های دانلود و آپدیت اپلیکیشن</a>\n"
        "🔸 <a href='https://t.me/SuperExNews_Iran/3400'>آموزش فعال‌سازی اعلانات (نوتیفیکیشن) اپلیکیشن</a>\n\n"
        "🎓 <b>آموزش‌های معاملاتی (صفر تا صد):</b>\n"
        "🔹 <a href='https://t.me/SuperExNews_Iran/328'>آموزش ثبت‌نام، واریز/برداشت، اسپات، فیوچرز و رفرال‌گیری</a>\n"
        "🔹 <a href='https://t.me/SuperExNews_Iran/379'>آموزش تخصصی کار با فیوچرز</a>\n"
        "🔹 <a href='https://t.me/SuperExNews_Iran/4195'>نحوه معامله طلا و نقره در اسپات و فیوچرز</a>\n"
        "🔹 <a href='https://t.me/SuperExNews_Iran/3372'>نحوه کپی کردن معاملات در کپی‌تریدینگ</a>\n"
        "🔹 <a href='https://t.me/SuperExNews_Iran/3359'>نحوه درخواست برای تبدیل شدن به لیدر کپی‌ترید</a>\n\n"
        "🎁 <b>کسب درآمد، پاداش و رویدادها:</b>\n"
        "💸 <a href='https://t.me/SuperExNews_Iran/3426'>استیک USDT با سود ثابت ۱۰٪ در سال</a>\n"
        "🎯 <a href='https://t.me/SuperExNews_Iran/3277'>چالش هفتگی و ماهانه با پاداش نقدی تا ۸۰ دلار</a>\n"
        "🎟 <a href='https://t.me/SuperExNews_Iran/3634'>نحوه شرکت در لاتاری ۱ دلاری (1USD)</a>\n"
        "🤝 <a href='https://t.me/SuperExNews_Iran/1621'>ثبت درخواست سفیر شدن یا ایجاد رویداد با لینک اختصاصی</a>\n\n"
        "━━━━━━━━━━━━━━━━━━\n"
        "💬 در صورت داشتن هرگونه سوال، به گروه پشتیبانی مراجعه کنید."
    )
    await message.reply(welcome_text, parse_mode="HTML", disable_web_page_preview=True)


@dp.message(lambda message: is_valid_ticker_symbol(message.text or ""))
async def handle_ticker_input(message: types.Message):
    symbol = message.text.strip().upper()

    price_task = asyncio.create_task(get_price_data_cached(symbol))
    chart_task = asyncio.create_task(generate_chart_image(symbol, "1h"))

    data = await price_task

    if "error" in data:
        chart_task.cancel()
        return  

    try:
        coin_emoji = CRYPTO_EMOJIS.get(data['symbol'], "🪙")
        
        price_float = float(data['price'])
        high_float = float(data['high'])
        low_float = float(data['low'])
        vol_float = float(data.get('raw_volume', 0.0))
        change_str = data['change_24h']
        
        p_val = format_comma(price_float)
        h_val = format_comma(high_float)
        l_val = format_comma(low_float)
        vol_val = format_abbrev(vol_float)
        
        # آیدی‌های دقیق درخواستی شما
        E_P = "<tg-emoji emoji-id='5375296873982604963'>💰</tg-emoji>"
        E_24H = "<tg-emoji emoji-id='5246762912428603768'>📉</tg-emoji>"
        E_H = "<tg-emoji emoji-id='5244837092042750681'>📈</tg-emoji>"
        E_L = "<tg-emoji emoji-id='5429518319243775957'>📉</tg-emoji>"
        E_VOL = "<tg-emoji emoji-id='5231200819986047254'>📊</tg-emoji>"
        E_USDT = "<tg-emoji emoji-id='5197434882321567830'>💲</tg-emoji>"

        if USDT_TOMAN_RATE:
            p_tom = format_abbrev(price_float * USDT_TOMAN_RATE)
            h_tom = format_abbrev(high_float * USDT_TOMAN_RATE)
            l_tom = format_abbrev(low_float * USDT_TOMAN_RATE)
            usdt_val = f"{int(USDT_TOMAN_RATE):,}"
            
            caption = (
                f"{coin_emoji} <b>{data['symbol']}</b>\n"
                f"{E_P} P: {p_val} ≈ <b>{p_tom} تومان</b>\n"
                f"{E_24H} 24h: <b>{change_str}%</b>\n\n"
                f"{E_H} H: <b>${h_val}</b> | {h_tom} تومان\n"
                f"{E_L} L: <b>${l_val}</b> | {l_tom} تومان\n"
                f"{E_VOL} Vol: <b>{vol_val} USDT</b>\n"
                f"{E_USDT} USDT: <b>{usdt_val} تومان</b>\n"
            )
        else:
            caption = (
                f"{coin_emoji} <b>{data['symbol']}</b>\n"
                f"{E_P} P: <b>{p_val}</b>\n"
                f"{E_24H} 24h: <b>{change_str}%</b>\n\n"
                f"{E_H} H: <b>${h_val}</b>\n"
                f"{E_L} L: <b>${l_val}</b>\n"
                f"{E_VOL} Vol: <b>{vol_val} USDT</b>\n"
                f"{E_USDT} USDT: <b>در دسترس نیست</b>\n"
            )

    except Exception as e:
        logging.error(f"Error formatting caption: {e}")
        caption = f"{CRYPTO_EMOJIS.get(symbol, '🪙')} <b>{symbol}</b>\nError formatting data."

    try:
        chart_bytes = await chart_task
        photo = BufferedInputFile(chart_bytes, filename=f"{symbol}_chart.png")

        await message.reply_photo(
            photo=photo,
            caption=caption,
            parse_mode="HTML",
            reply_markup=get_price_keyboard(symbol)
        )
    except Exception as e:
        logging.error(f"Chart generation error: {e}")
        await message.reply(caption + "\n\n<i>(Chart unavailable)</i>", parse_mode="HTML")

@dp.callback_query(ChartCallback.filter())
async def process_chart_timeframe(query: types.CallbackQuery, callback_data: ChartCallback):
    symbol = callback_data.symbol
    timeframe = callback_data.timeframe
    
    await query.answer(f"Loading {timeframe} chart...")
    
    try:
        chart_bytes = await generate_chart_image(symbol, timeframe)
        new_photo = types.InputMediaPhoto(
            media=BufferedInputFile(chart_bytes, filename=f"{symbol}_{timeframe}.png"),
            caption=query.message.caption,
            parse_mode="HTML"
        )
        
        await query.message.edit_media(
            media=new_photo,
            reply_markup=get_price_keyboard(symbol)
        )
    except Exception as e:
        logging.error(f"Error updating chart: {e}")
        await query.answer("Failed to update chart.", show_alert=True)

# ---------------------------------------------------------
# اتصال روتر ۳ قابلیت جدید
# ---------------------------------------------------------
dp.include_router(tools_router)

# ---------------------------------------------------------
# Web Server Setup (For Render)
# ---------------------------------------------------------
async def health_check(request):
    return web.Response(text="SuperEx Bot is Running smoothly!")

async def main():
    global shared_session
    shared_session = aiohttp.ClientSession()
    
    app = web.Application()
    app.router.add_get('/', health_check)
    
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', PORT)
    
    logging.info(f"🌐 Web server starting on port {PORT}")
    await site.start()

    await bot.delete_webhook(drop_pending_updates=True)

    # اجرای تسک پس‌زمینه برای آپدیت نرخ تتر
    asyncio.create_task(update_tether_rate_loop())

    logging.info("🚀 Bot polling started")
    try:
        await dp.start_polling(bot, allowed_updates=["message", "callback_query"])
    finally:
        await shared_session.close()
        CHART_RENDER_EXECUTOR.shutdown(wait=False, cancel_futures=True)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logging.info("🛑 Bot stopped gracefully.")
