import asyncio
import os
import io
import time
import uuid
import json
import base64
import gzip
import logging
import aiohttp
import pandas as pd
import mplfinance as mpf
import matplotlib
matplotlib.use('Agg') # Prevents GUI crashes on headless servers like Render
import matplotlib.pyplot as plt
from PIL import Image, ImageChops
from concurrent.futures import ThreadPoolExecutor

from aiogram import Bot, Dispatcher, types, F
from aiohttp import web
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, BufferedInputFile
from aiogram.filters.callback_data import CallbackData
from dotenv import load_dotenv

# ---------------------------------------------------------
# Configuration & Setup
# ---------------------------------------------------------
load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN")
PORT = int(os.getenv("PORT", 8080))

logging.basicConfig(level=logging.INFO)

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

TIMEFRAME_MAP = {
    "1m": 60,
    "15m": 900,
    "1h": 3600,
    "4h": 14400,
    "1d": 86400
}

# ---------------------------------------------------------
# Performance: Chart rendering pool + caches
# ---------------------------------------------------------
# رندر چارت با matplotlib یک عملیات CPU-bound و نسبتاً سنگین است.
# اگر مستقیم در event loop اصلی اجرا شود، کل ربات (برای همه‌ی کاربران)
# در حین رندر یک چارت بلاک می‌شود. به همین دلیل آن را در یک
# ThreadPoolExecutor مجزا اجرا می‌کنیم.
CHART_RENDER_EXECUTOR = ThreadPoolExecutor(
    max_workers=max(2, (os.cpu_count() or 2)),
    thread_name_prefix="chart-render"
)

# محدود کردن تعداد رندرهای هم‌زمان تا CPU سرور زیر بار سنگین له نشود
# (مقدار قابل تنظیم با متغیر محیطی MAX_CONCURRENT_CHARTS)
CHART_RENDER_SEMAPHORE = asyncio.Semaphore(int(os.getenv("MAX_CONCURRENT_CHARTS", "4")))

# کش کوتاه‌مدت روی تصویر نهایی چارت: وقتی چند کاربر هم‌زمان همون نماد/تایم‌فریم
# رو می‌خوان یا کاربر سریع روی دکمه‌های تایم‌فریم کلیک می‌کنه، به‌جای فراخوانی
# مجدد وب‌ساکت + رندر مجدد، تصویر کش‌شده برگردونده می‌شه.
CHART_CACHE_TTL = 5  # ثانیه
CHART_CACHE: dict = {}

# کش کوتاه‌مدت روی نتیجه‌ی استعلام قیمت (خود fetch_price_data دست‌نخورده می‌ماند،
# این فقط یک لایه‌ی wrapper جلوی آن است)
PRICE_CACHE_TTL = 3  # ثانیه
PRICE_CACHE: dict = {}

_CACHE_MAX_ENTRIES = 300  # سقف اندازه‌ی کش‌ها تا حافظه بی‌رویه رشد نکند


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


# استایل و رنگ‌های چارت قبلاً هر بار داخل تابع رندر از نو ساخته می‌شدند؛
# چون به هیچ ورودی وابسته نیستند، یک‌بار در سطح ماژول ساخته می‌شوند.
#
# استایل «مشکی خالص + سبز/قرمز پررنگ + قاب نازک خاکستری» مطابق نمونه‌ی
# مرجع (چارت بات دیگر که کاربر خواسته شبیه‌اش باشیم).
_MARKET_COLORS = mpf.make_marketcolors(
    up='#00d964',      # سبز پررنگ و واضح (به‌جای تیل کم‌رنگ قبلی)
    down='#ff3b3b',    # قرمز پررنگ و واضح
    edge='inherit',
    wick='inherit',
    volume='in',
    ohlc='i'
)

CHART_STYLE = mpf.make_mpf_style(
    marketcolors=_MARKET_COLORS,
    base_mpf_style='binance',  # پایه‌ای ساده‌تر، بدون سایه‌ها/افکت‌های اضافه‌ی nightclouds
    facecolor='#000000',   # پس‌زمینه‌ی مشکی خالص (طبق تصویر مرجع)
    edgecolor='#555555',   # رنگ قاب/کادر دور چارت
    figcolor='#000000',
    gridcolor='#222222',   # خطوط شبکه بسیار کم‌رنگ روی زمینه‌ی مشکی
    gridstyle='--',
    y_on_right=False,      # لیبل‌های قیمت سمت چپ، مطابق تصویر مرجع
    rc={
        'font.family': 'sans-serif',
        'axes.titleweight': 'normal',   # عنوان نازک، نه بولد
        'axes.titlesize': 13,
        'axes.titlecolor': '#e6e6e6',
        'axes.labelcolor': '#cfcfcf',   # رنگ لیبل «Price (USDT)»
        'xtick.color': '#9a9a9a',
        'ytick.color': '#9a9a9a',
        'text.color': '#9a9a9a',        # رنگ پیش‌فرض متن (امضا هم از همینه)
    }
)

# ---------------------------------------------------------
# Callback Data Factories
# ---------------------------------------------------------
class ChartCallback(CallbackData, prefix="chart"):
    """Callback factory for chart timeframe buttons."""
    symbol: str
    timeframe: str

# ---------------------------------------------------------
# API Helper Functions (دست‌نخورده - طبق درخواست)
# ---------------------------------------------------------
def get_superex_headers() -> dict:
    """Generates dynamic security headers required by SuperEx API."""
    return {
        "accept": "*/*",
        "accept-language": "en",
        "client": "1",
        "nonce": uuid.uuid4().hex,
        "timestamp": str(int(time.time() * 1000)),
        "token": "",
        "content-type": "application/x-www-form-urlencoded"
    }

async def fetch_binance_fallback(symbol: str) -> dict:
    """Fallback to Binance API for 24h Ticker."""
    base_symbol = symbol.upper().replace("_USDT", "").replace("USDT", "")
    binance_symbol = f"{base_symbol}USDT"
    
    url = f"https://api.binance.com/api/v3/ticker/24hr?symbol={binance_symbol}"
    
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(url) as response:
                if response.status == 200:
                    data = await response.json()
                    return {
                        "symbol": base_symbol,
                        "price": data.get("lastPrice", "0.0"),
                        "change_24h": data.get("priceChangePercent", "0.0"),
                        "high": data.get("highPrice", "0.0"),
                        "low": data.get("lowPrice", "0.0"),
                        "volume": data.get("quoteVolume", "0.0"),
                        "source": "Binance"
                    }
        except Exception as e:
            logging.error(f"Binance ticker fallback error: {e}")
            
    return {"error": "Symbol not found on SuperEx or Binance."}

async def fetch_binance_kline_fallback(symbol: str, timeframe: str) -> list:
    """Fallback to Binance Data API for Kline (Chart) data (Bypasses US Geo-block)."""
    base_symbol = symbol.upper().replace("_USDT", "").replace("USDT", "")
    binance_symbol = f"{base_symbol}USDT"
    
    url = f"https://data-api.binance.vision/api/v3/klines?symbol={binance_symbol}&interval={timeframe}&limit=60"
    
    parsed_data = []
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(url) as response:
                if response.status == 200:
                    data = await response.json()
                    for item in data:
                        parsed_data.append({
                            "Date": pd.to_datetime(int(item[0]), unit="ms"),
                            "Open": float(item[1]),
                            "High": float(item[2]),
                            "Low": float(item[3]),
                            "Close": float(item[4]),
                            "Volume": float(item[5])
                        })
        except Exception as e:
            logging.error(f"Binance kline fallback error: {e}")
            
    return parsed_data

async def fetch_price_data(symbol: str) -> dict:
    """
    Fetches the latest 24h ticker data directly using the new SuperEx resource API.
    """
    base_symbol = symbol.lower().replace("_usdt", "").replace("usdt", "")
    url = f"https://api.superexchang.com/resource/v3/public/currency/new?currency={base_symbol}"
    
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(url, headers=get_superex_headers()) as response:
                if response.status == 200:
                    res_data = await response.json()
                    data_obj = res_data.get("data", {})
                    
                    if data_obj and data_obj.get("newPrice"):
                        return {
                            "symbol": base_symbol.upper(),
                            "price": str(data_obj.get("newPrice", "0.0")),
                            "change_24h": str(data_obj.get("change", "0.0")),
                            "high": str(data_obj.get("maxPrice", "0.0")),
                            "low": str(data_obj.get("minPrice", "0.0")),
                            "volume": str(data_obj.get("sumNumber", "0.0")),
                            "source": "SuperEx"
                        }
        except Exception as e:
            logging.error(f"Error fetching ticker for {symbol}: {e}")
            
    logging.warning(f"Symbol {symbol} not found on SuperEx. Trying Binance fallback...")
    return await fetch_binance_fallback(symbol)

async def fetch_superex_kline_ws(symbol: str, timeframe: str) -> list:
    """
    Connects to SuperEx WebSocket, sends the kline request, decodes the GZIP/Base64 response.
    """
    ws_url = "wss://api.superexchang.com/ws"
    tf_seconds = TIMEFRAME_MAP.get(timeframe, 3600)
    base_symbol = symbol.lower().replace("_usdt", "").replace("usdt", "")
    topic = f"spot/candle{tf_seconds}:{base_symbol}_usdt"
    
    ws_headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Origin": "https://www.superex.com"
    }
    
    parsed_data = []
    try:
        async with aiohttp.ClientSession() as session:
            async with session.ws_connect(ws_url, headers=ws_headers, timeout=5.0) as ws:
                req_msg = {"op": "req", "action": "action", "args": [topic], "to": 300}
                await ws.send_json(req_msg)
                
                for _ in range(10):
                    msg = await ws.receive(timeout=3.0)
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        if msg.data == 'ping':
                            await ws.send_str('pong')
                            continue
                        
                        try:
                            b64_data = msg.data + "=" * ((4 - len(msg.data) % 4) % 4)
                            uncompressed = gzip.decompress(base64.b64decode(b64_data)).decode('utf-8')
                            data_json = json.loads(uncompressed)
                            
                            if data_json.get("action") == "action":
                                klines = data_json.get("data", [])
                                for item in klines:
                                    if isinstance(item, dict):
                                        t = item.get("time", 0)
                                        o = item.get("open", 0)
                                        h = item.get("high", 0)
                                        l = item.get("low", 0)
                                        c = item.get("close", 0)
                                        v = item.get("volume", 0)
                                        
                                        parsed_data.append({
                                            "Date": pd.to_datetime(int(t), unit="ms"),
                                            "Open": float(o), "High": float(h),
                                            "Low": float(l), "Close": float(c),
                                            "Volume": float(v)
                                        })
                                if parsed_data:
                                    return parsed_data
                        except Exception:
                            pass
    except Exception as e:
        logging.error(f"SuperEx WS Kline error: {e}")
        
    return parsed_data

# ---------------------------------------------------------
# Cached wrappers around the exchange query methods
# (خود متدهای بالا دست‌نخورده‌اند؛ این‌ها فقط یک لایه‌ی نازک کش جلوشون هستن)
# ---------------------------------------------------------
async def get_price_data_cached(symbol: str) -> dict:
    """
    اگر همین نماد در چند ثانیه‌ی اخیر استعلام شده باشه (مثلاً چند کاربر
    هم‌زمان BTC رو می‌پرسن)، به‌جای زدن درخواست جدید، نتیجه‌ی کش‌شده برمی‌گرده.
    """
    cache_key = symbol.upper()
    cached = _cache_get(PRICE_CACHE, cache_key, PRICE_CACHE_TTL)
    if cached is not None:
        return cached

    data = await fetch_price_data(symbol)
    if "error" not in data:
        _cache_set(PRICE_CACHE, cache_key, data)
    return data

# ---------------------------------------------------------
# Chart Generation
# ---------------------------------------------------------
def _autocrop_black_margins(png_bytes: bytes, padding: int = 18, min_width: int = 1100) -> bytes:
    """
    حاشیه‌ی خالیِ مشکیِ اضافه دور کل تصویر رو حذف می‌کنه تا چارت داخل قاب
    پیام تلگرام لبه‌به‌لبه و بزرگ دیده بشه، نه کوچیک و توی عمق.

    برخلاف bbox_inches='tight' (که layout دستیِ subplots_adjust رو
    نادیده می‌گیره و باعث تداخل امضا با لیبل‌های تاریخ می‌شد)، این تابع
    کاملاً بعد از رندر و روی خودِ تصویر PNG کار می‌کنه، پس مشکل قبلی رو
    تکرار نمی‌کنه.
    """
    img = Image.open(io.BytesIO(png_bytes)).convert('RGB')
    background = Image.new('RGB', img.size, (0, 0, 0))
    diff = ImageChops.difference(img, background)
    bbox = diff.getbbox()

    if bbox:
        left, upper, right, lower = bbox
        left = max(0, left - padding)
        upper = max(0, upper - padding)
        right = min(img.width, right + padding)
        lower = min(img.height, lower + padding)
        img = img.crop((left, upper, right, lower))

    # تلگرام تصاویر کوچیک رو upscale نمی‌کنه (فقط بزرگ‌ها رو کوچیک می‌کنه)،
    # پس اگه بعد از کراپ، عرض تصویر خیلی کم بمونه، چارت داخل باکس پیام
    # کوچیک و دور نمایش داده می‌شه. برای همین حداقل عرض رو تضمین می‌کنیم.
    if img.width < min_width:
        scale = min_width / img.width
        new_size = (min_width, round(img.height * scale))
        img = img.resize(new_size, Image.LANCZOS)

    out = io.BytesIO()
    img.save(out, format='PNG')
    return out.getvalue()


def _render_chart_sync(df: pd.DataFrame, symbol: str, timeframe: str) -> bytes:
    """
    بخش سنگین و CPU-bound رسم چارت (matplotlib). این تابع sync است و
    همیشه باید از طریق run_in_executor در یک ترد جدا فراخوانی بشه، نه
    مستقیم در event loop - وگرنه هنگام رندر، کل ربات برای همه‌ی
    کاربران دیگه هم بلاک می‌شه.
    """
    # فرمت تاریخ محور X بر اساس تایم‌فریم:
    # - تایم‌فریم‌های کوچک‌تر از روزانه (1m/15m/1h/4h): فقط ساعت:دقیقه، بدون تاریخ/ماه
    # - تایم‌فریم روزانه (1d): فقط نام کوتاه ماه (مثل Feb)
    if timeframe == "1d":
        date_format = '%b'
        x_rotation = 0
    else:
        date_format = '%H:%M'
        x_rotation = 0

    fig, axlist = mpf.plot(
        df,
        type='candle',
        style=CHART_STYLE,
        volume=False,
        title=f"{symbol.upper().replace('USDT', '')}/USDT | {timeframe}",
        ylabel='Price (USDT)',   # لیبل محور قیمت، مطابق تصویر مرجع
        datetime_format=date_format,
        xrotation=x_rotation,
        tight_layout=False,
        returnfig=True,
        figsize=(13, 7.8)   # بزرگ‌تر شده تا با اندازه‌ی سایر ربات‌ها هم‌خوان باشه
    )

    # حاشیه‌های خیلی تنگ‌تر شدن (نسبت به قبل) تا چارت واقعی بیشترین فضای
    # ممکن رو از کل بوم تصویر پر کنه و به‌جای «کوچیک و توی عمق»، لبه‌به‌لبه
    # و نزدیک دیده بشه - دقیقاً مثل تصویر مرجع.
    fig.subplots_adjust(top=0.94, bottom=0.11, left=0.065, right=0.985)

    # قاب/کادر نازک دور کل چارت: مطمئن می‌شیم هر ۴ لبه (spine) نمایش داده
    # بشن و رنگ یکسان داشته باشن، چون بعضی استایل‌های پایه‌ی mplfinance
    # بعضی لبه‌ها رو به‌صورت پیش‌فرض مخفی می‌کنن.
    for ax in axlist:
        for spine in ax.spines.values():
            spine.set_visible(True)
            spine.set_color('#555555')
            spine.set_linewidth(0.8)

    watermark_text = "Created by @SuperExFa_bot | @SuperexIR"
    fig.text(
        0.5, 0.02, watermark_text,
        ha='center', va='center', fontsize=10.5, color='#9a9a9a', fontweight='normal',
        transform=fig.transFigure
    )

    buf = io.BytesIO()
    try:
        fig.savefig(
            buf, dpi=140,
            bbox_inches=None, pad_inches=0,
            facecolor=fig.get_facecolor(), edgecolor='none'
        )
        # حذف حاشیه‌ی خالی مشکی اضافه دور تصویر (توضیح در _autocrop_black_margins)
        return _autocrop_black_margins(buf.getvalue(), padding=10)
    finally:
        buf.close()
        # نکته‌ی مهم: در نسخه‌ی قبلی figure ها هیچ‌وقت close نمی‌شدند،
        # که باعث نشت حافظه‌ی تدریجی و کند شدن ربات بعد از مدتی کار می‌شد.
        plt.close(fig)


async def generate_chart_image(symbol: str, timeframe: str) -> bytes:
    """
    Generates a professional, TradingView-style candlestick chart 
    with high precision UI and a clean, non-overlapping SuperEx watermark.
    """
    cache_key = (symbol.upper(), timeframe)
    cached = _cache_get(CHART_CACHE, cache_key, CHART_CACHE_TTL)
    if cached is not None:
        return cached

    parsed_data = await fetch_superex_kline_ws(symbol, timeframe)
    
    if not parsed_data:
        logging.warning(f"SuperEx WS failed for {symbol} chart. Using Binance fallback...")
        parsed_data = await fetch_binance_kline_fallback(symbol, timeframe)

    if not parsed_data:
        raise ValueError("No chart data available from any source.")

    df = pd.DataFrame(parsed_data)
    df.set_index("Date", inplace=True)
    df.sort_index(inplace=True)

    async with CHART_RENDER_SEMAPHORE:
        loop = asyncio.get_running_loop()
        image_bytes = await loop.run_in_executor(
            CHART_RENDER_EXECUTOR, _render_chart_sync, df, symbol, timeframe
        )

    _cache_set(CHART_CACHE, cache_key, image_bytes)
    return image_bytes

def get_price_keyboard(symbol: str) -> InlineKeyboardMarkup:
    """Generates the inline keyboard for timeframes and links."""
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
# Message Handlers
# ---------------------------------------------------------

@dp.message(F.text == "/test_emojis")
async def show_all_emojis(message: types.Message):
    """
    فرمان موقت برای رندر کردن آیدی‌های نامشخص و پیدا کردن نام آن‌ها
    """
    unknown_ids = [
        "5321041614443944130", "5319019251783212762", "5204021979174157518",
        "5204367105566193797", "5203973501878286332", "5203921507004201718",
        "5204298115506517373", "5203997304587040556", "5206210561364210906",
        "5204177620199027192", "5206384773827670642", "5204021042871286051",
        "5206338061763362251", "5204004198009551074", "5206292569469760905",
        "5204344763146320501", "5204418030993422385", "5204329022091180922",
        "5204173131958204067", "5204241323153961416"
    ]
    
    text = "🔍 **لیست تشخیص ایموجی‌ها:**\n\n"
    
    for i, eid in enumerate(unknown_ids, 1):
        text += f"{i}. <tg-emoji emoji-id='{eid}'>🪙</tg-emoji> ➔ <code>{eid}</code>\n"
        
    await message.reply(text, parse_mode="HTML")


@dp.message(F.entities | F.caption_entities)
async def extract_custom_emoji(message: types.Message):
    """
    Utility handler: Catch any message (or forwarded media with caption) 
    that contains custom emojis and reply with a list of all their IDs.
    """
    entities = message.caption_entities if message.photo or message.document else message.entities
    full_text = message.caption if message.photo or message.document else message.text
    
    found_emojis = []
    
    if entities and full_text:
        for entity in entities:
            if entity.type == "custom_emoji":
                emoji_char = full_text[entity.offset : entity.offset + entity.length]
                entry = f"ایموجی: {emoji_char} ➔ آیدی: `{entity.custom_emoji_id}`"
                
                if entry not in found_emojis:
                    found_emojis.append(entry)
            
    if found_emojis:
        response_text = "✨ **ایموجی‌های یافت شده در این پیام:**\n\n" + "\n\n".join(found_emojis)
        await message.reply(response_text, parse_mode="Markdown")
        return
        
    text = message.text.strip().upper() if message.text else ""
    if text.isalnum() and len(text) <= 10:
        await handle_ticker_input(message)

@dp.message(F.text)
async def handle_ticker_input(message: types.Message):
    """
    Listens to any text message. Treats short, alphanumeric text as a crypto ticker.
    """
    text = message.text.strip().upper()
    
    if not text.isalnum() or len(text) > 10:
        return
        
    symbol = text
    processing_msg = await message.reply("⏳ Fetching data...")

    # نکته‌ی کلیدی سرعت: قبلاً ابتدا قیمت و بعد (کاملاً جداگانه) چارت
    # fetch/render می‌شد -> زمان پاسخ تقریباً برابر مجموع این دو بود.
    # حالا هر دو به‌صورت موازی شروع می‌شن و زمان پاسخ تقریباً برابر
    # طولانی‌ترین یکی از این دو عملیات می‌شه.
    price_task = asyncio.create_task(get_price_data_cached(symbol))
    chart_task = asyncio.create_task(generate_chart_image(symbol, "1h"))

    data = await price_task

    if "error" in data:
        chart_task.cancel()
        await processing_msg.edit_text("❌ Symbol not found on SuperEx or Binance.")
        return

    # Formatting the caption
    caption = (
        f"🪙 **{data['symbol']}**\n"
        f"💰 **P:** ${data['price']}\n"
        f"📉 **24h:** {data['change_24h']}%\n\n"
        f"📈 **H:** ${data['high']}\n"
        f"📉 **L:** ${data['low']}\n"
        f"📊 **Vol:** {data['volume']} USDT\n"
    )
    
    if data.get("source") != "SuperEx":
        caption += f"\n🌐 Source: {data['source']} Fallback"

    try:
        chart_bytes = await chart_task
        photo = BufferedInputFile(chart_bytes, filename=f"{symbol}_chart.png")

        # نکته‌ی مهم: متدهای aiogram (مثل reply_photo/delete) یک شیء
        # TelegramMethod (مثل SendPhoto) برمی‌گردونن، نه یک coroutine واقعی.
        # این شیء awaitable هست ولی asyncio.create_task/gather فقط
        # coroutine یا Task/Future واقعی قبول می‌کنن، در نتیجه پیچیدن این
        # متدها با create_task یا gather خطا می‌ده. پس این دو تا فراخوانی
        # رو ساده و پشت‌سرهم await می‌کنیم (سودِ موازی‌سازی‌شون هم ناچیز بود).
        await message.reply_photo(
            photo=photo,
            caption=caption,
            parse_mode="Markdown",
            reply_markup=get_price_keyboard(symbol)
        )
        await processing_msg.delete()
    except Exception as e:
        logging.error(f"Chart generation error: {e}")
        await message.reply(caption + "\n\n*(Chart unavailable)*", parse_mode="Markdown")
        await processing_msg.delete()

@dp.callback_query(ChartCallback.filter())
async def process_chart_timeframe(query: types.CallbackQuery, callback_data: ChartCallback):
    """
    Handles inline button clicks to change the chart timeframe.
    """
    symbol = callback_data.symbol
    timeframe = callback_data.timeframe
    
    await query.answer(f"Loading {timeframe} chart...")
    
    try:
        # اگر همین نماد/تایم‌فریم به‌تازگی رندر شده باشه (مثلاً کاربر
        # سریع چند بار کلیک کنه)، از کش برمی‌گرده و تقریباً آنی جواب می‌ده.
        chart_bytes = await generate_chart_image(symbol, timeframe)
        new_photo = types.InputMediaPhoto(
            media=BufferedInputFile(chart_bytes, filename=f"{symbol}_{timeframe}.png"),
            caption=query.message.caption,
            parse_mode="Markdown"
        )
        
        await query.message.edit_media(
            media=new_photo,
            reply_markup=get_price_keyboard(symbol)
        )
    except Exception as e:
        logging.error(f"Error updating chart: {e}")
        await query.answer("Failed to update chart.", show_alert=True)

# ---------------------------------------------------------
# Web Server Setup (For Render)
# ---------------------------------------------------------
async def health_check(request):
    """HTTP endpoint to keep the bot alive on Render."""
    return web.Response(text="SuperEx Bot is Running smoothly!")

async def main():
    """Starts the web server and the bot polling."""
    app = web.Application()
    app.router.add_get('/', health_check)
    
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', PORT)
    
    logging.info(f"🌐 Web server starting on port {PORT}")
    await site.start()

    # حذف هرگونه webhook فعال + دور ریختن آپدیت‌های عقب‌مانده قبل از شروع polling.
    # این کار باعث می‌شه اگه یه نمونه‌ی قبلی از ربات (مثلاً باقی‌مونده‌ی یک
    # دیپلوی قبلی روی Render که هنوز کامل shutdown نشده) هنوز به تلگرام
    # وصل بود، برخورد و خطای TelegramConflictError به حداقل برسه.
    await bot.delete_webhook(drop_pending_updates=True)

    # فقط update های واقعاً استفاده‌شده (پیام و کلیک روی دکمه) پردازش می‌شن؛
    # این باعث کاهش overhead پردازش update های غیرضروری در long polling می‌شه.
    logging.info("🚀 Bot polling started")
    try:
        await dp.start_polling(bot, allowed_updates=["message", "callback_query"])
    finally:
        CHART_RENDER_EXECUTOR.shutdown(wait=False, cancel_futures=True)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logging.info("🛑 Bot stopped gracefully.")
