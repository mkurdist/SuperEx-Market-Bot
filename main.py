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
CHART_RENDER_EXECUTOR = ThreadPoolExecutor(
    max_workers=max(2, (os.cpu_count() or 2)),
    thread_name_prefix="chart-render"
)

CHART_RENDER_SEMAPHORE = asyncio.Semaphore(int(os.getenv("MAX_CONCURRENT_CHARTS", "4")))

CHART_CACHE_TTL = 5  
CHART_CACHE: dict = {}

PRICE_CACHE_TTL = 3  
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
    up='#00d964',      
    down='#ff3b3b',    
    edge='inherit',
    wick='inherit',
    volume='in',
    ohlc='i'
)

CHART_STYLE = mpf.make_mpf_style(
    marketcolors=_MARKET_COLORS,
    base_mpf_style='binance',  
    facecolor='#000000',   
    edgecolor='#555555',   
    figcolor='#000000',
    gridcolor='#222222',   
    gridstyle='--',
    y_on_right=False,      
    rc={
        'font.family': 'sans-serif',
        'axes.titleweight': 'normal',   
        'axes.titlesize': 13,
        'axes.titlecolor': '#e6e6e6',
        'axes.labelcolor': '#cfcfcf',   
        'xtick.color': '#9a9a9a',
        'ytick.color': '#9a9a9a',
        'text.color': '#9a9a9a',        
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
# API Helper Functions
# ---------------------------------------------------------
def get_superex_headers() -> dict:
    """
    Generates dynamic security headers required by SuperEx API.
    Added 'language' and 'application/json' for REST API compatibility.
    """
    return {
        "accept": "application/json",
        "language": "en",
        "client": "1",
        "nonce": uuid.uuid4().hex,
        "timestamp": str(int(time.time() * 1000)),
        "token": "",
        "content-type": "application/x-www-form-urlencoded"
    }

async def fetch_price_data(symbol: str) -> dict:
    """
    Fetches the latest 24h ticker data directly using the new SuperEx resource API.
    No external fallbacks are used.
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
            
    logging.warning(f"Symbol {symbol} not found on SuperEx.")
    return {"error": "Symbol not found on SuperEx."}

async def get_currency_id(symbol: str) -> int:
    """
    Fetches the internal currencyId for a given symbol from SuperEx.
    Uses a robust multi-key search to prevent JSON parsing errors.
    """
    base_symbol = symbol.lower().replace("_usdt", "").replace("usdt", "")
    url = f"https://api.superexchang.com/api/free-spot/v3/symbols?currency={base_symbol}"
    
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(url, headers=get_superex_headers(), timeout=5.0) as response:
                if response.status == 200:
                    res_json = await response.json()
                    data = res_json.get("data")
                    
                    # 1. Locate the actual list of items
                    items = []
                    if isinstance(data, list):
                        items = data
                    elif isinstance(data, dict):
                        # Try to find the list in common nested keys
                        for key in ["list", "items", "rows", "data"]:
                            if isinstance(data.get(key), list):
                                items = data[key]
                                break
                    
                    # 2. Iterate through items and find the matching currency
                    for item in items:
                        if not isinstance(item, dict):
                            continue
                        
                        # Check multiple possible keys for the symbol name
                        candidates = [
                            str(item.get("currency", "")),
                            str(item.get("currencyName", "")),
                            str(item.get("name", "")),
                            str(item.get("symbol", ""))
                        ]
                        
                        for candidate in candidates:
                            if candidate.lower() == base_symbol:
                                cid = item.get("currencyId")
                                if cid is not None:
                                    return int(cid)
                                    
                    # 3. Log a snippet of the response if the symbol is not found for debugging
                    logging.warning(f"Symbol {base_symbol} not found in parsed items. Raw data snippet: {str(data)[:200]}")
        except Exception as e:
            logging.error(f"Error fetching currencyId: {e}")
            
    return 0

async def fetch_superex_kline_rest(symbol: str, timeframe: str) -> list:
    """
    Fetches Kline data via SuperEx official REST API.
    Replaces the unstable WebSocket approach without using any external fallbacks.
    """
    base_symbol = symbol.lower().replace("_usdt", "").replace("usdt", "")
    currency_id = await get_currency_id(base_symbol)
    
    if not currency_id:
        logging.error(f"Could not find currencyId for {base_symbol}")
        return []
        
    tf_seconds = TIMEFRAME_MAP.get(timeframe, 3600)
    url = f"https://api.superexchang.com/api/free-spot/v3/klines?currencyId={currency_id}&timeType={tf_seconds}&limit=60"
    
    parsed_data = []
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=get_superex_headers(), timeout=5.0) as response:
                if response.status == 200:
                    res_json = await response.json()
                    klines = res_json.get("data", [])
                    
                    # Data format: timestamp, high, open, low, close, volume
                    for item in klines:
                        if isinstance(item, list) and len(item) >= 6:
                            t, h, o, l, c, v = item[0], item[1], item[2], item[3], item[4], item[5]
                            parsed_data.append({
                                "Date": pd.to_datetime(int(t), unit="ms"),
                                "Open": float(o),
                                "High": float(h),
                                "Low": float(l),
                                "Close": float(c),
                                "Volume": float(v)
                            })
    except Exception as e:
        logging.error(f"SuperEx REST Kline error: {e}")
        
    return parsed_data

# ---------------------------------------------------------
# Cached wrappers around the exchange query methods
# ---------------------------------------------------------
async def get_price_data_cached(symbol: str) -> dict:
    """
    اگر همین نماد در چند ثانیه‌ی اخیر استعلام شده باشه، از کش برمی‌گرده.
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
def _render_chart_sync(df: pd.DataFrame, symbol: str, timeframe: str) -> bytes:
    """
    بخش سنگین و CPU-bound رسم چارت (matplotlib).
    """
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
        title=f"\n{symbol.upper().replace('USDT', '')}/USDT | {timeframe}",
        ylabel='Price (USDT)',   
        datetime_format=date_format,
        xrotation=x_rotation,
        tight_layout=False,
        returnfig=True,
        figsize=(12, 7.4)   
    )

    fig.subplots_adjust(top=0.90, bottom=0.16, left=0.09, right=0.96)

    for ax in axlist:
        for spine in ax.spines.values():
            spine.set_visible(True)
            spine.set_color('#555555')
            spine.set_linewidth(0.8)

    watermark_text = "Created by @SuperExFa_bot | @SuperexIR"
    fig.text(
        0.5, 0.025, watermark_text,
        ha='center', va='center', fontsize=10.5, color='#9a9a9a', fontweight='normal',
        transform=fig.transFigure
    )

    buf = io.BytesIO()
    try:
        fig.savefig(
            buf, dpi=130,
            bbox_inches=None, pad_inches=0,
            facecolor=fig.get_facecolor(), edgecolor='none'
        )
        return buf.getvalue()
    finally:
        buf.close()
        plt.close(fig)


async def generate_chart_image(symbol: str, timeframe: str) -> bytes:
    """
    Generates a professional, TradingView-style candlestick chart.
    """
    cache_key = (symbol.upper(), timeframe)
    cached = _cache_get(CHART_CACHE, cache_key, CHART_CACHE_TTL)
    if cached is not None:
        return cached

    # Use REST API instead of WebSocket
    parsed_data = await fetch_superex_kline_rest(symbol, timeframe)
    
    if not parsed_data:
        raise ValueError("No chart data available from SuperEx.")

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

    price_task = asyncio.create_task(get_price_data_cached(symbol))
    chart_task = asyncio.create_task(generate_chart_image(symbol, "1h"))

    data = await price_task

    if "error" in data:
        chart_task.cancel()
        await processing_msg.edit_text("❌ Symbol not found on SuperEx.")
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

    try:
        chart_bytes = await chart_task
        photo = BufferedInputFile(chart_bytes, filename=f"{symbol}_chart.png")

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

    await bot.delete_webhook(drop_pending_updates=True)

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
