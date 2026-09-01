import asyncio
import os
import io
import time
import uuid
import json
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
from dotenv import load_dotenv

# ---------------------------------------------------------
# ماژول ایزوله ۳ قابلیت جدید (طلا/سکه، دلار/تتر، ماشین‌حساب کریپتو)
# تمام منطق این ۳ قابلیت داخل tools.py است و از طریق یک روتر واحد
# (tools_router) به دیسپچر متصل می‌شود - بدون هیچ دخالتی در بخش‌های زیر.
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

TIMEFRAME_MAP = {
    "1m": "1m",
    "15m": "15m",
    "1h": "1h",
    "4h": "4h",
    "1d": "1d"
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
    symbol: str
    timeframe: str

# ---------------------------------------------------------
# API Helper Functions
# ---------------------------------------------------------
def get_superex_headers() -> dict:
    return {
        "accept": "*/*",
        "accept-language": "en",
        "client": "1",
        "nonce": uuid.uuid4().hex,
        "timestamp": str(int(time.time() * 1000)),
        "token": "",
        "content-type": "application/x-www-form-urlencoded"
    }

async def fetch_price_data(symbol: str) -> dict:
    base_symbol = symbol.lower().replace("_usdt", "").replace("usdt", "")
    url = f"https://api.superexchang.com/resource/v3/public/currency/new?currency={base_symbol}"
    
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(url, headers=get_superex_headers()) as response:
                if response.status == 200:
                    res_data = await response.json()
                    data_obj = res_data.get("data", {})
                    
                    if data_obj and data_obj.get("newPrice"):
                        price = float(data_obj.get("newPrice", "0.0"))
                        sum_number = float(data_obj.get("sumNumber", "0.0"))
                        
                        volume_usdt = price * sum_number
                        
                        if volume_usdt >= 1000:
                            formatted_vol = f"{volume_usdt:,.2f}"
                        else:
                            formatted_vol = f"{volume_usdt:.4f}"

                        return {
                            "symbol": base_symbol.upper(),
                            "price": str(data_obj.get("newPrice", "0.0")),
                            "change_24h": str(data_obj.get("change", "0.0")),
                            "high": str(data_obj.get("maxPrice", "0.0")),
                            "low": str(data_obj.get("minPrice", "0.0")),
                            "volume": formatted_vol,
                            "source": "SuperEx"
                        }
        except Exception as e:
            logging.error(f"Error fetching ticker for {symbol}: {e}")
            
    logging.warning(f"Symbol {symbol} not found on SuperEx.")
    return {"error": "Symbol not found on SuperEx."}

async def fetch_binance_kline(symbol: str, timeframe: str) -> list:
    base_symbol = symbol.lower().replace("_usdt", "").replace("usdt", "")
    binance_symbol = f"{base_symbol.upper()}USDT"
    
    interval = TIMEFRAME_MAP.get(timeframe, "1h")
    url = f"https://data-api.binance.vision/api/v3/klines?symbol={binance_symbol}&interval={interval}&limit=60"
    
    parsed_data = []
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(url, timeout=5.0) as response:
                if response.status == 200:
                    items = await response.json()
                    if isinstance(items, list) and items:
                        for item in items:
                            t, o, h, l, c, v = item[0], item[1], item[2], item[3], item[4], item[5]
                            parsed_data.append({
                                "Date": pd.to_datetime(int(t), unit="ms"),
                                "Open": float(o),
                                "High": float(h),
                                "Low": float(l),
                                "Close": float(c),
                                "Volume": float(v)
                            })
        except Exception as e:
            logging.error(f"Binance Kline error for {symbol}: {e}")
            
    return parsed_data

async def fetch_coingecko_market_chart(symbol: str) -> list:
    base_symbol = symbol.lower().replace("_usdt", "").replace("usdt", "")
    search_url = f"https://api.coingecko.com/api/v3/search?query={base_symbol}"
    
    parsed_data = []
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(search_url, timeout=5.0) as resp:
                if resp.status == 200:
                    search_data = await resp.json()
                    coins = search_data.get("coins", [])
                    if coins:
                        coin_id = coins[0].get("id")
                        chart_url = f"https://api.coingecko.com/api/v3/coins/{coin_id}/market_chart?vs_currency=usd&days=1"
                        async with session.get(chart_url, timeout=5.0) as chart_resp:
                            if chart_resp.status == 200:
                                chart_data = await chart_resp.json()
                                prices = chart_data.get("prices", [])
                                volumes = chart_data.get("total_volumes", [])
                                
                                for i, p_item in enumerate(prices):
                                    t, price = p_item[0], p_item[1]
                                    vol = volumes[i][1] if i < len(volumes) else 0.0
                                    parsed_data.append({
                                        "Date": pd.to_datetime(int(t), unit="ms"),
                                        "Open": float(price),
                                        "High": float(price),
                                        "Low": float(price),
                                        "Close": float(price),
                                        "Volume": float(vol)
                                    })
        except Exception as e:
            logging.error(f"CoinGecko fallback error for {symbol}: {e}")
            
    return parsed_data

async def fetch_kline_with_fallback(symbol: str, timeframe: str) -> list:
    data = await fetch_binance_kline(symbol, timeframe)
    if data:
        return data
        
    logging.info(f"Binance failed for {symbol}, falling back to CoinGecko...")
    data = await fetch_coingecko_market_chart(symbol)
    return data

async def get_price_data_cached(symbol: str) -> dict:
    cache_key = symbol.upper()
    cached = _cache_get(PRICE_CACHE, cache_key, PRICE_CACHE_TTL)
    if cached is not None:
        return cached

    data = await fetch_price_data(symbol)
    if "error" not in data:
        _cache_set(PRICE_CACHE, cache_key, data)
    return data

def _render_chart_sync(df: pd.DataFrame, symbol: str, timeframe: str) -> bytes:
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
        ylabel='Price (USDT)',   
        datetime_format=date_format,
        xrotation=x_rotation,
        tight_layout=True,
        returnfig=True,
        figsize=(10, 6)   
    )

    ax = axlist[0]
    ax.set_title(ax.get_title(), pad=10, fontsize=13, color='#e6e6e6')

    watermark_text = "Credit by SuperEx"
    ax.text(
        0.5, 0.03, watermark_text,
        transform=ax.transAxes,
        ha='center', va='bottom', fontsize=9.5, color='#9a9a9a', fontweight='normal'
    )

    buf = io.BytesIO()
    try:
        fig.savefig(
            buf, dpi=130,
            bbox_inches='tight', pad_inches=0.1,
            facecolor=fig.get_facecolor(), edgecolor='none'
        )
        return buf.getvalue()
    finally:
        buf.close()
        plt.close(fig)

async def generate_chart_image(symbol: str, timeframe: str) -> bytes:
    cache_key = (symbol.upper(), timeframe)
    cached = _cache_get(CHART_CACHE, cache_key, CHART_CACHE_TTL)
    if cached is not None:
        return cached

    parsed_data = await fetch_kline_with_fallback(symbol, timeframe)
    
    if not parsed_data:
        raise ValueError("No chart data available from any API provider.")

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
# تشخیص هوشمند نماد کریپتو (Ticker Detection) - بازنویسی‌شده
# ---------------------------------------------------------
# چرا این تابع لازم است؟
# ربات در گروه‌های شلوغ حضور دارد و هندلر تیکر قبلی صرفاً با یک ریجکس
# ساده (۲ تا ۱۰ حرف انگلیسی) هر پیامی مثل "hi"، "ok"، "buy"، "admin" و
# همچنین کلیدواژه‌های قابلیت‌های جدید مثل "gold"، "dollar"، "usdt" را هم
# به‌عنوان نماد کریپتو تفسیر می‌کرد. این هم باعث اسپم پیام خطا در گروه
# می‌شد و هم باعث تداخل با قابلیت‌های جدید طلا/دلار در tools.py می‌شد
# (چون هندلرهای خود dp زودتر از روترهای include شده بررسی می‌شوند).
#
# راه‌حل: یک Blocklist از کلمات پرکاربرد انگلیسی + کلیدواژه‌های
# اختصاصی قابلیت‌های جدید، به همراه یک تابع اعتبارسنجی مرکزی.
COMMON_WORDS_BLOCKLIST = {
    # کلمات محاوره‌ای/عمومی رایج در گروه‌ها
    "hi", "hello", "hey", "yo", "sup", "ok", "okay", "yes", "no", "yep", "nope",
    "bye", "please", "thanks", "thank", "welcome", "sorry", "lol", "lmao", "wow",
    "nice", "cool", "good", "bad", "great", "ok?", "test", "help", "info", "menu",
    # کلمات مرتبط با نقش/مدیریت گروه
    "admin", "admins", "mod", "mods", "owner", "dev", "team", "support", "staff",
    "report", "spam", "ban", "kick", "mute", "bot", "bots",
    # کلمات معاملاتی عمومی (که خودشان نماد نیستند)
    "buy", "sell", "sold", "pump", "dump", "moon", "scam", "fake", "real", "up", "down",
    "long", "short", "hold", "hodl", "new", "old", "price", "chart", "link", "join",
    "group", "channel", "start", "stop",
    # کلیدواژه‌های اختصاصی قابلیت‌های جدید (طلا/دلار) که در tools.py مدیریت می‌شوند
    "gold", "dollar", "usdt", "tether",
}


def is_valid_ticker_symbol(text: str) -> bool:
    """
    بررسی می‌کند که آیا متن ورودی یک نماد کریپتوی معتبر و بی‌خطر برای
    استعلام از SuperEx است یا نه. متن باید:
      - فقط شامل حروف انگلیسی (۲ تا ۱۰ کاراکتر) باشد (فارسی هرگز match نمی‌شود)
      - داخل لیست سیاه کلمات رایج/کلیدواژه‌های اختصاصی نباشد
    """
    if not text:
        return False
    cleaned = text.strip()
    if not re.match(r"^[a-zA-Z]{2,10}$", cleaned):
        return False
    if cleaned.lower() in COMMON_WORDS_BLOCKLIST:
        return False
    return True


# ---------------------------------------------------------
# Message Handlers
# ---------------------------------------------------------

@dp.message(F.text == "/test_emojis")
async def show_all_emojis(message: types.Message):
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
        
    text = message.text.strip() if message.text else ""
    # فقط نمادهای معتبر کریپتو (بدون تداخل با کلمات رایج/کلیدواژه‌های طلا و دلار)
    if is_valid_ticker_symbol(text):
        await handle_ticker_input(message)

# فیلتر هوشمند: فقط نمادهای کریپتوی معتبر (مثلاً BTC، ETH) - نه کلمات رایج، نه کلیدواژه‌های تولز
@dp.message(lambda message: is_valid_ticker_symbol(message.text or ""))
async def handle_ticker_input(message: types.Message):
    symbol = message.text.strip().upper()
    processing_msg = await message.reply("⏳ Fetching data...")

    price_task = asyncio.create_task(get_price_data_cached(symbol))
    chart_task = asyncio.create_task(generate_chart_image(symbol, "1h"))

    data = await price_task

    if "error" in data:
        chart_task.cancel()
        # Silent Fail در گروه‌های شلوغ - فقط در چت خصوصی خطا نمایش داده می‌شود
        if message.chat.type == "private":
            await processing_msg.edit_text("❌ Symbol not found on SuperEx.")
        else:
            try:
                await processing_msg.delete()
            except Exception:
                pass
        return

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
# اتصال روتر ۳ قابلیت جدید (طلا/سکه، دلار/تتر، ماشین‌حساب کریپتو)
# ---------------------------------------------------------
# نکته: هندلرهای بالا مستقیماً روی dp ثبت شده‌اند و طبق رفتار aiogram
# زودتر از روترهای include شده بررسی می‌شوند. به همین دلیل Blocklist
# بالا (COMMON_WORDS_BLOCKLIST) تضمین می‌کند که پیام‌های حاوی
# "gold"/"dollar"/"usdt" هرگز توسط handle_ticker_input مصرف نشوند و
# سالم به tools_router برسند. برای متون فارسی (طلا، سکه، دلار، تتر)
# اصلاً تداخلی وجود ندارد چون ریجکس تیکر فقط حروف لاتین را می‌پذیرد.
dp.include_router(tools_router)

# ---------------------------------------------------------
# Web Server Setup (For Render)
# ---------------------------------------------------------
async def health_check(request):
    return web.Response(text="SuperEx Bot is Running smoothly!")

async def main():
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
