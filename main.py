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

from aiohttp import web
from aiogram import Bot, Dispatcher, types, F
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
    
    # Using Binance's public data endpoint which doesn't block Cloud servers (Render)
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
    No currencyId mapping needed!
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
    Includes browser headers to bypass Cloudflare 200 OK blocks.
    """
    ws_url = "wss://api.superexchang.com/ws"
    tf_seconds = TIMEFRAME_MAP.get(timeframe, 3600)
    base_symbol = symbol.lower().replace("_usdt", "").replace("usdt", "")
    topic = f"spot/candle{tf_seconds}:{base_symbol}_usdt"
    
    # Headers to bypass Cloudflare WAF
    ws_headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Origin": "https://www.superex.com"
    }
    
    parsed_data = []
    try:
        async with aiohttp.ClientSession() as session:
            # Passing the headers to the ws_connect method
            async with session.ws_connect(ws_url, headers=ws_headers, timeout=5.0) as ws:
                req_msg = {"op": "req", "action": "action", "args": [topic], "to": 300}
                await ws.send_json(req_msg)
                
                # Listen for the response (up to 10 messages to skip pings)
                for _ in range(10):
                    msg = await ws.receive(timeout=3.0)
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        if msg.data == 'ping':
                            await ws.send_str('pong')
                            continue
                        
                        try:
                            # Fix Base64 padding if necessary
                            b64_data = msg.data + "=" * ((4 - len(msg.data) % 4) % 4)
                            # Decode Base64 -> Decompress GZIP -> Parse JSON
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

async def generate_chart_image(symbol: str, timeframe: str) -> bytes:
    """
    Fetches real Kline (OHLCV) data from SuperEx WS (with Binance fallback)
    and generates a professional candlestick chart with a custom watermark.
    """
    parsed_data = await fetch_superex_kline_ws(symbol, timeframe)
    
    # Trigger Binance fallback if WebSocket failed or returned empty
    if not parsed_data:
        logging.warning(f"SuperEx WS failed for {symbol} chart. Using Binance fallback...")
        parsed_data = await fetch_binance_kline_fallback(symbol, timeframe)

    if not parsed_data:
        raise ValueError("No chart data available from any source.")

    df = pd.DataFrame(parsed_data)
    df.set_index("Date", inplace=True)
    df.sort_index(inplace=True)

    # Styling the chart
    mc = mpf.make_marketcolors(up='#00ff00', down='#ff0000', edge='inherit', wick='inherit', volume='in', ohlc='i')
    s = mpf.make_mpf_style(marketcolors=mc, base_mpf_style='nightclouds', facecolor='#1e1e1e', edgecolor='#444444', figcolor='#121212')

    buf = io.BytesIO()
    
    # Capture the figure and axes to add custom elements (like text) before saving
    fig, axlist = mpf.plot(
        df, 
        type='candle', 
        style=s, 
        volume=False, 
        title=f"\n{symbol.upper().replace('USDT', '')}/USDT | {timeframe}",
        tight_layout=True,
        returnfig=True  # این پارامتر اجازه می‌دهد روی چارت تغییرات دستی بدهیم
    )
    
    # ---------------------------------------------------------
    # اضافه کردن امضا (Credit/Watermark) زیر چارت
    # ---------------------------------------------------------
    watermark_text = "Created by @SuperExFa_bot | @SuperexIR"
    
    # قرار دادن متن در مرکز (0.5) و پایین (-0.05) کادر چارت
    fig.text(0.5, -0.03, watermark_text, ha='center', va='center', fontsize=11, color='#888888')
    
    # Save the modified figure to the buffer
    fig.savefig(buf, dpi=100, bbox_inches='tight', facecolor=fig.get_facecolor())
    
    buf.seek(0)
    image_bytes = buf.getvalue()
    buf.close()
    
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
    # لیستی از آیدی‌هایی که فرستادی
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
        # ساخت تگ HTML برای رندر ایموجی پرمیوم در تلگرام
        text += f"{i}. <tg-emoji emoji-id='{eid}'>🪙</tg-emoji> ➔ <code>{eid}</code>\n"
        
    await message.reply(text, parse_mode="HTML")


@dp.message(F.entities | F.caption_entities)
async def extract_custom_emoji(message: types.Message):
    """
    Utility handler: Catch any message (or forwarded media with caption) 
    that contains custom emojis and reply with a list of all their IDs.
    Runs BEFORE the main ticker handler.
    """
    # Check if the message has a caption (like a forwarded photo) or is just text
    entities = message.caption_entities if message.photo or message.document else message.entities
    full_text = message.caption if message.photo or message.document else message.text
    
    found_emojis = []
    
    if entities and full_text:
        for entity in entities:
            if entity.type == "custom_emoji":
                emoji_char = full_text[entity.offset : entity.offset + entity.length]
                entry = f"ایموجی: {emoji_char} ➔ آیدی: `{entity.custom_emoji_id}`"
                
                # Prevent adding duplicate emojis to the list
                if entry not in found_emojis:
                    found_emojis.append(entry)
            
    if found_emojis:
        response_text = "✨ **ایموجی‌های یافت شده در این پیام:**\n\n" + "\n\n".join(found_emojis)
        await message.reply(response_text, parse_mode="Markdown")
        return
        
    # If no custom emojis were found, but it looks like a valid ticker, pass it down.
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
    
    data = await fetch_price_data(symbol)
    
    if "error" in data:
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
        chart_bytes = await generate_chart_image(symbol, "1h")
        photo = BufferedInputFile(chart_bytes, filename=f"{symbol}_chart.png")
        
        await message.reply_photo(
            photo=photo,
            caption=caption,
            parse_mode="Markdown",
            reply_markup=get_price_keyboard(symbol)
        )
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
    
    logging.info("🚀 Bot polling started")
    await dp.start_polling(bot)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logging.info("🛑 Bot stopped gracefully.")
