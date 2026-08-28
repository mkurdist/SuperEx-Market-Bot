import asyncio
import os
import io
import logging
import aiohttp
import pandas as pd
import mplfinance as mpf
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

# Cache for symbol to currencyId mapping
SYMBOL_MAP_CACHE = {}

# Timeframe mapping for SuperEx API
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
async def get_currency_id(symbol: str) -> str:
    """
    Fetches the currencyId for a given symbol from SuperEx.
    Uses memory caching to avoid redundant API calls.
    """
    base_symbol = symbol.upper().replace("_USDT", "").replace("USDT", "")
    
    if base_symbol in SYMBOL_MAP_CACHE:
        return SYMBOL_MAP_CACHE[base_symbol]
        
    url = "https://api.superexchang.com/free-spot/v3/public/symbols"
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(url) as response:
                if response.status == 200:
                    data = await response.json()
                    for item in data.get("data", []):
                        currency = item.get("currency", "").upper()
                        currency_id = item.get("currencyId")
                        if currency and currency_id:
                            SYMBOL_MAP_CACHE[currency] = currency_id
                    
                    return SYMBOL_MAP_CACHE.get(base_symbol)
        except Exception as e:
            logging.error(f"Error fetching symbols mapping: {e}")
            
    return None

async def fetch_coingecko_fallback(symbol: str) -> dict:
    """
    Fallback to CoinGecko API if the symbol is not available on SuperEx.
    """
    base_symbol = symbol.lower().replace("_usdt", "").replace("usdt", "")
    search_url = f"https://api.coingecko.com/api/v3/search?query={base_symbol}"
    
    async with aiohttp.ClientSession() as session:
        try:
            # 1. Search for the coin ID
            async with session.get(search_url) as search_res:
                if search_res.status == 200:
                    search_data = await search_res.json()
                    coins = search_data.get("coins", [])
                    if not coins:
                        return {"error": "Symbol not found on CoinGecko."}
                    
                    coin_id = coins[0].get("id")
                    
                    # 2. Fetch price data
                    price_url = f"https://api.coingecko.com/api/v3/simple/price?ids={coin_id}&vs_currencies=usd&include_24hr_change=true&include_24hr_vol=true"
                    async with session.get(price_url) as price_res:
                        if price_res.status == 200:
                            price_data = await price_res.json()
                            coin_data = price_data.get(coin_id, {})
                            
                            return {
                                "symbol": base_symbol.upper(),
                                "price": str(coin_data.get("usd", "0.0")),
                                "change_24h": str(round(coin_data.get("usd_24h_change", 0.0), 2)),
                                "high": "N/A",
                                "low": "N/A",
                                "volume": str(coin_data.get("usd_24h_vol", "0.0")),
                                "source": "CoinGecko"
                            }
        except Exception as e:
            logging.error(f"CoinGecko fallback error: {e}")
            
    return {"error": "Fallback failed."}

async def fetch_price_data(symbol: str) -> dict:
    """
    Fetches the latest 24h ticker data from SuperEx.
    Triggers fallback if symbol is missing.
    """
    currency_id = await get_currency_id(symbol)
    
    if not currency_id:
        logging.warning(f"Symbol {symbol} not found on SuperEx. Trying fallback...")
        return await fetch_coingecko_fallback(symbol)

    url = f"https://api.superexchang.com/free-spot/v3/market/twentyfour?currencyId={currency_id}"
    
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(url) as response:
                if response.status == 200:
                    res_data = await response.json()
                    return {
                        "symbol": symbol.upper(),
                        "price": res_data.get("hqzrsp", "0.0"),
                        "change_24h": res_data.get("change", "0.0"),
                        "high": res_data.get("maxPrice", "0.0"),
                        "low": res_data.get("minPrice", "0.0"),
                        "volume": res_data.get("sumAmount", "0.0"),
                        "source": "SuperEx"
                    }
        except Exception as e:
            logging.error(f"Error fetching ticker for {symbol}: {e}")
            
    return {"error": "Failed to fetch data."}

async def generate_chart_image(symbol: str, timeframe: str) -> bytes:
    """
    Fetches real Kline (OHLCV) data from SuperEx and generates 
    a professional candlestick chart using mplfinance.
    """
    currency_id = await get_currency_id(symbol)
    if not currency_id:
        raise ValueError("Currency ID not found for chart generation.")

    time_type = TIMEFRAME_MAP.get(timeframe, 3600)
    url = f"https://api.superexchang.com/free-spot/v3/klines?currencyId={currency_id}&timeType={time_type}&limit=60"
    
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            if response.status != 200:
                raise ConnectionError("Failed to fetch kline data.")
            
            raw_data = await response.json()
            parsed_data = []
            for item in raw_data.get("data", []):
                parts = item.split(",")
                parsed_data.append({
                    "Date": pd.to_datetime(int(parts[0]), unit="ms"),
                    "High": float(parts[1]),
                    "Open": float(parts[2]),
                    "Low": float(parts[3]),
                    "Close": float(parts[4]),
                    "Volume": float(parts[5])
                })

    if not parsed_data:
        raise ValueError("No chart data available.")

    df = pd.DataFrame(parsed_data)
    df.set_index("Date", inplace=True)
    df.sort_index(inplace=True)

    # Styling the chart (Dark mode)
    mc = mpf.make_marketcolors(up='#00ff00', down='#ff0000', edge='inherit', wick='inherit', volume='in', ohlc='i')
    s = mpf.make_mpf_style(marketcolors=mc, base_mpf_style='nightclouds', facecolor='#1e1e1e', edgecolor='#444444', figcolor='#121212')

    buf = io.BytesIO()
    mpf.plot(
        df, 
        type='candle', 
        style=s, 
        volume=False, 
        title=f"\n{symbol.upper()}/USDT | {timeframe}",
        tight_layout=True,
        savefig=dict(fname=buf, dpi=100, bbox_inches='tight')
    )
    
    buf.seek(0)
    image_bytes = buf.getvalue()
    buf.close()
    
    return image_bytes

# ---------------------------------------------------------
# Keyboards
# ---------------------------------------------------------
def get_price_keyboard(symbol: str) -> InlineKeyboardMarkup:
    """Generates the inline keyboard for timeframes and links."""
    url_register = "https://app.superex.live/register?invitationCode=VQK2N6DDS"
    url_group = "https://t.me/SuperexIR"
    
    keyboard = [
        [
            InlineKeyboardButton(text="1m", callback_data=ChartCallback(symbol=symbol, timeframe="1m").pack()),
            InlineKeyboardButton(text="15m", callback_data=ChartCallback(symbol=symbol, timeframe="15m").pack()),
            InlineKeyboardButton(text="1h", callback_data=ChartCallback(symbol=symbol, timeframe="1h").pack()),
            InlineKeyboardButton(text="4h", callback_data=ChartCallback(symbol=symbol, timeframe="4h").pack()),
            InlineKeyboardButton(text="1d", callback_data=ChartCallback(symbol=symbol, timeframe="1d").pack()),
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
        await processing_msg.edit_text("❌ Symbol not found on SuperEx or CoinGecko.")
        return

    # Formatting the caption exactly as requested
    caption = (
        f"🪙 **{data['symbol']}**\n"
        f"💰 **P:** ${data['price']}\n"
        f"📉 **24h:** {data['change_24h']}%\n\n"
    )
    
    if data.get("source") == "SuperEx":
        caption += (
            f"📈 **H:** ${data['high']}\n"
            f"📉 **L:** ${data['low']}\n"
            f"📊 **Vol:** {data['volume']} USDT\n"
        )
    else:
        caption += f"🌐 Source: CoinGecko Fallback\n"

    try:
        # Generate initial 1h chart only if source is SuperEx
        if data.get("source") == "SuperEx":
            chart_bytes = await generate_chart_image(symbol, "1h")
            photo = BufferedInputFile(chart_bytes, filename=f"{symbol}_chart.png")
            
            await message.reply_photo(
                photo=photo,
                caption=caption,
                parse_mode="Markdown",
                reply_markup=get_price_keyboard(symbol)
            )
        else:
            await message.reply(caption, parse_mode="Markdown")
            
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
