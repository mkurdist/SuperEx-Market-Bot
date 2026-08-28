import asyncio
import os
import io
import logging
from aiohttp import web
from aiogram import Bot, Dispatcher, types, F
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, BufferedInputFile
from aiogram.filters.callback_data import CallbackData
import matplotlib.pyplot as plt

# ---------------------------------------------------------
# Configuration & Setup
# ---------------------------------------------------------
BOT_TOKEN = os.getenv("BOT_TOKEN")
PORT = int(os.getenv("PORT", 8080)) # Default port for Render

logging.basicConfig(level=logging.INFO)

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# ---------------------------------------------------------
# Callback Data Factories
# ---------------------------------------------------------
class ChartCallback(CallbackData, prefix="chart"):
    """Callback factory for chart timeframe buttons."""
    symbol: str
    timeframe: str

# ---------------------------------------------------------
# API Helper Functions (Mocked for SuperEx & CoinGecko)
# ---------------------------------------------------------
async def fetch_price_data(symbol: str):
    """
    Fetches ticker data from SuperEx.
    If not found, fallbacks to CoinGecko.
    Returns a dictionary with price, 24h change, high, low, volume.
    """
    # TODO: Implement actual SuperEx REST API call here.
    # TODO: Implement CoinGecko fallback if SuperEx returns 404/Empty.
    
    # Mock data for demonstration
    return {
        "symbol": symbol.upper(),
        "price": "64,231.50",
        "change_24h": "+2.45",
        "high": "65,100.00",
        "low": "62,800.00",
        "volume": "1.2B"
    }

async def generate_chart_image(symbol: str, timeframe: str) -> bytes:
    """
    Fetches Kline (OHLCV) data from SuperEx based on timeframe,
    generates a simple line/candle chart using matplotlib, 
    and returns the image as bytes.
    """
    # TODO: Fetch real Kline data from SuperEx WebSocket or REST API
    
    # Generate a dummy plot for demonstration
    plt.figure(figsize=(6, 3), facecolor='#1e1e1e')
    ax = plt.axes()
    ax.set_facecolor('#1e1e1e')
    ax.tick_params(colors='white')
    for spine in ax.spines.values():
        spine.set_color('#333333')
        
    x = [1, 2, 3, 4, 5]
    y = [10, 15, 13, 18, 16] # Dummy price points
    
    plt.plot(x, y, color='#00ff00', linewidth=2)
    plt.title(f"{symbol.upper()} - {timeframe}", color='white', pad=10)
    plt.tight_layout()
    
    # Save plot to memory buffer
    buf = io.BytesIO()
    plt.savefig(buf, format='png', bbox_inches='tight')
    buf.seek(0)
    plt.close()
    
    return buf.getvalue()

# ---------------------------------------------------------
# Keyboards
# ---------------------------------------------------------
def get_price_keyboard(symbol: str) -> InlineKeyboardMarkup:
    """Generates the inline keyboard for timeframes and group/register links."""
    
    # Register & Group Links
    url_register = "https://app.superex.live/register?invitationCode=VQK2N6DDS"
    url_group = "https://t.me/SuperexIR"
    
    keyboard = [
        # Timeframe row
        [
            InlineKeyboardButton(text="1m", callback_data=ChartCallback(symbol=symbol, timeframe="1m").pack()),
            InlineKeyboardButton(text="15m", callback_data=ChartCallback(symbol=symbol, timeframe="15m").pack()),
            InlineKeyboardButton(text="1h", callback_data=ChartCallback(symbol=symbol, timeframe="1h").pack()),
            InlineKeyboardButton(text="4h", callback_data=ChartCallback(symbol=symbol, timeframe="4h").pack()),
        ],
        # Action buttons row
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
    Listens to any text message. If the text is short (1-10 chars) 
    and alphanumeric, treats it as a crypto ticker.
    """
    text = message.text.strip().upper()
    
    # Basic validation to ensure it looks like a ticker (e.g., BTC, SOL, ETH)
    if not text.isalnum() or len(text) > 10:
        return
        
    symbol = text
    
    # 1. Fetch Price Data
    data = await fetch_price_data(symbol)
    
    # 2. Format the caption (matching the user's requested style)
    caption = (
        f"🪙 **{data['symbol']}**\n"
        f"💰 **P:** ${data['price']}\n"
        f"📉 **24h:** {data['change_24h']}%\n\n"
        f"📈 **H:** ${data['high']}\n"
        f"📉 **L:** ${data['low']}\n"
        f"📊 **Vol:** {data['volume']} USDT\n"
    )
    
    # 3. Generate initial 1h chart
    chart_bytes = await generate_chart_image(symbol, "1h")
    photo = BufferedInputFile(chart_bytes, filename=f"{symbol}_chart.png")
    
    # 4. Send photo with keyboard
    await message.reply_photo(
        photo=photo,
        caption=caption,
        parse_mode="Markdown",
        reply_markup=get_price_keyboard(symbol)
    )

@dp.callback_query(ChartCallback.filter())
async def process_chart_timeframe(query: types.CallbackQuery, callback_data: ChartCallback):
    """
    Handles inline button clicks to change the chart timeframe.
    Generates a new chart and edits the message media.
    """
    symbol = callback_data.symbol
    timeframe = callback_data.timeframe
    
    await query.answer(f"Loading {timeframe} chart...")
    
    # Generate new chart based on selected timeframe
    chart_bytes = await generate_chart_image(symbol, timeframe)
    new_photo = types.InputMediaPhoto(
        media=BufferedInputFile(chart_bytes, filename=f"{symbol}_{timeframe}.png"),
        # We must preserve the original caption
        caption=query.message.caption,
        parse_mode="Markdown"
    )
    
    # Edit the existing message with the new chart
    await query.message.edit_media(
        media=new_photo,
        reply_markup=get_price_keyboard(symbol)
    )

# ---------------------------------------------------------
# Web Server Setup (For Render / UptimeRobot)
# ---------------------------------------------------------
async def health_check(request):
    """Simple HTTP endpoint to keep the bot alive on Render."""
    return web.Response(text="SuperEx Bot is Running!")

async def main():
    """Starts the web server and the bot polling simultaneously."""
    # 1. Setup aiohttp web application
    app = web.Application()
    app.router.add_get('/', health_check)
    
    # 2. Setup the aiohttp runner
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', PORT)
    
    # 3. Start the web server (non-blocking)
    logging.info(f"🌐 Web server starting on port {PORT}")
    await site.start()
    
    # 4. Start Bot Polling
    logging.info("🚀 Bot polling started")
    await dp.start_polling(bot)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logging.info("🛑 Bot stopped gracefully.")
