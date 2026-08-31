import re
import aiohttp
from aiogram import Router, F, types
from dollar_service import get_best_tether_price_toman

calculator_router = Router()

# الگوی دقیق: عدد + حروف انگلیسی نماد ارز (با فاصله یا بدون فاصله)
CALC_REGEX = re.compile(r"^\s*(\d+(?:\.\d+)?)\s*([a-zA-Z]{2,10})\s*$", re.IGNORECASE)

async def fetch_crypto_price_usd(symbol: str) -> float:
    base = symbol.upper()
    
    # ۱. بایننس
    url_binance = f"https://data-api.binance.vision/api/v3/ticker/price?symbol={base}USDT"
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url_binance, timeout=3.5) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return float(data.get("price", 0))
    except Exception:
        pass

    # ۲. فال‌بک صرافی SuperEx
    url_superex = f"https://api.superexchang.com/resource/v3/public/currency/new?currency={base.lower()}"
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url_superex, timeout=3.5) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    price = data.get("data", {}).get("newPrice")
                    if price:
                        return float(price)
    except Exception:
        pass

    return 0.0

@calculator_router.message(F.text.regexp(CALC_REGEX))
async def handle_calculator_queries(message: types.Message):
    match = CALC_REGEX.match(message.text.strip())
    if not match:
        return

    amount = float(match.group(1))
    symbol = match.group(2).upper()

    crypto_price = await fetch_crypto_price_usd(symbol)
    tether_rate = await get_best_tether_price_toman()

    if crypto_price <= 0:
        return

    total_usd = amount * crypto_price
    total_toman = total_usd * tether_rate

    msg = (
        f"🧮 **محاسبه‌گر ارزش ارز دیجیتال**\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"🔹 **مقدار:** `{amount:g} {symbol}`\n"
        f"💵 **قیمت واحد:** `${crypto_price:,.4f}`\n"
        f"🇮🇷 **نرخ مبنای تتر:** `{int(tether_rate):,} تومان`\n\n"
        f"💰 **مجموع به دلار:** `${total_usd:,.2f}`\n"
        f"💳 **مجموع به تومان:** `{int(total_toman):,} تومان`\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"🤖 @SuperExFa_bot | @SuperexIR"
    )

    await message.reply(msg, parse_mode="Markdown")
