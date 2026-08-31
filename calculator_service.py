import re
import aiohttp
import logging
from aiogram import Router, types
from dollar_service import get_best_tether_price_toman

calculator_router = Router()

# الگوی هوشمند برای تشخیص ورودی‌هایی مثل "1.5 btc" یا "500 trx"
CALC_REGEX = re.compile(r"^(\d+(?:\.\d+)?)\s*([a-zA-Z]{2,10})$", re.IGNORECASE)

async def fetch_crypto_price_usd(symbol: str) -> float:
    """دریافت قیمت دلاری ارز از بایننس یا صرافی SuperEx"""
    base = symbol.upper()
    
    # ۱. استعلام از بایننس
    url_binance = f"https://data-api.binance.vision/api/v3/ticker/price?symbol={base}USDT"
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url_binance, timeout=3.5) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return float(data.get("price", 0))
    except Exception:
        pass

    # ۲. استعلام پشتیبان از SuperEx
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

@calculator_router.message()
async def handle_calculator_queries(message: types.Message):
    if not message.text:
        return

    text = message.text.strip()
    match = CALC_REGEX.match(text)
    
    if not match:
        return  # اگر الگوی عدد + نام ارز نبود، اجازه می‌دهد سایر هندلرها اجرا شوند

    amount = float(match.group(1))
    symbol = match.group(2).upper()

    # استعلام قیمت دلاری و نرخ تتر به تومان به صورت موازی
    crypto_price_task = fetch_crypto_price_usd(symbol)
    tether_rate_task = get_best_tether_price_toman()

    crypto_price = await crypto_price_task
    tether_rate = await tether_rate_task

    if crypto_price <= 0:
        return  # اگر ارزی پیدا نشد، پیام خطا نمی‌دهد تا ربات مزاحم چت‌های عادی نشود

    total_usd = amount * crypto_price
    total_toman = total_usd * tether_rate

    # فرمت‌دهی خروجی
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
