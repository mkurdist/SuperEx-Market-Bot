import aiohttp
import logging
from aiogram import Router, F, types

dollar_router = Router()

async def fetch_free_market_dollar() -> dict:
    url = "https://call1.tgju.org/ajax.json"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Referer": "https://www.tgju.org/"
    }
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers, timeout=4.0) as resp:
                if resp.status == 200:
                    res = await resp.json()
                    item = res.get("current", {}).get("price_dollar_rl", {})
                    if item:
                        p_rial = int(float(str(item.get("p", "0")).replace(",", "")))
                        toman = p_rial // 10
                        return {
                            "source": "بازار آزاد (تهران)",
                            "toman": toman,
                            "change": str(item.get("dp", "0")),
                            "formatted": f"{toman:,} تومان"
                        }
    except Exception as e:
        logging.warning(f"Error fetching free market dollar: {e}")
    return {}

async def fetch_nobitex_tether() -> dict:
    url = "https://api.nobitex.ir/market/stats?srcCurrency=usdt&dstCurrency=rls"
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=3.5) as resp:
                if resp.status == 200:
                    res = await resp.json()
                    stat = res.get("stats", {}).get("usdt-rls", {})
                    if stat:
                        rial = int(float(stat.get("latest", 0)))
                        toman = rial // 10
                        return {
                            "source": "نوبیتکس (Nobitex)",
                            "toman": toman,
                            "change": str(stat.get("dayChange", "0")),
                            "formatted": f"{toman:,} تومان"
                        }
    except Exception as e:
        logging.warning(f"Error fetching Nobitex: {e}")
    return {}

async def fetch_bitpin_tether() -> dict:
    url = "https://api.bitpin.ir/v1/mkt/markets/"
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=3.5) as resp:
                if resp.status == 200:
                    res = await resp.json()
                    results = res.get("results", [])
                    for m in results:
                        if m.get("code") == "USDT_IRT":
                            price = int(float(m.get("price", 0)))
                            return {
                                "source": "بیت‌پین (Bitpin)",
                                "toman": price,
                                "change": str(m.get("day_change", "0")),
                                "formatted": f"{price:,} تومان"
                            }
    except Exception as e:
        logging.warning(f"Error fetching Bitpin: {e}")
    return {}

async def fetch_wallex_tether() -> dict:
    url = "https://api.wallex.ir/v1/markets"
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=3.5) as resp:
                if resp.status == 200:
                    res = await resp.json()
                    market = res.get("result", {}).get("symbols", {}).get("USDTTMN", {})
                    stats = market.get("stats", {})
                    if stats:
                        price = int(float(stats.get("lastPrice", 0)))
                        return {
                            "source": "والکس (Wallex)",
                            "toman": price,
                            "change": str(stats.get("24h_ch", "0")),
                            "formatted": f"{price:,} تومان"
                        }
    except Exception as e:
        logging.warning(f"Error fetching Wallex: {e}")
    return {}

async def get_best_tether_price_toman() -> float:
    prices = []
    for fetcher in [fetch_nobitex_tether, fetch_bitpin_tether, fetch_wallex_tether, fetch_free_market_dollar]:
        res = await fetcher()
        if res and res.get("toman", 0) > 0:
            prices.append(res["toman"])
    if prices:
        return sum(prices) / len(prices)
    return 60000.0

def format_dollar_message(free_usd: dict, exchanges: list) -> str:
    msg = "💵 **قیمت لحظه‌ای دلار و تتر (USDT)**\n"
    msg += "━━━━━━━━━━━━━━━━━━\n\n"

    if free_usd:
        ch_sign = "+" if not free_usd['change'].startswith("-") and free_usd['change'] != "0" else ""
        msg += f"🇺🇸 **دلار بازار آزاد:** `{free_usd['formatted']}` ({ch_sign}{free_usd['change']}%)\n\n"

    msg += "🌐 **نرخ تتر در صرافی‌های معتبر داخلی:**\n"
    for ex in exchanges:
        if ex:
            ch_sign = "+" if not ex['change'].startswith("-") and ex['change'] != "0" else ""
            msg += f"🔹 **{ex['source']}:** `{ex['formatted']}` ({ch_sign}{ex['change']}%)\n"

    msg += "\n━━━━━━━━━━━━━━━━━━\n"
    msg += "🤖 @SuperExFa_bot | @SuperexIR"
    return msg

@dollar_router.message(F.text.func(lambda text: text and text.strip().lower() in ["دلار", "تتر", "usdt", "dollar", "قیمت دلار", "قیمت تتر", "نرخ دلار"]))
async def handle_dollar_query(message: types.Message):
    wait_msg = await message.reply("⏳ در حال استعلام نرخ دلار و تتر...")
    
    import asyncio
    free_usd_task = asyncio.create_task(fetch_free_market_dollar())
    nobitex_task = asyncio.create_task(fetch_nobitex_tether())
    bitpin_task = asyncio.create_task(fetch_bitpin_tether())
    wallex_task = asyncio.create_task(fetch_wallex_tether())

    free_usd = await free_usd_task
    exchanges = [
        await nobitex_task,
        await bitpin_task,
        await wallex_task
    ]
    
    text = format_dollar_message(free_usd, [e for e in exchanges if e])
    await wait_msg.edit_text(text, parse_mode="Markdown")
