import aiohttp
import logging
from aiogram import Router, F, types

gold_router = Router()

def clean_number(val) -> int:
    """تبدیل رشته‌های عددی دارای کاما به عدد صحیح ریال"""
    if not val:
        return 0
    clean_str = str(val).replace(",", "").strip()
    try:
        return int(float(clean_str))
    except (ValueError, TypeError):
        return 0

def rial_to_toman_str(rial_val: int) -> str:
    """تبدیل ریال به تومان با جداکننده سه رقمی"""
    toman = rial_val // 10
    return f"{toman:,}"

async def fetch_gold_and_coin_prices() -> dict:
    """دریافت قیمت لحظه‌ای طلا و سکه از سرورهای داده زنده TGJU"""
    urls = [
        "https://call1.tgju.org/ajax.json",
        "https://call.tgju.org/ajax.json",
        "https://call5.tgju.org/ajax.json"
    ]
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
        "Accept": "application/json, text/javascript, */*; q=0.01"
    }

    async with aiohttp.ClientSession() as session:
        for url in urls:
            try:
                async with session.get(url, headers=headers, timeout=4.0) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        current = data.get("current", {})
                        if current:
                            return parse_tgju_gold(current)
            except Exception as e:
                logging.warning(f"Failed fetching gold from {url}: {e}")
                continue

    return {}

def parse_tgju_gold(current: dict) -> dict:
    """استخراج و قالب‌بندی اقلام مختلف طلا و سکه"""
    items = {
        "geram18": "طلای ۱۸ عیار",
        "geram24": "طلای ۲۴ عیار",
        "mesghal": "مثقال طلا (آبشده)",
        "ons": "انس جهانی طلا",
        "sekee": "سکه امامی (طرح جدید)",
        "sekeb": "سکه بهار آزادی (طرح قدیم)",
        "nim": "نیم سکه",
        "rob": "ربع سکه",
        "gerami": "سکه گرمی"
    }
    
    parsed = {}
    for key, name in items.items():
        row = current.get(key, {})
        if row:
            p_rial = clean_number(row.get("p"))
            h_rial = clean_number(row.get("h"))
            l_rial = clean_number(row.get("l"))
            dp = str(row.get("dp", "0"))
            
            if key == "ons":
                price_display = f"${p_rial:,}" if p_rial else str(row.get("p", "0"))
            else:
                price_display = f"{rial_to_toman_str(p_rial)} تومان"
                
            parsed[key] = {
                "name": name,
                "price": price_display,
                "change": dp,
                "high": rial_to_toman_str(h_rial) if key != "ons" else str(h_rial),
                "low": rial_to_toman_str(l_rial) if key != "ons" else str(l_rial),
                "time": row.get("t", "")
            }
    return parsed

def format_gold_message(data: dict) -> str:
    """تولید متن زیبا و جذاب برای تلگرام"""
    if not data:
        return "❌ در حال حاضر دریافت نرخ طلا و سکه با مشکل مواجه شد. لطفاً کمی بعد تلاش کنید."

    msg = "🏆 **نرخ لحظه‌ای طلا، سکه و آبشده**\n"
    msg += "━━━━━━━━━━━━━━━━━━\n\n"
    
    # طلا و آبشده
    msg += "🪙 **بخش طلا و آبشده:**\n"
    for k in ["geram18", "mesghal", "geram24", "ons"]:
        if k in data:
            item = data[k]
            ch_sign = "+" if not item['change'].startswith("-") and item['change'] != "0" else ""
            msg += f"🔸 **{item['name']}:** `{item['price']}` ({ch_sign}{item['change']}%)\n"

    msg += "\n👑 **بخش انواع سکه:**\n"
    for k in ["sekee", "sekeb", "nim", "rob", "gerami"]:
        if k in data:
            item = data[k]
            ch_sign = "+" if not item['change'].startswith("-") and item['change'] != "0" else ""
            msg += f"🔹 **{item['name']}:** `{item['price']}` ({ch_sign}{item['change']}%)\n"

    msg += "\n━━━━━━━━━━━━━━━━━━\n"
    msg += "🤖 @SuperExFa_bot | @SuperexIR"
    return msg

# هندلر پاسخ به پیام‌های کاربران
@gold_router.message(F.text.lower().in_(["طلا", "سکه", "قیمت طلا", "قیمت سکه", "gold", "مظنه", "ابشده", "آبشده"]))
async def handle_gold_query(message: types.Message):
    wait_msg = await message.reply("⏳ در حال دریافت آخرین نرخ طلا و سکه...")
    data = await fetch_gold_and_coin_prices()
    text = format_gold_message(data)
    await wait_msg.edit_text(text, parse_mode="Markdown")
