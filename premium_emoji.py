# -*- coding: utf-8 -*-
"""
premium_emoji.py
---------------------------------------------------------
تمام ایموجی‌های پریمیوم (Custom Emoji) ربات فقط همین‌جا تعریف شده‌اند.
برای عوض کردن هر ایموجی، فقط عدد آن را در جدول‌های پایین عوض کنید.

قواعد:
  - هر جای کد با e("نام") یا digits(...) ایموجی می‌گیرد.
  - اگر تلگرام ایموجی پریمیوم را نپذیرفت، کدهای دیگر خودشان همان پیام را
    با ایموجی معمولی (PLAIN) دوباره می‌فرستند و چیزی گم نمی‌شود.
  - برای خاموش کردن همه‌ی ایموجی‌های پریمیوم: PREMIUM_EMOJI=0
---------------------------------------------------------
"""
import os
import re

ENABLED = os.getenv("PREMIUM_EMOJI", "1").strip() not in ("0", "false", "False", "")

# ---------- ایموجی‌های تکی ----------
IDS = {
    "flower_l": "5008226549137146668",    # گل اول (کنار تیتر)
    "flower_r": "5008089496730731370",    # گل دوم (کنار تیتر)
    "arrow_link": "5008058452707115927",  # فلش کنار «مشاهده‌ی جزئیات»
    "arrow_btn": "5008529078043543655",   # فلش روی دکمه‌ی «عضویت در کانال»
    "lock": "5917936184759165044",        # قفل باز روی دکمه‌ی «عضو شدم»
    "bell": "5918186907770037878",        # زنگ کنار عنوان خبر
    "hash": "5825847726541117806",        # جای علامت #
    "date": "5008309429121057674",        # ایموجی تاریخ (عصای جادویی)
}

# ---------- ارقام ----------
GOLD = {
    "1": "5008374149983240991", "2": "5008052718925776038", "3": "5008323795786662694",
    "4": "5010475376833463914", "5": "5008199486548215008", "6": "5008110361681855442",
    "7": "5008059376125084593", "8": "5008038305015530427", "9": "5010376137319121570",
    "0": "5010299759915697009",
}
SILVER = {
    "1": "5825503545041885943", "2": "5825650965499353571", "3": "5827722466880920987",
    "4": "5825833797962178495", "5": "5827851994504633831", "6": "5825542002179053217",
    "7": "5825665194726006628", "8": "5825695899447203892", "9": "5825725294203378570",
    "0": "5828199637747505123",
}

# متنِ داخل تگ (فقط برای کلاینت‌های خیلی قدیمی که ایموجی پریمیوم را نشان نمی‌دهند)
_FALLBACK = {"flower_l": "✨", "flower_r": "✨", "arrow_link": "⬅️", "arrow_btn": "📢",
             "lock": "🔓", "bell": "💎", "hash": "#", "date": "📅"}
_KEYCAP = {"0": "0️⃣", "1": "1️⃣", "2": "2️⃣", "3": "3️⃣", "4": "4️⃣",
           "5": "5️⃣", "6": "6️⃣", "7": "7️⃣", "8": "8️⃣", "9": "9️⃣"}

# حالت بدون ایموجی پریمیوم (همان ظاهر قبلی)
PLAIN = {"flower_l": "", "flower_r": "", "arrow_link": "🔗", "arrow_btn": "📢",
         "lock": "✅", "bell": "🔔", "hash": "#", "date": "📅"}

_FA = str.maketrans("0123456789", "۰۱۲۳۴۵۶۷۸۹")


def active(premium: bool = True) -> bool:
    """آیا در این پیام باید ایموجی پریمیوم استفاده شود؟"""
    return bool(premium and ENABLED)


def _tag(emoji_id: str, fallback: str) -> str:
    return f'<tg-emoji emoji-id="{emoji_id}">{fallback}</tg-emoji>'


def e(name: str, premium: bool = True) -> str:
    """ایموجی تکی با نام؛ در حالت غیرپریمیوم معادل معمولی."""
    return _tag(IDS[name], _FALLBACK[name]) if active(premium) else PLAIN[name]


def icon_id(name: str, premium: bool = True):
    """شناسه‌ی ایموجی برای دکمه‌ها (icon_custom_emoji_id)؛ None یعنی بدون آیکن."""
    return IDS[name] if active(premium) else None


def digits(value, style: str = "gold", premium: bool = True, persian: bool = False) -> str:
    """هر رقم را به ایموجی طلایی/نقره‌ای تبدیل می‌کند (غیرپریمیوم: عدد ساده)."""
    text = str(value)
    if not active(premium):
        return text.translate(_FA) if persian else text
    
    table = GOLD if style == "gold" else SILVER
    parts = [_tag(table[c], _KEYCAP[c]) if c in table else c for c in text]
    
    # در متون راست‌به‌چپ تلگرام (مثل زبان فارسی ربات)، تصاویر و ایموجی‌ها از راست چیده می‌شوند.
    # با معکوس کردن لیست، ترتیب بصری ارقام به طور خودکار از چپ به راست تنظیم و ایزوله می‌شود.
    parts.reverse()

    return "".join(parts)


def hashtag(tag_text: str, premium: bool = True) -> str:
    """هشتگ واقعی، متنی و قابل‌کلیک (بدون ایموجی پریمیوم)."""
    return tag_text


_ENTITY_OPEN_RE = re.compile(
    r"<(?:tg-emoji|b|strong|i|em|u|ins|s|strike|del|a|code|pre|tg-spoiler|blockquote)\b[^>]*>",
    re.IGNORECASE,
)


def entity_count(html_text: str) -> int:
    """تعداد entity های پیام (سقف تلگرام ۱۰۰ تا است)."""
    if not html_text:
        return 0
    return len(_ENTITY_OPEN_RE.findall(html_text))


def test_text() -> str:
    """متن آزمایشی برای دستور /emojitest."""
    lines = ["<b>تست ایموجی‌های پریمیوم</b>", ""]
    for name in IDS:
        lines.append(f"{name}: {e(name)}")
    lines.append("")
    lines.append("طلایی: " + digits("0123456789", "gold"))
    lines.append("نقره‌ای: " + digits("0123456789", "silver"))
    return "\n".join(lines)
