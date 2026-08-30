import asyncio
import io
import json
import logging
import os
import time
import uuid
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from contextlib import suppress
from typing import Any, Optional

import aiohttp
import pandas as pd
import mplfinance as mpf
import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt

from PIL import Image, ImageChops

from aiogram import Bot, Dispatcher, F, types
from aiogram.exceptions import TelegramConflictError
from aiogram.filters.callback_data import CallbackData
from aiogram.types import (
    BufferedInputFile,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
)
from aiohttp import web
from dotenv import load_dotenv


# ============================================================
# CONFIG
# ============================================================

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()

PORT = int(os.getenv("PORT", "10000"))

ADMIN_CHAT_ID = int(
    os.getenv("ADMIN_CHAT_ID", "0")
)

SUPEREX_API_BASE = os.getenv(
    "SUPEREX_API_BASE",
    "https://api.superexchang.com"
).rstrip("/")


# ------------------------------------------------------------
# Render / networking
# ------------------------------------------------------------

HTTP_TIMEOUT = float(
    os.getenv("HTTP_TIMEOUT", "12")
)

HTTP_RETRIES = int(
    os.getenv("HTTP_RETRIES", "2")
)

KLINE_LIMIT = int(
    os.getenv("KLINE_LIMIT", "100")
)


# ------------------------------------------------------------
# Cache
# ------------------------------------------------------------

PRICE_CACHE_TTL = float(
    os.getenv("PRICE_CACHE_TTL", "3")
)

CHART_CACHE_TTL = float(
    os.getenv("CHART_CACHE_TTL", "5")
)

CACHE_MAX_ENTRIES = int(
    os.getenv("CACHE_MAX_ENTRIES", "300")
)


# ------------------------------------------------------------
# Chart rendering
# ------------------------------------------------------------

MAX_CONCURRENT_CHARTS = int(
    os.getenv("MAX_CONCURRENT_CHARTS", "2")
)

CHART_RENDER_WORKERS = int(
    os.getenv(
        "CHART_RENDER_WORKERS",
        str(max(2, min(4, os.cpu_count() or 2)))
    )
)


# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format=(
        "%(asctime)s | "
        "%(levelname)s | "
        "%(name)s | "
        "%(message)s"
    ),
)

logger = logging.getLogger("superex-bot")


# ============================================================
# VALIDATION
# ============================================================

if not BOT_TOKEN:
    raise RuntimeError(
        "BOT_TOKEN is not configured."
    )


# ============================================================
# TELEGRAM
# ============================================================

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()


# ============================================================
# HTTP SESSION
# ============================================================

HTTP_SESSION: Optional[aiohttp.ClientSession] = None


# ============================================================
# TIMEFRAMES
# ============================================================

TIMEFRAME_MAP = {
    "1m": "1min",
    "15m": "15min",
    "1h": "1hour",
    "4h": "4hour",
    "1d": "1day",
}


TIMEFRAME_SECONDS = {
    "1m": 60,
    "15m": 900,
    "1h": 3600,
    "4h": 14400,
    "1d": 86400,
}


# ============================================================
# CALLBACK
# ============================================================

class ChartCallback(CallbackData, prefix="chart"):
    symbol: str
    timeframe: str


# ============================================================
# CACHE
# ============================================================

PRICE_CACHE = {}
CHART_CACHE = {}

CACHE_LOCK = asyncio.Lock()


def cache_get(
    store: dict,
    key: Any,
    ttl: float,
):
    entry = store.get(key)

    if not entry:
        return None

    timestamp, value = entry

    if time.monotonic() - timestamp >= ttl:
        store.pop(key, None)
        return None

    return value


def cache_set(
    store: dict,
    key: Any,
    value: Any,
):
    store[key] = (
        time.monotonic(),
        value,
    )

    if len(store) > CACHE_MAX_ENTRIES:

        oldest_key = min(
            store,
            key=lambda k: store[k][0],
        )

        store.pop(
            oldest_key,
            None,
        )


# ============================================================
# IN-FLIGHT REQUEST CONTROL
# ============================================================

KLINE_LOCKS = defaultdict(asyncio.Lock)


# ============================================================
# ADMIN ALERT
# ============================================================

ADMIN_ALERT_COOLDOWN = 300

_admin_alert_last_sent = {}


async def notify_admin_error(
    error_key: str,
    message: str,
):

    if not ADMIN_CHAT_ID:
        logger.warning(
            "ADMIN_CHAT_ID is not configured."
        )
        return

    now = time.monotonic()

    last = _admin_alert_last_sent.get(
        error_key,
        0,
    )

    if now - last < ADMIN_ALERT_COOLDOWN:
        return

    _admin_alert_last_sent[error_key] = now

    text = (
        "⚠️ SuperEx Bot Error\n\n"
        f"Type: {error_key}\n\n"
        f"{message}"
    )

    try:

        await bot.send_message(
            ADMIN_CHAT_ID,
            text,
        )

    except Exception as exc:

        logger.error(
            "Failed to notify admin: %s",
            exc,
        )


# ============================================================
# SUPEREX HEADERS
# ============================================================

def get_superex_headers() -> dict:

    return {
        "accept": "*/*",
        "accept-language": "en-US,en;q=0.9",
        "client": "1",
        "nonce": uuid.uuid4().hex,
        "timestamp": str(
            int(time.time() * 1000)
        ),
        "token": "",
        "content-type":
            "application/x-www-form-urlencoded",
        "user-agent":
            "Mozilla/5.0 "
            "(Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 "
            "(KHTML, like Gecko) "
            "Chrome/151.0.0.0 Safari/537.36",
    }


# ============================================================
# HTTP SESSION MANAGEMENT
# ============================================================

async def create_http_session():

    global HTTP_SESSION

    timeout = aiohttp.ClientTimeout(
        total=HTTP_TIMEOUT,
        connect=5,
        sock_read=HTTP_TIMEOUT,
    )

    connector = aiohttp.TCPConnector(
        limit=30,
        ttl_dns_cache=300,
        enable_cleanup_closed=True,
    )

    HTTP_SESSION = aiohttp.ClientSession(
        timeout=timeout,
        connector=connector,
        headers={
            "accept": "*/*",
        },
    )

    logger.info(
        "SuperEx HTTP session initialized"
    )


async def close_http_session():

    global HTTP_SESSION

    if HTTP_SESSION:

        await HTTP_SESSION.close()

        HTTP_SESSION = None

        logger.info(
            "SuperEx HTTP session closed"
        )


# ============================================================
# SYMBOL NORMALIZATION
# ============================================================

def normalize_symbol(symbol: str) -> str:

    symbol = (
        str(symbol)
        .strip()
        .upper()
        .replace("-", "")
        .replace("/", "")
        .replace(" ", "")
    )

    if symbol.endswith("_USDT"):
        symbol = symbol[:-5]

    if symbol.endswith("USDT"):
        symbol = symbol[:-4]

    return symbol


def superex_symbol(symbol: str) -> str:

    return (
        normalize_symbol(symbol)
        .lower()
        + "_usdt"
    )


# ============================================================
# NUMBER PARSER
# ============================================================

def to_float(value: Any) -> Optional[float]:

    if value is None:
        return None

    if isinstance(value, bool):
        return None

    try:

        result = float(value)

        if pd.isna(result):
            return None

        return result

    except (
        TypeError,
        ValueError,
    ):
        return None


# ============================================================
# TIMESTAMP PARSER
# ============================================================

def timestamp_to_datetime(
    timestamp: Any,
) -> Optional[pd.Timestamp]:

    try:

        value = float(timestamp)

    except (
        TypeError,
        ValueError,
    ):
        return None

    if value <= 0:
        return None

    # seconds
    if value < 10_000_000_000:

        return pd.to_datetime(
            int(value),
            unit="s",
            utc=True,
        )

    # milliseconds
    if value < 10_000_000_000_000:

        return pd.to_datetime(
            int(value),
            unit="ms",
            utc=True,
        )

    # microseconds
    if value < 10_000_000_000_000_000:

        return pd.to_datetime(
            int(value),
            unit="us",
            utc=True,
        )

    # nanoseconds
    return pd.to_datetime(
        int(value),
        unit="ns",
        utc=True,
    )


# ============================================================
# JSON EXTRACTION
# ============================================================

def extract_kline_container(
    payload: Any,
) -> list:

    if payload is None:
        return []

    # --------------------------------------------------------
    # Direct list
    # --------------------------------------------------------

    if isinstance(payload, list):
        return payload

    # --------------------------------------------------------
    # Recursive dictionary search
    # --------------------------------------------------------

    if isinstance(payload, dict):

        preferred_keys = [
            "data",
            "rows",
            "list",
            "result",
            "records",
            "items",
            "klines",
            "candles",
            "kline",
        ]

        for key in preferred_keys:

            value = payload.get(key)

            if isinstance(value, list):

                return value

            if isinstance(value, dict):

                nested = extract_kline_container(
                    value
                )

                if nested:
                    return nested

        # Search recursively
        for value in payload.values():

            if isinstance(value, dict):

                nested = extract_kline_container(
                    value
                )

                if nested:
                    return nested

            elif isinstance(value, list):

                if value and any(
                    isinstance(x, (list, dict))
                    for x in value
                ):
                    return value

    return []


# ============================================================
# SINGLE KLINE PARSER
# ============================================================

def parse_kline_item(
    item: Any,
) -> Optional[dict]:

    # --------------------------------------------------------
    # Dictionary format
    # --------------------------------------------------------

    if isinstance(item, dict):

        timestamp = (
            item.get("time")
            or item.get("timestamp")
            or item.get("ts")
            or item.get("t")
            or item.get("date")
            or item.get("id")
        )

        open_price = (
            item.get("open")
            if item.get("open") is not None
            else item.get("o")
        )

        high_price = (
            item.get("high")
            if item.get("high") is not None
            else item.get("h")
        )

        low_price = (
            item.get("low")
            if item.get("low") is not None
            else item.get("l")
        )

        close_price = (
            item.get("close")
            if item.get("close") is not None
            else item.get("c")
        )

        volume = (
            item.get("volume")
            if item.get("volume") is not None
            else item.get("v", 0)
        )

    # --------------------------------------------------------
    # Array format
    # --------------------------------------------------------

    elif isinstance(item, (list, tuple)):

        if len(item) < 6:
            return None

        timestamp = item[0]

        open_price = item[1]

        high_price = item[2]

        low_price = item[3]

        close_price = item[4]

        volume = item[5]

    else:
        return None

    dt = timestamp_to_datetime(
        timestamp
    )

    if dt is None:
        return None

    o = to_float(open_price)
    h = to_float(high_price)
    l = to_float(low_price)
    c = to_float(close_price)
    v = to_float(volume)

    if None in (
        o,
        h,
        l,
        c,
    ):
        return None

    if h <= 0 or l <= 0:
        return None

    return {
        "Date": dt,
        "Open": o,
        "High": h,
        "Low": l,
        "Close": c,
        "Volume": v or 0.0,
    }


# ============================================================
# KLINE VALIDATION
# ============================================================

def validate_kline_data(
    rows: list,
) -> list:

    parsed = []

    for item in rows:

        candle = parse_kline_item(
            item
        )

        if candle:

            parsed.append(candle)

    if not parsed:
        return []

    df = pd.DataFrame(parsed)

    # Remove invalid rows
    df = df.dropna(
        subset=[
            "Date",
            "Open",
            "High",
            "Low",
            "Close",
        ]
    )

    # Remove duplicates
    df = df.drop_duplicates(
        subset=["Date"],
        keep="last",
    )

    # Validate OHLC
    df = df[
        (df["High"] >= df["Low"])
        & (df["High"] >= df["Open"])
        & (df["High"] >= df["Close"])
        & (df["Low"] <= df["Open"])
        & (df["Low"] <= df["Close"])
    ]

    df = df.sort_values(
        "Date"
    )

    if df.empty:
        return []

    return df.to_dict(
        orient="records"
    )


# ============================================================
# FETCH JSON FROM SUPEREX
# ============================================================

async def superex_get(
    path: str,
    params: dict,
):

    global HTTP_SESSION

    if HTTP_SESSION is None:

        await create_http_session()

    url = (
        SUPEREX_API_BASE
        + path
    )

    last_error = None

    for attempt in range(
        HTTP_RETRIES + 1
    ):

        try:

            logger.info(
                "SuperEx GET attempt=%s url=%s params=%s",
                attempt + 1,
                url,
                params,
            )

            async with HTTP_SESSION.get(
                url,
                params=params,
                headers=get_superex_headers(),
            ) as response:

                text = await response.text()

                logger.info(
                    "SuperEx response status=%s "
                    "content_type=%s "
                    "body=%s",
                    response.status,
                    response.headers.get(
                        "Content-Type",
                        ""
                    ),
                    text[:2000],
                )

                if response.status != 200:

                    last_error = (
                        f"HTTP {response.status}: "
                        f"{text[:500]}"
                    )

                    if response.status in (
                        400,
                        401,
                        403,
                        404,
                    ):
                        break

                    await asyncio.sleep(
                        0.6 * (attempt + 1)
                    )

                    continue

                try:

                    return json.loads(
                        text
                    )

                except json.JSONDecodeError as exc:

                    last_error = (
                        f"Invalid JSON: {exc}; "
                        f"body={text[:500]}"
                    )

                    break

        except (
            asyncio.TimeoutError,
            aiohttp.ClientError,
        ) as exc:

            last_error = repr(exc)

            logger.warning(
                "SuperEx network error attempt=%s: %r",
                attempt + 1,
                exc,
            )

            if attempt < HTTP_RETRIES:

                await asyncio.sleep(
                    0.6 * (attempt + 1)
                )

    raise RuntimeError(
        last_error
        or "Unknown SuperEx HTTP error"
    )


# ============================================================
# PRICE API
# ============================================================

async def fetch_price_data(
    symbol: str,
) -> dict:

    base_symbol = normalize_symbol(
        symbol
    )

    path = (
        "/resource/v3/public/currency/new"
    )

    try:

        payload = await superex_get(
            path,
            {
                "currency":
                    base_symbol.lower()
            },
        )

        data = payload.get(
            "data",
            {}
        ) if isinstance(
            payload,
            dict
        ) else {}

        if not isinstance(
            data,
            dict
        ):
            data = {}

        price = (
            data.get("newPrice")
            or data.get("price")
        )

        if price is None:

            raise RuntimeError(
                "SuperEx ticker returned "
                "no newPrice/price"
            )

        return {
            "symbol": base_symbol,
            "price": str(price),
            "change_24h": str(
                data.get(
                    "change",
                    "0"
                )
            ),
            "high": str(
                data.get(
                    "maxPrice",
                    data.get(
                        "high",
                        "0"
                    )
                )
            ),
            "low": str(
                data.get(
                    "minPrice",
                    data.get(
                        "low",
                        "0"
                    )
                )
            ),
            "volume": str(
                data.get(
                    "sumNumber",
                    data.get(
                        "volume",
                        "0"
                    )
                )
            ),
            "source": "SuperEx",
        }

    except Exception as exc:

        logger.exception(
            "SuperEx price error for %s",
            symbol,
        )

        await notify_admin_error(
            "superex_price",
            (
                f"Symbol: {base_symbol}\n"
                f"Error: {exc}"
            ),
        )

        return {
            "error":
                "دریافت قیمت از SuperEx "
                "با خطا مواجه شد."
        }


# ============================================================
# CACHED PRICE
# ============================================================

async def get_price_data_cached(
    symbol: str,
) -> dict:

    key = normalize_symbol(
        symbol
    )

    async with CACHE_LOCK:

        cached = cache_get(
            PRICE_CACHE,
            key,
            PRICE_CACHE_TTL,
        )

    if cached is not None:

        return cached

    data = await fetch_price_data(
        key
    )

    if "error" not in data:

        async with CACHE_LOCK:

            cache_set(
                PRICE_CACHE,
                key,
                data,
            )

    return data


# ============================================================
# KLINE API
# ============================================================

async def fetch_superex_kline(
    symbol: str,
    timeframe: str,
) -> list:

    symbol_clean = normalize_symbol(
        symbol
    )

    if timeframe not in TIMEFRAME_MAP:

        raise ValueError(
            f"Unsupported timeframe: {timeframe}"
        )

    resolution = TIMEFRAME_MAP[
        timeframe
    ]

    market_symbol = (
        symbol_clean.lower()
        + "_usdt"
    )

    path = (
        "/resource/v3/public/kline"
    )

    params = {
        "symbol": market_symbol,
        "resolution": resolution,
        "limit": KLINE_LIMIT,
    }

    # --------------------------------------------------------
    # Prevent multiple identical Kline requests
    # --------------------------------------------------------

    lock_key = (
        symbol_clean,
        timeframe,
    )

    async with KLINE_LOCKS[
        lock_key
    ]:

        payload = await superex_get(
            path,
            params,
        )

    rows = extract_kline_container(
        payload
    )

    logger.info(
        "SuperEx Kline raw rows: %s",
        len(rows),
    )

    if not rows:

        logger.error(
            "SuperEx Kline returned no rows. "
            "payload=%s",
            str(payload)[:4000],
        )

        raise RuntimeError(
            "SuperEx Kline response "
            "contains no candle rows."
        )

    parsed = validate_kline_data(
        rows
    )

    logger.info(
        "SuperEx Kline parsed candles: %s",
        len(parsed),
    )

    if not parsed:

        raise RuntimeError(
            "SuperEx Kline rows were "
            "received but could not be parsed."
        )

    return parsed


# ============================================================
# CHART STYLE
# ============================================================

MARKET_COLORS = mpf.make_marketcolors(
    up="#00d964",
    down="#ff3b3b",
    edge="inherit",
    wick="inherit",
    volume="in",
    ohlc="i",
)


CHART_STYLE = mpf.make_mpf_style(
    marketcolors=MARKET_COLORS,
    base_mpf_style="binance",
    facecolor="#000000",
    edgecolor="#555555",
    figcolor="#000000",
    gridcolor="#222222",
    gridstyle="--",
    y_on_right=False,
    rc={
        "font.family":
            "DejaVu Sans",
        "axes.titleweight":
            "normal",
        "axes.titlesize":
            13,
        "axes.titlecolor":
            "#e6e6e6",
        "axes.labelcolor":
            "#cfcfcf",
        "xtick.color":
            "#9a9a9a",
        "ytick.color":
            "#9a9a9a",
        "text.color":
            "#9a9a9a",
    },
)


# ============================================================
# CHART EXECUTOR
# ============================================================

CHART_RENDER_EXECUTOR = (
    ThreadPoolExecutor(
        max_workers=CHART_RENDER_WORKERS,
        thread_name_prefix="chart-render",
    )
)


CHART_RENDER_SEMAPHORE = (
    asyncio.Semaphore(
        MAX_CONCURRENT_CHARTS
    )
)


# ============================================================
# IMAGE CROP
# ============================================================

def autocrop_black_margins(
    png_bytes: bytes,
    padding: int = 14,
    min_width: int = 1100,
) -> bytes:

    img = Image.open(
        io.BytesIO(
            png_bytes
        )
    ).convert("RGB")

    background = Image.new(
        "RGB",
        img.size,
        (0, 0, 0),
    )

    diff = ImageChops.difference(
        img,
        background,
    )

    bbox = diff.getbbox()

    if bbox:

        left, top, right, bottom = bbox

        left = max(
            0,
            left - padding,
        )

        top = max(
            0,
            top - padding,
        )

        right = min(
            img.width,
            right + padding,
        )

        bottom = min(
            img.height,
            bottom + padding,
        )

        img = img.crop(
            (
                left,
                top,
                right,
                bottom,
            )
        )

    if img.width < min_width:

        scale = (
            min_width
            / img.width
        )

        img = img.resize(
            (
                min_width,
                round(
                    img.height
                    * scale
                ),
            ),
            Image.Resampling.LANCZOS,
        )

    output = io.BytesIO()

    img.save(
        output,
        format="PNG",
        optimize=True,
    )

    return output.getvalue()


# ============================================================
# SYNC CHART RENDERER
# ============================================================

def render_chart_sync(
    records: list,
    symbol: str,
    timeframe: str,
) -> bytes:

    df = pd.DataFrame(
        records
    )

    if df.empty:

        raise ValueError(
            "Empty DataFrame"
        )

    df["Date"] = pd.to_datetime(
        df["Date"],
        utc=True,
    )

    df.set_index(
        "Date",
        inplace=True,
    )

    df = df[
        [
            "Open",
            "High",
            "Low",
            "Close",
            "Volume",
        ]
    ]

    df = df.sort_index()

    # mplfinance prefers timezone-naive indexes
    df.index = (
        df.index
        .tz_convert(None)
    )

    if timeframe == "1d":

        date_format = "%b"
        x_rotation = 0

    else:

        date_format = "%H:%M"
        x_rotation = 0

    fig, axes = mpf.plot(
        df,
        type="candle",
        style=CHART_STYLE,
        volume=False,
        title=(
            f"{symbol.upper()}/USDT"
            f" | {timeframe}"
        ),
        ylabel="Price (USDT)",
        datetime_format=date_format,
        xrotation=x_rotation,
        tight_layout=False,
        returnfig=True,
        figsize=(13, 7.8),
        show_nontrading=False,
    )

    try:

        fig.subplots_adjust(
            top=0.94,
            bottom=0.11,
            left=0.065,
            right=0.985,
        )

        for ax in axes:

            for spine in (
                ax.spines.values()
            ):

                spine.set_visible(
                    True
                )

                spine.set_color(
                    "#555555"
                )

                spine.set_linewidth(
                    0.8
                )

        fig.text(
            0.5,
            0.02,
            "Created by @SuperExFa_bot | @SuperexIR",
            ha="center",
            va="center",
            fontsize=10.5,
            color="#9a9a9a",
        )

        output = io.BytesIO()

        fig.savefig(
            output,
            dpi=140,
            bbox_inches=None,
            pad_inches=0,
            facecolor=fig.get_facecolor(),
            edgecolor="none",
            format="png",
        )

        return autocrop_black_margins(
            output.getvalue()
        )

    finally:

        output.close()

        plt.close(fig)


# ============================================================
# GENERATE CHART
# ============================================================

async def generate_chart_image(
    symbol: str,
    timeframe: str,
) -> bytes:

    symbol_clean = normalize_symbol(
        symbol
    )

    cache_key = (
        symbol_clean,
        timeframe,
    )

    async with CACHE_LOCK:

        cached = cache_get(
            CHART_CACHE,
            cache_key,
            CHART_CACHE_TTL,
        )

    if cached is not None:

        logger.info(
            "Chart cache HIT %s %s",
            symbol_clean,
            timeframe,
        )

        return cached

    logger.info(
        "Generating chart %s %s",
        symbol_clean,
        timeframe,
    )

    try:

        records = (
            await fetch_superex_kline(
                symbol_clean,
                timeframe,
            )
        )

    except Exception as exc:

        logger.exception(
            "SuperEx Kline failed "
            "symbol=%s timeframe=%s",
            symbol_clean,
            timeframe,
        )

        await notify_admin_error(
            "superex_chart",
            (
                f"Symbol: {symbol_clean}\n"
                f"Timeframe: {timeframe}\n"
                f"Error: {exc}"
            ),
        )

        raise ValueError(
            "چارت SuperEx در دسترس نیست."
        )

    async with CHART_RENDER_SEMAPHORE:

        loop = asyncio.get_running_loop()

        image_bytes = (
            await loop.run_in_executor(
                CHART_RENDER_EXECUTOR,
                render_chart_sync,
                records,
                symbol_clean,
                timeframe,
            )
        )

    async with CACHE_LOCK:

        cache_set(
            CHART_CACHE,
            cache_key,
            image_bytes,
        )

    return image_bytes


# ============================================================
# KEYBOARD
# ============================================================

def get_price_keyboard(
    symbol: str,
) -> InlineKeyboardMarkup:

    symbol_clean = normalize_symbol(
        symbol
    )

    register_url = (
        "https://app.superex.live/"
        "register?"
        "invitationCode=VQK2N6DDS"
    )

    group_url = (
        "https://t.me/SuperexIR"
    )

    keyboard = [
        [
            InlineKeyboardButton(
                text="1m",
                callback_data=ChartCallback(
                    symbol=symbol_clean,
                    timeframe="1m",
                ).pack(),
            ),
            InlineKeyboardButton(
                text="15m",
                callback_data=ChartCallback(
                    symbol=symbol_clean,
                    timeframe="15m",
                ).pack(),
            ),
            InlineKeyboardButton(
                text="1h",
                callback_data=ChartCallback(
                    symbol=symbol_clean,
                    timeframe="1h",
                ).pack(),
            ),
            InlineKeyboardButton(
                text="4h",
                callback_data=ChartCallback(
                    symbol=symbol_clean,
                    timeframe="4h",
                ).pack(),
            ),
            InlineKeyboardButton(
                text="1d",
                callback_data=ChartCallback(
                    symbol=symbol_clean,
                    timeframe="1d",
                ).pack(),
            ),
        ],
        [
            InlineKeyboardButton(
                text="عضویت در گروه 👥",
                url=group_url,
            ),
            InlineKeyboardButton(
                text="ثبت نام در صرافی 🏦",
                url=register_url,
            ),
        ],
    ]

    return InlineKeyboardMarkup(
        inline_keyboard=keyboard
    )


# ============================================================
# CAPTION
# ============================================================

def build_price_caption(
    data: dict,
) -> str:

    return (
        f"🪙 **{data['symbol']}**\n"
        f"💰 **P:** ${data['price']}\n"
        f"📉 **24h:** {data['change_24h']}%\n\n"
        f"📈 **H:** ${data['high']}\n"
        f"📉 **L:** ${data['low']}\n"
        f"📊 **Vol:** "
        f"{data['volume']} USDT\n"
    )


# ============================================================
# START COMMAND
# ============================================================

@dp.message(F.text == "/start")
async def start_handler(
    message: types.Message,
):

    await message.answer(
        "👋 به ربات SuperEx خوش آمدید.\n\n"
        "نماد ارز را ارسال کنید.\n\n"
        "مثال:\n"
        "`BTC`\n"
        "`ETH`\n"
        "`SOL`",
        parse_mode="Markdown",
    )


# ============================================================
# HEALTH / DEBUG COMMANDS
# ============================================================

@dp.message(F.text == "/health")
async def health_command(
    message: types.Message,
):

    session_status = (
        "OK"
        if HTTP_SESSION
        and not HTTP_SESSION.closed
        else "CLOSED"
    )

    await message.answer(
        "🟢 Bot status\n\n"
        f"HTTP Session: {session_status}\n"
        f"SuperEx API: {SUPEREX_API_BASE}\n"
        f"Cache prices: {len(PRICE_CACHE)}\n"
        f"Cache charts: {len(CHART_CACHE)}"
    )


@dp.message(
    F.text.startswith("/debug_kline")
)
async def debug_kline_command(
    message: types.Message,
):

    parts = message.text.split()

    symbol = (
        parts[1]
        if len(parts) >= 2
        else "BTC"
    )

    timeframe = (
        parts[2]
        if len(parts) >= 3
        else "1h"
    )

    if timeframe not in TIMEFRAME_MAP:

        await message.answer(
            "Timeframe معتبر نیست.\n"
            "1m / 15m / 1h / 4h / 1d"
        )

        return

    processing = await message.answer(
        "🔎 در حال تست مستقیم Kline SuperEx..."
    )

    try:

        records = (
            await fetch_superex_kline(
                symbol,
                timeframe,
            )
        )

        first = records[0]
        last = records[-1]

        await processing.edit_text(
            "✅ Kline OK\n\n"
            f"Symbol: {normalize_symbol(symbol)}\n"
            f"Timeframe: {timeframe}\n"
            f"Candles: {len(records)}\n\n"
            f"First close: {first['Close']}\n"
            f"Last close: {last['Close']}"
        )

    except Exception as exc:

        logger.exception(
            "Debug Kline failed"
        )

        await processing.edit_text(
            "❌ Kline FAILED\n\n"
            f"{str(exc)[:3000]}"
        )


# ============================================================
# TICKER HANDLER
# ============================================================

@dp.message(F.text)
async def handle_ticker_input(
    message: types.Message,
):

    if not message.text:
        return

    text = (
        message.text
        .strip()
        .upper()
    )

    if text.startswith("/"):
        return

    if not text.isalnum():
        return

    if len(text) > 15:
        return

    symbol = normalize_symbol(
        text
    )

    processing = await message.answer(
        "⏳ در حال دریافت اطلاعات SuperEx..."
    )

    # Start price and chart simultaneously
    price_task = asyncio.create_task(
        get_price_data_cached(
            symbol
        )
    )

    chart_task = asyncio.create_task(
        generate_chart_image(
            symbol,
            "1h",
        )
    )

    try:

        price_data = await price_task

        if "error" in price_data:

            chart_task.cancel()

            with suppress(
                asyncio.CancelledError
            ):
                await chart_task

            await processing.edit_text(
                f"❌ {price_data['error']}"
            )

            return

        caption = (
            build_price_caption(
                price_data
            )
        )

        try:

            chart_bytes = (
                await chart_task
            )

            photo = BufferedInputFile(
                chart_bytes,
                filename=(
                    f"{symbol}_1h.png"
                ),
            )

            await message.answer_photo(
                photo=photo,
                caption=caption,
                parse_mode="Markdown",
                reply_markup=(
                    get_price_keyboard(
                        symbol
                    )
                ),
            )

        except Exception as chart_exc:

            logger.exception(
                "Chart generation error"
            )

            await message.answer(
                caption
                + "\n\n"
                + "⚠️ چارت SuperEx "
                  "در حال حاضر در دسترس نیست.",
                parse_mode="Markdown",
                reply_markup=(
                    get_price_keyboard(
                        symbol
                    )
                ),
            )

        finally:

            with suppress(
                Exception
            ):
                await processing.delete()

    except Exception as exc:

        logger.exception(
            "Ticker handler failed"
        )

        with suppress(
            Exception
        ):
            chart_task.cancel()

        with suppress(
            Exception
        ):
            await processing.edit_text(
                "❌ خطایی هنگام دریافت "
                "اطلاعات رخ داد."
            )


# ============================================================
# TIMEFRAME CALLBACK
# ============================================================

@dp.callback_query(
    ChartCallback.filter()
)
async def process_chart_timeframe(
    query: types.CallbackQuery,
    callback_data: ChartCallback,
):

    symbol = normalize_symbol(
        callback_data.symbol
    )

    timeframe = callback_data.timeframe

    if timeframe not in TIMEFRAME_MAP:

        await query.answer(
            "تایم‌فریم نامعتبر است.",
            show_alert=True,
        )

        return

    await query.answer(
        f"در حال دریافت چارت {timeframe}..."
    )

    try:

        chart_bytes = (
            await generate_chart_image(
                symbol,
                timeframe,
            )
        )

        new_photo = types.InputMediaPhoto(
            media=BufferedInputFile(
                chart_bytes,
                filename=(
                    f"{symbol}_{timeframe}.png"
                ),
            ),
            caption=(
                query.message.caption
                or ""
            ),
            parse_mode="Markdown",
        )

        await query.message.edit_media(
            media=new_photo,
            reply_markup=(
                get_price_keyboard(
                    symbol
                )
            ),
        )

    except Exception as exc:

        logger.exception(
            "Chart timeframe update failed"
        )

        await query.answer(
            "❌ چارت SuperEx در دسترس نیست.",
            show_alert=True,
        )


# ============================================================
# WEB SERVER
# ============================================================

async def health_check(
    request: web.Request,
):

    return web.json_response(
        {
            "status": "ok",
            "service": "superex-market-bot",
            "time": int(time.time()),
        }
    )


async def create_web_server():

    app = web.Application()

    app.router.add_get(
        "/",
        health_check,
    )

    app.router.add_get(
        "/health",
        health_check,
    )

    runner = web.AppRunner(
        app
    )

    await runner.setup()

    site = web.TCPSite(
        runner,
        "0.0.0.0",
        PORT,
    )

    await site.start()

    logger.info(
        "🌐 Web server started on port %s",
        PORT,
    )

    return runner


# ============================================================
# BOT STARTUP
# ============================================================

async def startup():

    await create_http_session()

    try:

        # Remove webhook before polling
        await bot.delete_webhook(
            drop_pending_updates=True
        )

        logger.info(
            "Webhook deleted successfully"
        )

    except Exception as exc:

        logger.warning(
            "Could not delete webhook: %s",
            exc,
        )


# ============================================================
# BOT SHUTDOWN
# ============================================================

async def shutdown():

    logger.info(
        "Shutting down bot..."
    )

    with suppress(
        Exception
    ):
        await close_http_session()

    with suppress(
        Exception
    ):
        await bot.session.close()

    with suppress(
        Exception
    ):
        CHART_RENDER_EXECUTOR.shutdown(
            wait=False,
            cancel_futures=True,
        )

    logger.info(
        "Shutdown complete"
    )


# ============================================================
# MAIN
# ============================================================

async def main():

    web_runner = None

    await startup()

    try:

        web_runner = (
            await create_web_server()
        )

        logger.info(
            "🚀 Starting Telegram polling..."
        )

        await dp.start_polling(
            bot,
            allowed_updates=[
                "message",
                "callback_query",
            ],
            handle_signals=True,
        )

    except TelegramConflictError:

        logger.error(
            "Telegram Conflict detected. "
            "Another process is polling "
            "the same bot token."
        )

        await notify_admin_error(
            "telegram_conflict",
            (
                "Another process is using "
                "getUpdates for this bot."
            ),
        )

        raise

    finally:

        if web_runner:

            with suppress(
                Exception
            ):
                await web_runner.cleanup()

        await shutdown()


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":

    try:

        asyncio.run(
            main()
        )

    except (
        KeyboardInterrupt,
        SystemExit,
    ):

        logger.info(
            "🛑 Bot stopped."
        )
