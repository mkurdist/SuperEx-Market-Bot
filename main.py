import asyncio
import io
import json
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import suppress
from typing import Optional

import aiohttp
import pandas as pd

import matplotlib
matplotlib.use("Agg")

import matplotlib.pyplot as plt
import mplfinance as mpf

from PIL import Image, ImageChops

from aiogram import Bot, Dispatcher, F, types
from aiogram.exceptions import TelegramConflictError
from aiogram.types import (
    BufferedInputFile,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    CallbackQuery,
)

from aiohttp import web
from dotenv import load_dotenv


# ============================================================
# ENV
# ============================================================

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()

PORT = int(
    os.getenv("PORT", "10000")
)

ADMIN_CHAT_ID = int(
    os.getenv("ADMIN_CHAT_ID", "0")
)

SUPEREX_API_BASE = os.getenv(
    "SUPEREX_API_BASE",
    "https://api.superexchang.com/api",
).rstrip("/")


# ============================================================
# CONFIG
# ============================================================

HTTP_TIMEOUT = float(
    os.getenv("HTTP_TIMEOUT", "15")
)

HTTP_RETRIES = int(
    os.getenv("HTTP_RETRIES", "2")
)

KLINE_LIMIT = int(
    os.getenv("KLINE_LIMIT", "100")
)

PRICE_CACHE_TTL = float(
    os.getenv("PRICE_CACHE_TTL", "3")
)

CHART_CACHE_TTL = float(
    os.getenv("CHART_CACHE_TTL", "10")
)

MAX_CONCURRENT_CHARTS = int(
    os.getenv("MAX_CONCURRENT_CHARTS", "2")
)

CHART_RENDER_WORKERS = int(
    os.getenv("CHART_RENDER_WORKERS", "2")
)


if not BOT_TOKEN:
    raise RuntimeError(
        "BOT_TOKEN is not configured."
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

logger = logging.getLogger(
    "superex-market-bot"
)


# ============================================================
# TELEGRAM
# ============================================================

bot = Bot(
    token=BOT_TOKEN
)

dp = Dispatcher()


# ============================================================
# HTTP
# ============================================================

http_session: Optional[
    aiohttp.ClientSession
] = None


# ============================================================
# TIMEFRAMES
# ============================================================

TIMEFRAME_SECONDS = {

    "1m": 60,

    "5m": 300,

    "15m": 900,

    "30m": 1800,

    "1h": 3600,

    "4h": 14400,

    "12h": 43200,

    "1d": 86400,

    "1w": 604800,
}


# ============================================================
# CACHE
# ============================================================

PRICE_CACHE = {}

CHART_CACHE = {}

CURRENCY_ID_CACHE = {}

CACHE_LOCK = asyncio.Lock()


# ============================================================
# CHART LOCKS
# ============================================================

KLINE_LOCKS = {}


def get_kline_lock(
    symbol: str,
    timeframe: str,
):

    key = (
        symbol,
        timeframe,
    )

    if key not in KLINE_LOCKS:

        KLINE_LOCKS[key] = (
            asyncio.Lock()
        )

    return KLINE_LOCKS[key]


# ============================================================
# CHART THREAD POOL
# ============================================================

CHART_EXECUTOR = (
    ThreadPoolExecutor(
        max_workers=CHART_RENDER_WORKERS,
        thread_name_prefix="chart",
    )
)

CHART_SEMAPHORE = asyncio.Semaphore(
    MAX_CONCURRENT_CHARTS
)


# ============================================================
# ADMIN ALERT
# ============================================================

ADMIN_ALERT_LAST = {}

ADMIN_ALERT_COOLDOWN = 300


async def notify_admin(
    error_type: str,
    message: str,
):

    if not ADMIN_CHAT_ID:
        return

    now = time.monotonic()

    last = ADMIN_ALERT_LAST.get(
        error_type,
        0,
    )

    if (
        now - last
        < ADMIN_ALERT_COOLDOWN
    ):
        return

    ADMIN_ALERT_LAST[
        error_type
    ] = now

    text = (
        "⚠️ SuperEx Bot Error\n\n"
        f"Type: {error_type}\n\n"
        f"{message[:3500]}"
    )

    try:

        await bot.send_message(
            ADMIN_CHAT_ID,
            text,
        )

    except Exception as exc:

        logger.error(
            "Admin notification failed: %r",
            exc,
        )


# ============================================================
# SUPEREX HEADERS
# ============================================================

def superex_headers():

    return {

        "Accept": "application/json",

        "language": "en",

        "User-Agent":
            (
                "Mozilla/5.0 "
                "(Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 "
                "(KHTML, like Gecko) "
                "Chrome/151.0.0.0 "
                "Safari/537.36"
            ),

        "Connection":
            "keep-alive",
    }


# ============================================================
# HTTP SESSION CREATE
# ============================================================

async def create_http_session():

    global http_session

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

    http_session = (
        aiohttp.ClientSession(
            timeout=timeout,
            connector=connector,
        )
    )

    logger.info(
        "SuperEx HTTP session initialized"
    )


# ============================================================
# HTTP SESSION CLOSE
# ============================================================

async def close_http_session():

    global http_session

    if http_session:

        with suppress(Exception):

            await http_session.close()

        http_session = None

        logger.info(
            "SuperEx HTTP session closed"
        )


# ============================================================
# NORMALIZE SYMBOL
# ============================================================

def normalize_symbol(
    symbol: str,
) -> str:

    symbol = (
        str(symbol)
        .strip()
        .upper()
        .replace("/", "")
        .replace("-", "")
        .replace("_", "")
        .replace(" ", "")
    )

    if symbol.endswith("USDT"):

        symbol = symbol[:-4]

    return symbol


# ============================================================
# CACHE GET
# ============================================================

def cache_get(
    cache,
    key,
    ttl,
):

    value = cache.get(key)

    if value is None:
        return None

    timestamp, data = value

    if (
        time.monotonic()
        - timestamp
        > ttl
    ):

        cache.pop(
            key,
            None,
        )

        return None

    return data


# ============================================================
# CACHE SET
# ============================================================

def cache_set(
    cache,
    key,
    value,
    max_entries=300,
):

    cache[key] = (
        time.monotonic(),
        value,
    )

    if len(cache) > max_entries:

        oldest_key = min(
            cache,
            key=lambda k:
                cache[k][0],
        )

        cache.pop(
            oldest_key,
            None,
        )


# ============================================================
# SUPEREX GET
# ============================================================

async def superex_get(
    path: str,
    params: dict,
):

    global http_session

    if (
        http_session is None
        or http_session.closed
    ):

        await create_http_session()

    if path.startswith("/resource"):

        url = (
            SUPEREX_API_BASE.replace("/api", "")
            + path
        )

    else:

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
                "SUPEREX GET | "
                "attempt=%s | "
                "url=%s | "
                "params=%s",
                attempt + 1,
                url,
                params,
            )

            async with (
                http_session.get(
                    url,
                    params=params,
                    headers=superex_headers(),
                )
            ) as response:

                body = (
                    await response.text()
                )

                logger.info(
                    "SUPEREX RESPONSE | "
                    "status=%s | "
                    "url=%s | "
                    "body=%s",
                    response.status,
                    url,
                    body[:4000],
                )

                if response.status != 200:

                    last_error = (
                        f"HTTP {response.status}: "
                        f"{body[:1000]}"
                    )

                    if response.status in (
                        400,
                        401,
                        403,
                        404,
                    ):

                        break

                    if attempt < HTTP_RETRIES:

                        await asyncio.sleep(
                            0.7
                            * (attempt + 1)
                        )

                    continue

                try:

                    return json.loads(
                        body
                    )

                except json.JSONDecodeError:

                    raise RuntimeError(
                        "SuperEx returned "
                        "invalid JSON: "
                        f"{body[:1000]}"
                    )

        except asyncio.TimeoutError as exc:

            last_error = (
                "SuperEx timeout"
            )

            logger.warning(
                "SuperEx timeout: %r",
                exc,
            )

        except aiohttp.ClientError as exc:

            last_error = repr(exc)

            logger.warning(
                "SuperEx network error: %r",
                exc,
            )

        except Exception as exc:

            last_error = repr(exc)

            logger.exception(
                "SuperEx GET unexpected error"
            )

            break

        if attempt < HTTP_RETRIES:

            await asyncio.sleep(
                0.7
                * (attempt + 1)
            )

    raise RuntimeError(
        last_error
        or "Unknown SuperEx API error"
    )


# ============================================================
# PRICE
# ============================================================

async def fetch_price(
    symbol: str,
):

    symbol = normalize_symbol(
        symbol
    )

    cache_key = symbol

    async with CACHE_LOCK:

        cached = cache_get(
            PRICE_CACHE,
            cache_key,
            PRICE_CACHE_TTL,
        )

    if cached is not None:

        return cached

    path = (
        "/resource/v3/public/"
        "currency/new"
    )

    params = {
        "currency":
            symbol.lower(),
    }

    try:

        response = await superex_get(
            path,
            params,
        )

        if not isinstance(
            response,
            dict,
        ):

            raise RuntimeError(
                "Invalid ticker response."
            )

        data = response.get(
            "data",
            {},
        )

        if not isinstance(
            data,
            dict,
        ):

            raise RuntimeError(
                "Invalid ticker data."
            )

        price = (
            data.get("newPrice")
            or data.get("price")
        )

        if price is None:

            raise RuntimeError(
                "Price not found in "
                "SuperEx response."
            )

        result = {

            "symbol":
                symbol,

            "price":
                str(price),

            "change_24h":
                str(
                    data.get(
                        "change",
                        "0",
                    )
                ),

            "high":
                str(
                    data.get(
                        "maxPrice",
                        data.get(
                            "high",
                            "0",
                        ),
                    )
                ),

            "low":
                str(
                    data.get(
                        "minPrice",
                        data.get(
                            "low",
                            "0",
                        ),
                    )
                ),

            "volume":
                str(
                    data.get(
                        "sumNumber",
                        data.get(
                            "volume",
                            "0",
                        ),
                    )
                ),
        }

        async with CACHE_LOCK:

            cache_set(
                PRICE_CACHE,
                cache_key,
                result,
            )

        return result

    except Exception as exc:

        logger.exception(
            "Price fetch failed for %s",
            symbol,
        )

        await notify_admin(
            "PRICE",
            (
                f"Symbol: {symbol}\n"
                f"Error: {exc}"
            ),
        )

        raise


# ============================================================
# CURRENCY ID
# ============================================================

async def get_currency_id(
    symbol: str,
) -> int:

    symbol = normalize_symbol(
        symbol
    )

    cached = (
        CURRENCY_ID_CACHE.get(
            symbol
        )
    )

    if cached is not None:

        return cached

    path = (
        "/free-spot/v3/symbols"
    )

    params = {
        "currency":
            symbol.lower(),
    }

    response = await superex_get(
        path,
        params,
    )

    logger.info(
        "SUPEREX SYMBOLS DATA | %s",
        str(response)[:5000],
    )

    if not isinstance(
        response,
        dict,
    ):

        raise RuntimeError(
            "Invalid symbols response."
        )

    data = response.get(
        "data"
    )

    if isinstance(
        data,
        dict,
    ):

        possible_lists = [
            data.get("list"),
            data.get("items"),
            data.get("rows"),
            data.get("data"),
        ]

        data = next(
            (
                x
                for x in possible_lists
                if isinstance(x, list)
            ),
            None,
        )

    if not isinstance(
        data,
        list,
    ):

        raise RuntimeError(
            "SuperEx symbols data "
            "is not a list."
        )

    for item in data:

        if not isinstance(
            item,
            dict,
        ):
            continue

        currency = str(
            item.get(
                "currency",
                "",
            )
        ).upper()

        if (
            currency
            == symbol
        ):

            currency_id = (
                item.get(
                    "currencyId"
                )
            )

            if (
                currency_id
                is None
            ):
                continue

            currency_id = int(
                currency_id
            )

            CURRENCY_ID_CACHE[
                symbol
            ] = currency_id

            logger.info(
                "Currency ID resolved | "
                "%s -> %s",
                symbol,
                currency_id,
            )

            return currency_id

    # Some SuperEx responses may use
    # currencyName instead of currency.
    for item in data:

        if not isinstance(
            item,
            dict,
        ):
            continue

        candidates = [
            item.get("currencyName"),
            item.get("name"),
            item.get("symbol"),
        ]

        for candidate in candidates:

            if (
                str(candidate)
                .upper()
                == symbol
            ):

                currency_id = (
                    item.get(
                        "currencyId"
                    )
                )

                if (
                    currency_id
                    is not None
                ):

                    currency_id = int(
                        currency_id
                    )

                    CURRENCY_ID_CACHE[
                        symbol
                    ] = currency_id

                    return currency_id

    raise RuntimeError(
        f"currencyId not found for "
        f"{symbol}"
    )


# ============================================================
# PARSE KLINE ROW
# ============================================================

def parse_kline_row(
    row,
):

    if isinstance(
        row,
        str,
    ):

        parts = row.split(",")

    elif isinstance(
        row,
        (list, tuple),
    ):

        parts = row

    elif isinstance(
        row,
        dict,
    ):

        timestamp = (
            row.get("timestamp")
            or row.get("time")
            or row.get("ts")
            or row.get("t")
        )

        high = (
            row.get("high")
            or row.get("h")
        )

        open_price = (
            row.get("open")
            or row.get("o")
        )

        low = (
            row.get("low")
            or row.get("l")
        )

        close = (
            row.get("close")
            or row.get("c")
        )

        volume = (
            row.get("volume")
            or row.get("v")
            or 0
        )

        parts = [
            timestamp,
            high,
            open_price,
            low,
            close,
            volume,
        ]

    else:

        return None

    if len(parts) < 6:

        return None

    try:

        timestamp = int(
            float(parts[0])
        )

        # SuperEx format:
        #
        # timestamp,
        # high,
        # open,
        # low,
        # close,
        # volume

        high = float(parts[1])

        open_price = float(
            parts[2]
        )

        low = float(parts[3])

        close = float(
            parts[4]
        )

        volume = float(
            parts[5]
        )

        # Automatically support
        # seconds / milliseconds.

        if timestamp < 10_000_000_000:

            date = pd.to_datetime(
                timestamp,
                unit="s",
                utc=True,
            )

        else:

            date = pd.to_datetime(
                timestamp,
                unit="ms",
                utc=True,
            )

        if (
            open_price <= 0
            or high <= 0
            or low <= 0
            or close <= 0
        ):

            return None

        if high < low:

            return None

        if high < open_price:

            return None

        if high < close:

            return None

        if low > open_price:

            return None

        if low > close:

            return None

        return {

            "Date":
                date,

            "Open":
                open_price,

            "High":
                high,

            "Low":
                low,

            "Close":
                close,

            "Volume":
                volume,
        }

    except (
        ValueError,
        TypeError,
        OverflowError,
    ) as exc:

        logger.warning(
            "Invalid Kline row: %r | %r",
            row,
            exc,
        )

        return None


# ============================================================
# BINANCE FALLBACK
# ============================================================

async def fetch_binance_kline(
    symbol: str,
    timeframe: str,
) -> list:

    global http_session

    if (
        http_session is None
        or http_session.closed
    ):

        await create_http_session()

    binance_symbol = (
        symbol.upper()
        + "USDT"
    )

    url = (
        "https://api.binance.com"
        "/api/v3/klines"
    )

    params = {

        "symbol":
            binance_symbol,

        "interval":
            timeframe,

        "limit":
            KLINE_LIMIT,
    }

    logger.info(
        "BINANCE FALLBACK REQUEST | "
        "symbol=%s | "
        "timeframe=%s",
        binance_symbol,
        timeframe,
    )

    async with (
        http_session.get(
            url,
            params=params,
        )
    ) as response:

        if response.status != 200:

            body = (
                await response.text()
            )

            raise RuntimeError(
                f"Binance HTTP {response.status}: "
                f"{body[:200]}"
            )

        data = (
            await response.json()
        )

    if not isinstance(
        data,
        list,
    ):

        raise RuntimeError(
            "Binance returned invalid data"
        )

    records = []

    for row in data:

        records.append(
            {
                "Date":
                    pd.to_datetime(
                        row[0],
                        unit="ms",
                        utc=True,
                    ),

                "Open":
                    float(row[1]),

                "High":
                    float(row[2]),

                "Low":
                    float(row[3]),

                "Close":
                    float(row[4]),

                "Volume":
                    float(row[5]),
            }
        )

    if not records:

        raise RuntimeError(
            "Binance returned empty klines"
        )

    return records


# ============================================================
# KLINE
# ============================================================

async def fetch_kline(
    symbol: str,
    timeframe: str,
):

    symbol = normalize_symbol(
        symbol
    )

    if (
        timeframe
        not in TIMEFRAME_SECONDS
    ):

        raise ValueError(
            f"Unsupported timeframe: "
            f"{timeframe}"
        )

    cache_key = (
        symbol,
        timeframe,
    )

    async with CACHE_LOCK:

        cached = cache_get(
            CHART_CACHE,
            cache_key,
            CHART_CACHE_TTL,
        )

    # We cache parsed candles here
    # temporarily only if available.

    if (
        cached is not None
        and isinstance(
            cached,
            list,
        )
        and cached
        and isinstance(
            cached[0],
            dict,
        )
        and "Open" in cached[0]
    ):

        return cached

    lock = get_kline_lock(
        symbol,
        timeframe,
    )

    async with lock:

        # Check cache again after lock
        async with CACHE_LOCK:

            cached = cache_get(
                CHART_CACHE,
                cache_key,
                CHART_CACHE_TTL,
            )

        if (
            cached is not None
            and isinstance(
                cached,
                list,
            )
            and cached
            and "Open" in cached[0]
        ):

            return cached

        records = None

        try:

            currency_id = (
                await get_currency_id(
                    symbol
                )
            )

            time_type = (
                TIMEFRAME_SECONDS[
                    timeframe
                ]
            )

            path = (
                "/free-spot/v3/klines"
            )

            params = {

                "currencyId":
                    currency_id,

                "timeType":
                    time_type,

                "limit":
                    KLINE_LIMIT,
            }

            logger.info(
                "KLINE REQUEST | "
                "symbol=%s | "
                "currencyId=%s | "
                "timeframe=%s | "
                "timeType=%s",
                symbol,
                currency_id,
                timeframe,
                time_type,
            )

            response = await superex_get(
                path,
                params,
            )

            logger.info(
                "KLINE RAW RESPONSE | %s",
                str(response)[:6000],
            )

            if not isinstance(
                response,
                dict,
            ):

                raise RuntimeError(
                    "Kline response is not "
                    "a JSON object."
                )

            code = response.get(
                "code"
            )

            if (
                code is not None
                and str(code)
                not in (
                    "0",
                    "200",
                )
            ):

                raise RuntimeError(
                    "SuperEx Kline error: "
                    f"code={code}, "
                    f"msg={response.get('msg')}"
                )

            data = response.get(
                "data"
            )

            if isinstance(
                data,
                dict,
            ):

                for key in (
                    "data",
                    "list",
                    "rows",
                    "items",
                    "klines",
                ):

                    if isinstance(
                        data.get(key),
                        list,
                    ):

                        data = data[key]

                        break

            if not isinstance(
                data,
                list,
            ):

                raise RuntimeError(
                    "SuperEx Kline data "
                    "is not a list."
                )

            if not data:

                raise RuntimeError(
                    "SuperEx returned "
                    "empty Kline data."
                )

            records_temp = []

            for row in data:

                parsed = (
                    parse_kline_row(row)
                )

                if parsed:

                    records_temp.append(
                        parsed
                    )

            if not records_temp:

                raise RuntimeError(
                    "Kline rows were received "
                    "but none could be parsed."
                )

            df = pd.DataFrame(
                records_temp
            )

            df = (
                df.drop_duplicates(
                    subset=["Date"],
                    keep="last",
                )
                .sort_values(
                    "Date"
                )
            )

            df = df[
                [
                    "Date",
                    "Open",
                    "High",
                    "Low",
                    "Close",
                    "Volume",
                ]
            ]

            if df.empty:

                raise RuntimeError(
                    "Kline DataFrame is empty."
                )

            logger.info(
                "KLINE SUCCESS | "
                "symbol=%s | "
                "timeframe=%s | "
                "candles=%s | "
                "first=%s | "
                "last=%s",
                symbol,
                timeframe,
                len(df),
                df.iloc[0]["Date"],
                df.iloc[-1]["Date"],
            )

            records = df.to_dict(
                orient="records"
            )

        except Exception as exc:

            logger.warning(
                "SuperEx Kline failed for %s. "
                "Falling back to Binance. Error: %r",
                symbol,
                exc,
            )

            records = (
                await fetch_binance_kline(
                    symbol,
                    timeframe,
                )
            )

        async with CACHE_LOCK:

            cache_set(
                CHART_CACHE,
                cache_key,
                records,
                max_entries=300,
            )

        return records


# ============================================================
# CHART STYLE
# ============================================================

MARKET_COLORS = (
    mpf.make_marketcolors(
        up="#00d964",
        down="#ff3b30",
        edge="inherit",
        wick="inherit",
        volume="inherit",
    )
)

CHART_STYLE = (
    mpf.make_mpf_style(
        base_mpf_style="nightclouds",
        marketcolors=MARKET_COLORS,
        facecolor="#0b0b0b",
        figcolor="#0b0b0b",
        gridcolor="#242424",
        gridstyle="--",
        rc={
            "font.family":
                "DejaVu Sans",
            "axes.titlecolor":
                "#ffffff",
            "axes.labelcolor":
                "#cccccc",
            "xtick.color":
                "#999999",
            "ytick.color":
                "#999999",
        },
    )
)


# ============================================================
# CROP
# ============================================================

def crop_image(
    data: bytes,
) -> bytes:

    image = Image.open(
        io.BytesIO(data)
    ).convert("RGB")

    background = Image.new(
        "RGB",
        image.size,
        (11, 11, 11),
    )

    diff = ImageChops.difference(
        image,
        background,
    )

    bbox = diff.getbbox()

    if bbox:

        left, top, right, bottom = (
            bbox
        )

        padding = 12

        left = max(
            0,
            left - padding,
        )

        top = max(
            0,
            top - padding,
        )

        right = min(
            image.width,
            right + padding,
        )

        bottom = min(
            image.height,
            bottom + padding,
        )

        image = image.crop(
            (
                left,
                top,
                right,
                bottom,
            )
        )

    output = io.BytesIO()

    image.save(
        output,
        format="PNG",
        optimize=True,
    )

    return output.getvalue()


# ============================================================
# RENDER CHART SYNC
# ============================================================

def render_chart_sync(
    records,
    symbol,
    timeframe,
):

    df = pd.DataFrame(
        records
    )

    if df.empty:

        raise ValueError(
            "No candles to render."
        )

    df["Date"] = pd.to_datetime(
        df["Date"],
        utc=True,
    )

    df = df.sort_values(
        "Date"
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

    df.index = (
        df.index
        .tz_convert(None)
    )

    if len(df) > KLINE_LIMIT:

        df = df.iloc[
            -KLINE_LIMIT:
        ]

    fig, axes = mpf.plot(
        df,
        type="candle",
        style=CHART_STYLE,
        volume=False,
        returnfig=True,
        figsize=(13, 7.5),
        title=(
            f"{symbol}/USDT   •   "
            f"{timeframe}"
        ),
        ylabel="USDT",
        datetime_format=(
            "%H:%M"
            if timeframe != "1d"
            else "%Y-%m-%d"
        ),
        xrotation=0,
        tight_layout=False,
    )

    try:

        fig.subplots_adjust(
            top=0.93,
            bottom=0.11,
            left=0.065,
            right=0.985,
        )

        fig.text(
            0.5,
            0.025,
            "@SuperExPrice_bot",
            ha="center",
            va="center",
            fontsize=10,
            color="#888888",
        )

        output = io.BytesIO()

        fig.savefig(
            output,
            format="png",
            dpi=140,
            facecolor=fig.get_facecolor(),
            edgecolor="none",
            bbox_inches=None,
        )

        return crop_image(
            output.getvalue()
        )

    finally:

        plt.close(fig)


# ============================================================
# GENERATE CHART
# ============================================================

async def generate_chart(
    symbol,
    timeframe,
):

    symbol = normalize_symbol(
        symbol
    )

    async with CHART_SEMAPHORE:

        try:

            records = (
                await fetch_kline(
                    symbol,
                    timeframe,
                )
            )

            loop = (
                asyncio.get_running_loop()
            )

            image = (
                await loop.run_in_executor(
                    CHART_EXECUTOR,
                    render_chart_sync,
                    records,
                    symbol,
                    timeframe,
                )
            )

            return image

        except Exception as exc:

            logger.exception(
                "Chart generation failed | "
                "%s | %s",
                symbol,
                timeframe,
            )

            await notify_admin(
                "CHART",
                (
                    f"Symbol: {symbol}\n"
                    f"Timeframe: {timeframe}\n"
                    f"Error: {exc}"
                ),
            )

            raise


# ============================================================
# CALLBACK DATA
# ============================================================

def chart_callback(
    symbol,
    timeframe,
):

    return (
        f"chart:"
        f"{normalize_symbol(symbol)}:"
        f"{timeframe}"
    )


# ============================================================
# KEYBOARD
# ============================================================

def chart_keyboard(
    symbol,
):

    symbol = normalize_symbol(
        symbol
    )

    timeframes = [
        "1m",
        "5m",
        "15m",
        "30m",
        "1h",
        "4h",
        "12h",
        "1d",
        "1w",
    ]

    rows = []

    row = []

    for timeframe in timeframes:

        row.append(
            InlineKeyboardButton(
                text=timeframe,
                callback_data=(
                    chart_callback(
                        symbol,
                        timeframe,
                    )
                ),
            )
        )

        if len(row) == 5:

            rows.append(row)

            row = []

    if row:

        rows.append(row)

    rows.append(
        [
            InlineKeyboardButton(
                text="👥 گروه SuperEx",
                url=(
                    "https://t.me/SuperexIR"
                ),
            ),
            InlineKeyboardButton(
                text="🏦 ثبت‌نام SuperEx",
                url=(
                    "https://app.superex.live/"
                    "register?"
                    "invitationCode="
                    "VQK2N6DDS"
                ),
            ),
        ]
    )

    return InlineKeyboardMarkup(
        inline_keyboard=rows
    )


# ============================================================
# PRICE CAPTION
# ============================================================

def price_caption(
    data,
):

    return (
        f"🪙 <b>{data['symbol']}/USDT</b>\n\n"
        f"💰 <b>Price:</b> "
        f"${data['price']}\n"
        f"📊 <b>24h:</b> "
        f"{data['change_24h']}%\n\n"
        f"📈 <b>High:</b> "
        f"${data['high']}\n"
        f"📉 <b>Low:</b> "
        f"${data['low']}\n"
        f"📊 <b>Volume:</b> "
        f"{data['volume']} USDT\n\n"
        f"🏦 <b>Source:</b> SuperEx"
    )


# ============================================================
# START
# ============================================================

@dp.message(
    F.text == "/start"
)
async def start_handler(
    message: types.Message,
):

    await message.answer(
        "👋 <b>SuperEx Market Bot</b>\n\n"
        "نام ارز را ارسال کن تا قیمت "
        "و چارت مستقیم SuperEx نمایش "
        "داده شود.\n\n"
        "مثال:\n"
        "<code>BTC</code>\n"
        "<code>ETH</code>\n"
        "<code>SOL</code>\n\n"
        "تایم‌فریم‌های چارت:\n"
        "1m • 5m • 15m • 30m • 1h • "
        "4h • 12h • 1d • 1w",
        parse_mode="HTML",
    )


# ============================================================
# HEALTH
# ============================================================

@dp.message(
    F.text == "/health"
)
async def health_handler(
    message: types.Message,
):

    session_ok = (
        http_session is not None
        and not http_session.closed
    )

    await message.answer(
        "🟢 <b>Bot Health</b>\n\n"
        f"HTTP Session: "
        f"{'OK' if session_ok else 'DOWN'}\n"
        f"SuperEx: OK\n"
        f"Price Cache: "
        f"{len(PRICE_CACHE)}\n"
        f"Chart Cache: "
        f"{len(CHART_CACHE)}\n"
        f"Currency Cache: "
        f"{len(CURRENCY_ID_CACHE)}",
        parse_mode="HTML",
    )


# ============================================================
# DEBUG KLINE
# ============================================================

@dp.message(
    F.text.startswith("/debug_kline")
)
async def debug_kline_handler(
    message: types.Message,
):

    parts = (
        message.text
        .strip()
        .split()
    )

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

    symbol = normalize_symbol(
        symbol
    )

    if (
        timeframe
        not in TIMEFRAME_SECONDS
    ):

        await message.answer(
            "❌ Timeframe نامعتبر است.\n\n"
            "معتبر:\n"
            "1m 5m 15m 30m 1h "
            "4h 12h 1d 1w"
        )

        return

    status = await message.answer(
        "🔎 در حال تست مستقیم "
        "Kline SuperEx..."
    )

    try:

        records = (
            await fetch_kline(
                symbol,
                timeframe,
            )
        )

        first = records[0]

        last = records[-1]

        await status.edit_text(
            "✅ <b>Kline OK</b>\n\n"
            f"Symbol: <b>{symbol}</b>\n"
            f"Timeframe: <b>{timeframe}</b>\n"
            f"Candles: "
            f"<b>{len(records)}</b>\n\n"
            f"First:\n"
            f"{first['Date']} | "
            f"Close={first['Close']}\n\n"
            f"Last:\n"
            f"{last['Date']} | "
            f"Close={last['Close']}",
            parse_mode="HTML",
        )

    except Exception as exc:

        logger.exception(
            "DEBUG KLINE FAILED"
        )

        await status.edit_text(
            "❌ <b>Kline FAILED</b>\n\n"
            f"<code>{str(exc)[:3500]}</code>",
            parse_mode="HTML",
        )


# ============================================================
# USER SYMBOL
# ============================================================

@dp.message(
    F.text
)
async def symbol_handler(
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

    # Allow:
    #
    # BTC
    # BTCUSDT
    # BTC/USDT
    # BTC-USDT

    symbol = normalize_symbol(
        text
    )

    if not symbol:

        return

    if not symbol.isalnum():

        return

    if len(symbol) > 15:

        return

    status = await message.answer(
        "⏳ در حال دریافت اطلاعات "
        "از SuperEx..."
    )

    try:

        price_task = asyncio.create_task(
            fetch_price(
                symbol
            )
        )

        chart_task = asyncio.create_task(
            generate_chart(
                symbol,
                "1h",
            )
        )

        price = await price_task

        image = None

        try:

            image = await chart_task

        except Exception:

            image = None

        if image:

            photo = BufferedInputFile(
                image,
                filename=(
                    f"{symbol}_1h.png"
                ),
            )

            await message.answer_photo(
                photo=photo,
                caption=price_caption(
                    price
                ),
                parse_mode="HTML",
                reply_markup=(
                    chart_keyboard(
                        symbol
                    )
                ),
            )

        else:

            await message.answer(
                price_caption(
                    price
                )
                + (
                    "\n\n"
                    "⚠️ چارت SuperEx "
                    "فعلاً در دسترس نیست."
                ),
                parse_mode="HTML",
                reply_markup=(
                    chart_keyboard(
                        symbol
                    )
                ),
            )

    except Exception as exc:

        logger.exception(
            "Symbol handler failed"
        )

        await message.answer(
            "❌ دریافت اطلاعات SuperEx "
            "با خطا مواجه شد.\n\n"
            "لطفاً نماد را بررسی کن.",
        )

    finally:

        with suppress(
            Exception
        ):

            await status.delete()


# ============================================================
# CHART CALLBACK
# ============================================================

@dp.callback_query(
    F.data.startswith("chart:")
)
async def chart_callback_handler(
    query: CallbackQuery,
):

    if not query.data:

        return

    parts = (
        query.data.split(":")
    )

    if len(parts) != 3:

        await query.answer(
            "دکمه نامعتبر است.",
            show_alert=True,
        )

        return

    symbol = normalize_symbol(
        parts[1]
    )

    timeframe = parts[2]

    if (
        timeframe
        not in TIMEFRAME_SECONDS
    ):

        await query.answer(
            "تایم‌فریم نامعتبر است.",
            show_alert=True,
        )

        return

    await query.answer(
        f"در حال دریافت چارت {timeframe}..."
    )

    try:

        image = await generate_chart(
            symbol,
            timeframe,
        )

        media = (
            types.InputMediaPhoto(
                media=BufferedInputFile(
                    image,
                    filename=(
                        f"{symbol}_"
                        f"{timeframe}.png"
                    ),
                ),
                caption=(
                    query.message.caption
                    if query.message
                    and query.message.caption
                    else ""
                ),
                parse_mode="HTML",
            )
        )

        await query.message.edit_media(
            media=media,
            reply_markup=(
                chart_keyboard(
                    symbol
                )
            ),
        )

    except Exception as exc:

        logger.exception(
            "Chart callback failed"
        )

        await query.answer(
            "❌ چارت SuperEx "
            "در دسترس نیست.",
            show_alert=True,
        )


# ============================================================
# WEB SERVER
# ============================================================

async def health_http(
    request: web.Request,
):

    return web.json_response(
        {
            "status": "ok",
            "service":
                "superex-market-bot",
            "timestamp":
                int(time.time()),
        }
    )


async def start_web_server():

    app = web.Application()

    app.router.add_get(
        "/",
        health_http,
    )

    app.router.add_get(
        "/health",
        health_http,
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
        "🌐 Web server started on "
        "0.0.0.0:%s",
        PORT,
    )

    return runner


# ============================================================
# STARTUP
# ============================================================

async def startup():

    await create_http_session()

    try:

        await bot.delete_webhook(
            drop_pending_updates=True
        )

        logger.info(
            "Telegram webhook deleted."
        )

    except Exception as exc:

        logger.warning(
            "Webhook deletion failed: %r",
            exc,
        )


# ============================================================
# SHUTDOWN
# ============================================================

async def shutdown():

    logger.info(
        "Shutting down..."
    )

    await close_http_session()

    with suppress(Exception):

        await bot.session.close()

    with suppress(Exception):

        CHART_EXECUTOR.shutdown(
            wait=False,
            cancel_futures=True,
        )

    logger.info(
        "Shutdown complete."
    )


# ============================================================
# MAIN
# ============================================================

async def main():

    web_runner = None

    await startup()

    try:

        web_runner = (
            await start_web_server()
        )

        logger.info(
            "🚀 Telegram polling starting..."
        )

        await dp.start_polling(
            bot,
            allowed_updates=[
                "message",
                "callback_query",
            ],
        )

    except TelegramConflictError:

        logger.error(
            "Telegram Conflict: "
            "another instance is using "
            "getUpdates."
        )

        await notify_admin(
            "TELEGRAM_CONFLICT",
            (
                "Another process/instance "
                "is polling the same bot."
            ),
        )

        raise

    finally:

        if web_runner:

            with suppress(Exception):

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

    except KeyboardInterrupt:

        logger.info(
            "Bot stopped by keyboard."
        )

    except TelegramConflictError:

        logger.error(
            "Bot stopped because "
            "another polling instance "
            "is active."
        )

    except Exception:

        logger.exception(
            "Fatal application error."
        )
