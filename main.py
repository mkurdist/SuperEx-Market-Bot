# ---------------------------------------------------------
# API Helper Functions
# ---------------------------------------------------------
def get_superex_headers() -> dict:
    """
    Generates dynamic security headers required by SuperEx API.
    """
    return {
        "accept": "*/*",
        "accept-language": "en",
        "client": "1",
        "nonce": uuid.uuid4().hex,
        "timestamp": str(int(time.time() * 1000)),
        "token": "",
        "content-type": "application/x-www-form-urlencoded"
    }

async def get_currency_id(symbol: str) -> str:
    """
    Fetches the currencyId for a given symbol directly from SuperEx main spot market.
    """
    base_symbol = symbol.upper().replace("_USDT", "").replace("USDT", "")
    
    # Return from cache if we already know the ID
    if base_symbol in SYMBOL_MAP_CACHE:
        return SYMBOL_MAP_CACHE[base_symbol]
        
    # Requesting the specific currency ID from the MAIN spot market (removed 'free-')
    url = f"https://api.superexchang.com/spot/v3/symbols?currency={base_symbol}"
    
    async with aiohttp.ClientSession() as session:
        try:
            # Added security headers here!
            async with session.get(url, headers=get_superex_headers()) as response:
                if response.status == 200:
                    data = await response.json()
                    
                    # Parse the response to find the correct currencyId
                    for item in data.get("data", []):
                        currency = str(item.get("currency", "")).upper()
                        currency_id = str(item.get("currencyId"))
                        
                        if currency == base_symbol and currency_id:
                            SYMBOL_MAP_CACHE[currency] = currency_id
                            return currency_id
                else:
                    logging.error(f"SuperEx API error: Status {response.status}")
        except Exception as e:
            logging.error(f"Error fetching symbols mapping: {e}")
            
    return None

async def fetch_binance_fallback(symbol: str) -> dict:
    """
    Robust fallback to Binance API if the symbol is not available on SuperEx.
    Binance is much more reliable on cloud servers than CoinGecko.
    """
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
            logging.error(f"Binance fallback error: {e}")
            
    return {"error": "Symbol not found on SuperEx or Binance."}

async def fetch_price_data(symbol: str) -> dict:
    """
    Fetches the latest 24h ticker data from SuperEx.
    Triggers Binance fallback if symbol is missing.
    """
    currency_id = await get_currency_id(symbol)
    
    if not currency_id:
        logging.warning(f"Symbol {symbol} not found on SuperEx. Trying Binance fallback...")
        return await fetch_binance_fallback(symbol)

    # Targeting the MAIN spot market (removed 'free-')
    url = f"https://api.superexchang.com/spot/v3/market/twentyfour?currencyId={currency_id}"
    
    async with aiohttp.ClientSession() as session:
        try:
            # Added security headers here!
            async with session.get(url, headers=get_superex_headers()) as response:
                if response.status == 200:
                    res_data = await response.json()
                    return {
                        "symbol": symbol.upper().replace("_USDT", "").replace("USDT", ""),
                        "price": str(res_data.get("hqzrsp", "0.0")),
                        "change_24h": str(res_data.get("change", "0.0")),
                        "high": str(res_data.get("maxPrice", "0.0")),
                        "low": str(res_data.get("minPrice", "0.0")),
                        "volume": str(res_data.get("sumAmount", "0.0")),
                        "source": "SuperEx"
                    }
        except Exception as e:
            logging.error(f"Error fetching ticker for {symbol}: {e}")
            
    return await fetch_binance_fallback(symbol)

async def generate_chart_image(symbol: str, timeframe: str) -> bytes:
    """
    Fetches real Kline (OHLCV) data from SuperEx and generates 
    a professional candlestick chart.
    """
    currency_id = await get_currency_id(symbol)
    if not currency_id:
        raise ValueError("Currency ID not found for chart generation.")

    time_type = TIMEFRAME_MAP.get(timeframe, 3600)
    
    # Targeting the MAIN spot market (removed 'free-')
    url = f"https://api.superexchang.com/spot/v3/klines?currencyId={currency_id}&timeType={time_type}&limit=60"
    
    async with aiohttp.ClientSession() as session:
        # Added security headers here!
        async with session.get(url, headers=get_superex_headers()) as response:
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

    # Styling the chart
    mc = mpf.make_marketcolors(up='#00ff00', down='#ff0000', edge='inherit', wick='inherit', volume='in', ohlc='i')
    s = mpf.make_mpf_style(marketcolors=mc, base_mpf_style='nightclouds', facecolor='#1e1e1e', edgecolor='#444444', figcolor='#121212')

    buf = io.BytesIO()
    mpf.plot(
        df, 
        type='candle', 
        style=s, 
        volume=False, 
        title=f"\n{symbol.upper().replace('USDT', '')}/USDT | {timeframe}",
        tight_layout=True,
        savefig=dict(fname=buf, dpi=100, bbox_inches='tight')
    )
    
    buf.seek(0)
    image_bytes = buf.getvalue()
    buf.close()
    
    return image_bytes
