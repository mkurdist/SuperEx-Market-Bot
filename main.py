async def get_currency_id(symbol: str) -> str:
    """
    Fetches the currencyId for a given symbol directly from SuperEx main spot market.
    Includes deep logging to capture the exact JSON structure.
    """
    base_symbol = symbol.upper().replace("_USDT", "").replace("USDT", "")
    
    # Return from cache if we already know the ID
    if base_symbol in SYMBOL_MAP_CACHE:
        return SYMBOL_MAP_CACHE[base_symbol]
        
    # Requesting the currency config from the MAIN spot market (found via cURL)
    url = "https://api.superexchang.com/spot/spot/currency/config"
    
    async with aiohttp.ClientSession() as session:
        try:
            # Added security headers here!
            async with session.get(url, headers=get_superex_headers()) as response:
                if response.status == 200:
                    data = await response.json()
                    
                    # ---> THE MAGIC DEBUG LINE <---
                    logging.warning(f"🔍 RAW API RESPONSE FOR {base_symbol}: {data}")
                    
                    # Parse the response to find the correct currencyId
                    # Handle both list and dict response types robustly
                    items = data.get("data", []) if isinstance(data, dict) else data
                    
                    for item in items:
                        if isinstance(item, dict):
                            currency_raw = str(item.get("currency", "")).upper()
                            clean_currency = currency_raw.replace("_USDT", "").replace("USDT", "")
                            currency_id = str(item.get("currencyId", ""))
                            
                            # Cache every symbol we find to speed up future requests
                            if clean_currency and currency_id and currency_id != "None":
                                SYMBOL_MAP_CACHE[clean_currency] = currency_id
                    
                    return SYMBOL_MAP_CACHE.get(base_symbol)
                else:
                    logging.error(f"SuperEx API error: Status {response.status}")
        except Exception as e:
            logging.error(f"Error fetching symbols mapping: {e}")
            
    return None
