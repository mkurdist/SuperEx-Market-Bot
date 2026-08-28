async def get_currency_id(symbol: str) -> str:
    """
    Fetches the currencyId for a given symbol directly from SuperEx main spot market.
    Includes deep logging to capture the exact JSON structure.
    """
    base_symbol = symbol.upper().replace("_USDT", "").replace("USDT", "")
    
    # Return from cache if we already know the ID
    if base_symbol in SYMBOL_MAP_CACHE:
        return SYMBOL_MAP_CACHE[base_symbol]
        
    # Requesting the specific currency ID from the MAIN spot market
    url = f"https://api.superexchang.com/spot/v3/symbols?currency={base_symbol}"
    
    async with aiohttp.ClientSession() as session:
        try:
            # Added security headers here!
            async with session.get(url, headers=get_superex_headers()) as response:
                if response.status == 200:
                    data = await response.json()
                    
                    # ---> THE MAGIC DEBUG LINE <---
                    logging.warning(f"🔍 RAW API RESPONSE FOR {base_symbol}: {data}")
                    
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
