from coinalyze_api import get_future_markets

markets = get_future_markets()
for m in markets:
    if "BTC" in m["base_asset"]:
        print(m["exchange"], m["symbol"], m["base_asset"], m["quote_asset"])
