import os

# config.py
TARGET_LOOP_SECONDS = 100.0



TICKER_MAP = {
    "SPY": ("Stock", "SMART", "USD")
    # ,
    # "QQQ": ("Stock", "SMART", "USD")
    # ,
    # "DIA": ("Stock", "SMART", "USD")
    # ,
    # "IWM": ("Stock", "SMART", "USD")
    # ,
}

CONTRACT_MAP = {
    "ES":  {"symbol": "ES",  "exchange": "CME", "currency": "USD", "multiplier": 50},
    "MES": {"symbol": "MES", "exchange": "CME", "currency": "USD", "multiplier": 5},
    "NQ":  {"symbol": "NQ",  "exchange": "CME", "currency": "USD", "multiplier": 20},
    "MNQ": {"symbol": "MNQ", "exchange": "CME", "currency": "USD", "multiplier": 2},
    "YM":  {"symbol": "YM",  "exchange": "ECBOT", "currency": "USD", "multiplier": 5},
    "MYM": {"symbol": "MYM", "exchange": "ECBOT", "currency": "USD", "multiplier": 0.5},
    "RTY": {"symbol": "RTY", "exchange": "CME", "currency": "USD", "multiplier": 50},
    "M2K": {"symbol": "M2K", "exchange": "CME", "currency": "USD", "multiplier": 5},
}

ETF_TO_FUTURES = {
    "SPY": ["ES", "MES"],
    "QQQ": ["NQ", "MNQ"],
    "DIA": ["YM", "MYM"],
    "IWM": ["RTY", "M2K"],
}



DEFAULT_START_DATE = "2003-01-01"



# === Dropbox Path Helpers ===
DROPBOX_ROOT = ""
def get_dropbox_path(ticker: str, category: str, filename: str | None = None) -> str:
    """
    Return the full Dropbox path for a given ticker/category, optionally appending a filename.
    Category must be one of the literal subfolder names found in your Dropbox.
    """
    t = ticker.lower()
    folder_map = {
        "options": f"/{t}/spy-options",
        "gaps": f"/{t}/spy-gaps-analysis",
        "timebands": f"/{t}/spy-timebands",
        "es-timebands": f"/{t}/es-timebands",
        "mes-timebands": f"/{t}/mes-timebands",
        "red-green": f"/{t}/spy-red-green",
        "es-futures-model": f"/{t}/es-futures-model",
        "mes-futures-model": f"/{t}/mes-futures-model",
    }
    if category not in folder_map:
        raise ValueError(f"Unknown category: {category}")
    base = folder_map[category]
    return f"{base}/{filename}" if filename else base


DROPBOX_APP_KEY="ilx6grkgm5zt4dd"
DROPBOX_APP_SECRET="d937pkqtxg9kgul"
DROPBOX_REFRESH_TOKEN="SRab8QA4gDUAAAAAAAAAATc48veCpbcE9cc8er7qbZhjpHqmsfjQCnp6vYX50Eda"
import config, os
print("Using config.py at:", config.__file__)
print("Key present:", bool(config.DROPBOX_APP_KEY))
