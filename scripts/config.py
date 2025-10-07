import os

# config.py
TARGET_LOOP_SECONDS = 60.0



CONTRACT_MAP = {
    "SPY": ("Stock", "SMART", "USD"),
    "QQQ": ("Stock", "SMART", "USD")
    ,
    "DIA": ("Stock", "SMART", "USD"),
    "IWM": ("Stock", "SMART", "USD"),
}


DEFAULT_START_DATE = "2003-01-01"

# # === Dropbox root ===
# DROPBOX_ROOT = r"C:\Users\colby\Dropbox\2mindt"
#
#
# # === Folder resolver ===
# def get_folder(ticker: str, category: str) -> str:
#     """
#     Return Dropbox folder path for a given ticker + category.
#     Categories: 'gaps', 'options', 'timebands'
#     """
#     category_map = {
#         "gaps": f"{ticker.lower()}-gaps-analysis",
#         "options": f"{ticker.lower()}-options-data",
#         "timebands": f"{ticker.lower()}-timebands",
#     }
#
#     folder = category_map.get(category.lower())
#     if not folder:
#         raise ValueError(f"Unknown category: {category}")
#
#     return os.path.join(DROPBOX_ROOT, ticker.lower(), folder)



# === Dropbox Path Helpers ===
DROPBOX_ROOT = ""

def get_dropbox_path(ticker: str, category: str, filename: str | None = None) -> str:
    """
    Return the full Dropbox path for a given ticker/category, optionally appending a filename.
    """
    t = ticker.lower()
    folder_map = {
        "options": f"{DROPBOX_ROOT}/{t}/{t}-options-data",
        "gaps": f"{DROPBOX_ROOT}/{t}/{t}-gaps-analysis",
        "timebands": f"{DROPBOX_ROOT}/{t}/{t}-timebands",

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
