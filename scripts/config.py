import os

# config.py
TARGET_LOOP_SECONDS = 60.0



CONTRACT_MAP = {
    "SPY": ("Stock", "SMART", "USD"),
    "QQQ": ("Stock", "SMART", "USD"),
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
DROPBOX_ROOT = "/Apps/qma_live"

def get_dropbox_path(ticker: str, category: str) -> str:
    """
    Return Dropbox file path for given ticker and category (e.g. 'timebands', 'gaps', 'options').
    """
    t = ticker.lower()
    category_map = {
        "timebands": f"/{t}/{t}_timebands_history.xlsx",
        "gaps": f"/{t}/{t}-gaps-analysis/{t} gap analysis.xlsx",
        "options": f"/{t}/{t}-options-data/",
    }
    if category not in category_map:
        raise ValueError(f"Unknown category: {category}")
    return f"{DROPBOX_ROOT}{category_map[category]}"

from dropbox_utils import read_excel, upload_file, download_file
