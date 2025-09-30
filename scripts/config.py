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

# === Dropbox root ===
DROPBOX_ROOT = r"C:\Users\colby\Dropbox\2mindt"


# === Folder resolver ===
def get_folder(ticker: str, category: str) -> str:
    """
    Return Dropbox folder path for a given ticker + category.
    Categories: 'gaps', 'options', 'timebands'
    """
    category_map = {
        "gaps": f"{ticker.lower()}-gaps-analysis",
        "options": f"{ticker.lower()}-options-data",
        "timebands": f"{ticker.lower()}-timebands",
    }

    folder = category_map.get(category.lower())
    if not folder:
        raise ValueError(f"Unknown category: {category}")

    return os.path.join(DROPBOX_ROOT, ticker.lower(), folder)