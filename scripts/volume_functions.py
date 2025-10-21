# volume_functions.py — 30-min bands (RTH + ETH) per ticker in TICKER_MAP

import os
from datetime import datetime
from copy import copy

import pandas as pd

from zoneinfo import ZoneInfo
from openpyxl import load_workbook
from openpyxl.utils import get_column_letter

from config import TICKER_MAP
from config import DROPBOX_ROOT
US_EASTERN = ZoneInfo("America/New_York")

# ----- Band definitions -----
RTH_BANDS = [
    ("09:30","10:00"),("10:00","10:30"),("10:30","11:00"),("11:00","11:30"),
    ("11:30","12:00"),("12:00","12:30"),("12:30","13:00"),("13:00","13:30"),
    ("13:30","14:00"),("14:00","14:30"),("14:30","15:00"),("15:00","15:30"),
    ("15:30","16:00"),
]
ETH_BANDS = [
    # pre
    ("04:00","04:30"),("04:30","05:00"),("05:00","05:30"),("05:30","06:00"),
    ("06:00","06:30"),("06:30","07:00"),("07:00","07:30"),("07:30","08:00"),
    ("08:00","08:30"),("08:30","09:00"),("09:00","09:30"),
    # post
    ("16:00","16:30"),("16:30","17:00"),("17:00","17:30"),("17:30","18:00"),
    ("18:00","18:30"),("18:30","19:00"),("19:00","19:30"),("19:30","20:00"),
]
RTH_SET = {f"{s}–{e}" for s, e in RTH_BANDS}
ETH_SET = {f"{s}–{e}" for s, e in ETH_BANDS}
ALL_BANDS = RTH_BANDS + ETH_BANDS
ALL_SET = [f"{s}–{e}" for s, e in ALL_BANDS]
SESSION_MAP = ({b: "RTH" for b in RTH_SET} | {b: "ETH" for b in ETH_SET})



# ----- Fetch 30-min bars -----
def _fetch_30m(ticker: str, days: int) -> pd.DataFrame:
    from ib_insync import IB, Stock, Index, util
    ib = IB()
    ib.connect("127.0.0.1", 7496, clientId=abs(hash(ticker)) % 9000)

    secType, exchange, currency = TICKER_MAP[ticker]
    if secType == "Stock":
        contract = Stock(ticker, exchange, currency)
    else:
        contract = Index(ticker, exchange)

    bars = ib.reqHistoricalData(
        contract,
        endDateTime="",
        durationStr=f"{days} D",
        barSizeSetting="30 mins",
        whatToShow="TRADES",
        useRTH=False,   # ETH + RTH
        formatDate=2,
    )
    ib.disconnect()

    df = util.df(bars)
    if df.empty:
        return pd.DataFrame(columns=["timestamp","open","high","low","close","volume","barCount","average"])

    df = df.rename(columns={"date": "timestamp"})
    ts = (pd.to_datetime(df["timestamp"], utc=True)
            .dt.tz_convert(US_EASTERN)
            .dt.tz_localize(None))
    df["timestamp"] = ts
    print(f"Fetching Volume for {ticker}")
    cutoff = pd.Timestamp.now(tz=US_EASTERN).floor("30min").tz_localize(None)
    df = df[df["timestamp"] <= cutoff]
    return df


# ----- Excel formatting -----
def format_sheet(wb, sheetname: str):
    ws = wb[sheetname]
    for cell in ws[1]:
        f = copy(cell.font); f.bold = True; cell.font = f
    ws.freeze_panes = "A2"
    for col_idx, col_cells in enumerate(ws.columns, 1):
        max_len = max(len(str(c.value)) if c.value is not None else 0 for c in col_cells)
        ws.column_dimensions[get_column_letter(col_idx)].width = max_len + 2


def format_timebands_book(path: str):
    wb = load_workbook(path)
    if "Timebands" in wb.sheetnames:
        ws = wb["Timebands"]
        header = {cell.value: cell.column for cell in ws[1] if cell.value}
        fmt_map = {
            "timestamp": "yyyy-mm-dd hh:mm",
            "generated_at": "yyyy-mm-dd hh:mm",
            "volume": "#,##0",
            "avg_20d": "#,##0",
            "ratio_to_avg_20d": "0.00%",
        }
        for name, fmt in fmt_map.items():
            if name in header:
                col_idx = header[name]
                col_letter = get_column_letter(col_idx)
                for r in range(2, ws.max_row + 1):
                    ws[f"{col_letter}{r}"].number_format = fmt
        format_sheet(wb, "Timebands")
    wb.save(path)


# ----- Main runner -----
from dropbox_utils import upload_file
from config import get_dropbox_path

def run_timebands_30m(ticker, days=20, include_rth=True, include_eth=True):
    print(f"[run_timebands_30m] Starting for {ticker}...")
    from ib_insync import IB, Stock, Index

    ib = IB()
    try:
        cid = abs(hash(ticker)) % 9000
        ib.connect("127.0.0.1", 7496, clientId=cid)
        print(f"[run_timebands_30m] Connected to IB (clientId={cid})")

        stype, exch, curr = TICKER_MAP[ticker]
        contract = Stock(ticker, exch, curr) if stype == "Stock" else Index(ticker, exch)

        print("[run_timebands_30m] Requesting 30m bars...")
        bars = ib.reqHistoricalData(
            contract,
            endDateTime="",
            durationStr=f"{days} D",
            barSizeSetting="30 mins",
            whatToShow="TRADES",
            useRTH=0,
            formatDate=1,
        )
        print(f"[run_timebands_30m] Received {len(bars)} bars")
        if not bars:
            print("[run_timebands_30m] No bars returned.")
            return None

        df = pd.DataFrame(bars)
        raw_ts = pd.to_datetime(df["date"], errors="coerce", utc=True)
        ts = raw_ts.dt.tz_convert(US_EASTERN).dt.tz_localize(None)
        df["timestamp"] = ts
        df["date"] = ts.dt.date
        df["time"] = ts.dt.strftime("%H:%M")

        df["start"] = ts.dt.strftime("%H:%M")
        df["end"] = (ts + pd.Timedelta(minutes=30)).dt.strftime("%H:%M")
        df["band"] = df["start"] + "–" + df["end"]
        df["granularity"] = "30min"
        df["generated_at"] = datetime.now(tz=US_EASTERN).strftime("%Y-%m-%d %H:%M:%S")
        df["session"] = ts.dt.time.between(
            pd.to_datetime("09:30").time(), pd.to_datetime("16:00").time()
        )
        df["session"] = df["session"].map({True: "RTH", False: "ETH"})

        print("[run_timebands_30m] Sample rows after parsing:")
        print(df.head())

        # --- Rolling 20-day averages per band ---
        # --- Rolling 20-day averages and Z-score metrics per band ---
        df["avg_20d"] = (
            df.groupby("band", group_keys=False)["volume"]
            .transform(lambda x: x.rolling(20, min_periods=1).mean())
        )
        df["stdev_20d"] = (
            df.groupby("band", group_keys=False)["volume"]
            .transform(lambda x: x.rolling(20, min_periods=1).std())
        )
        df["zscore_20d"] = (df["volume"] - df["avg_20d"]) / df["stdev_20d"]

        def _sigma_flag(z):
            if pd.isna(z):
                return ""
            if z >= 3:
                return "≥3σ above"
            elif z >= 2:
                return "≥2σ above"
            elif z >= 1:
                return "≥1σ above"
            elif z <= -3:
                return "≥3σ below"
            elif z <= -2:
                return "≥2σ below"
            elif z <= -1:
                return "≥1σ below"
            return ""

        df["sigma_flag"] = df["zscore_20d"].apply(_sigma_flag)

        df["ratio_to_avg_20d"] = df["volume"] / df["avg_20d"]

        # --- Temporary local save ---
        # --- Dynamic per-ticker filenames (overwrite-safe) ---
        filename = f"{ticker.lower()}_timeband_volume.xlsx"
        local_filename = os.path.join(r"C:\2mdt\2mindt-site\scripts",
                                      filename)

        with pd.ExcelWriter(local_filename, engine="openpyxl", mode="w") as w:
            df.to_excel(w, sheet_name="Timebands", index=False)

        dropbox_path = get_dropbox_path(ticker, "timebands",
                                        filename)  # e.g., /spy/spy-timebands/spy_timeband_volume.xlsx
        upload_file(local_filename,
                    dropbox_path)  # must use WriteMode("overwrite") inside upload_file
        os.remove(local_filename)

        print(f"[Local] Deleted temporary file: {local_filename}")

    finally:
        try:
            ib.disconnect()
        except Exception:
            pass

