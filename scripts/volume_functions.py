# volume_functions.py — 30-min bands (RTH + ETH) per ticker in CONTRACT_MAP

import os
from datetime import datetime
from copy import copy

import pandas as pd

from zoneinfo import ZoneInfo
from openpyxl import load_workbook
from openpyxl.utils import get_column_letter

from config import CONTRACT_MAP
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

    secType, exchange, currency = CONTRACT_MAP[ticker]
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


# # ----- Add band labels -----
# def _add_bands(df: pd.DataFrame) -> pd.DataFrame:
#     ts = pd.to_datetime(df["timestamp"])
#     start = ts
#     end = start + pd.Timedelta(minutes=30)
#     out = df.copy()
#     out["date"] = start.dt.date
#     out["band"] = start.dt.strftime("%H:%M") + "–" + end.dt.strftime("%H:%M")
#     out["start"] = start.dt.strftime("%H:%M")
#     out["end"] = end.dt.strftime("%H:%M")
#     out["granularity"] = "30min"
#     out["generated_at"] = datetime.now()
#     out["session"] = out["band"].map(SESSION_MAP).fillna("OTHER")
#     return out[
#         ["timestamp","date","session","band","start","end",
#          "open","high","low","close","volume","barCount","average",
#          "granularity","generated_at"]
#     ]
#
#
# # ----- Densify -----
# def _densify(out: pd.DataFrame, include_rth: bool, include_eth: bool) -> pd.DataFrame:
#     if out.empty:
#         return out
#     dates = sorted(pd.unique(out["date"]))
#     allowed = set()
#     if include_rth: allowed |= RTH_SET
#     if include_eth: allowed |= ETH_SET
#     allowed_list = [b for b in ALL_SET if (not allowed) or (b in allowed)]
#
#     grid = pd.MultiIndex.from_product([dates, allowed_list], names=["date","band"]).to_frame(index=False)
#     full = grid.merge(out, on=["date","band"], how="left")
#
#     full["session"] = full["session"].fillna(full["band"].map(SESSION_MAP)).fillna("OTHER")
#
#     se = full["band"].str.split("–", n=1, expand=True)
#     full["start"] = full["start"].fillna(se[0])
#     full["end"]   = full["end"].fillna(se[1])
#
#     missing_ts = full["timestamp"].isna()
#     full.loc[missing_ts, "timestamp"] = pd.to_datetime(
#         full.loc[missing_ts, "date"].astype(str) + " " + full.loc[missing_ts, "start"], errors="coerce"
#     )
#
#     if "volume" in full:
#         full["volume"] = pd.to_numeric(full["volume"], errors="coerce").fillna(0).astype(int)
#
#     full["granularity"] = "30min"
#     full["generated_at"] = datetime.now()
#     full = full.sort_values(["date","start"]).reset_index(drop=True)
#     return full


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

        stype, exch, curr = CONTRACT_MAP[ticker]
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
        df["avg_20d"] = (
            df.groupby("band", group_keys=False)["volume"]
            .transform(lambda x: x.rolling(20, min_periods=1).mean())
        )
        df["ratio_to_avg_20d"] = df["volume"] / df["avg_20d"]

        # --- Temporary local save ---
        local_filename = f"{ticker.lower()}_timebands_history.xlsx"
        with pd.ExcelWriter(local_filename, engine="openpyxl", mode="w") as w:
            df.to_excel(w, sheet_name="Timebands", index=False)

        # --- Upload to Dropbox ---
        # --- Upload to Dropbox ---
        dropbox_path = get_dropbox_path(ticker, "timebands",
                                        f"{ticker.lower()}_timebands_history.xlsx")
        upload_file(local_filename, dropbox_path)
        print(f"[Dropbox] Uploaded {ticker} timebands → {dropbox_path}")


    finally:
        try:
            ib.disconnect()
        except Exception:
            pass


#
# import os
# import time
# import pandas as pd
# def run_timebands_30m(ticker, days=20, include_rth=True, include_eth=True,
#                       root: str = DROPBOX_ROOT):
#
#     print(f"[run_timebands_30m] Starting for {ticker}...")
#     from ib_insync import IB, Stock, Index
#
#     ib = IB()
#     try:
#         # connect (unique clientId)
#         cid = abs(hash(ticker)) % 9000
#         ib.connect("127.0.0.1", 7496, clientId=cid)
#         print(f"[run_timebands_30m] Connected to IB (clientId={cid})")
#
#         # contract from CONTRACT_MAP
#         stype, exch, curr = CONTRACT_MAP[ticker]
#         contract = Stock(ticker, exch, curr) if stype == "Stock" else Index(ticker, exch)
#
#         # fetch 30m bars, ETH+RTH
#         print("[run_timebands_30m] Requesting 30m bars...")
#         bars = ib.reqHistoricalData(
#             contract,
#             endDateTime="",
#             durationStr=f"{days} D",
#             barSizeSetting="30 mins",
#             whatToShow="TRADES",
#             useRTH=0,
#             formatDate=1,
#         )
#         print(f"[run_timebands_30m] Received {len(bars)} bars")
#         if not bars:
#             print("[run_timebands_30m] No bars returned.")
#             return None
#
#         # parse → Eastern clock, tz-naive for Excel
#         df = pd.DataFrame(bars)
#         raw_ts = pd.to_datetime(df["date"], errors="coerce", utc=True)
#         ts = raw_ts.dt.tz_convert(US_EASTERN).dt.tz_localize(None)
#         df["timestamp"] = ts
#         df["date"] = ts.dt.date
#         df["time"] = ts.dt.strftime("%H:%M")
#
#         # band + session metadata
#         df["start"] = ts.dt.strftime("%H:%M")
#         df["end"] = (ts + pd.Timedelta(minutes=30)).dt.strftime("%H:%M")
#         df["band"] = df["start"] + "–" + df["end"]
#         df["granularity"] = "30min"
#         df["generated_at"] = datetime.now(tz=US_EASTERN).strftime("%Y-%m-%d %H:%M:%S")
#         df["session"] = ts.dt.time.between(pd.to_datetime("09:30").time(),
#                                            pd.to_datetime("16:00").time())
#         df["session"] = df["session"].map({True: "RTH", False: "ETH"})
#
#         print("[run_timebands_30m] Sample rows after parsing:")
#         print(df.head())
#         print(f"[run_timebands_30m] Date range: {df['date'].min()} → {df['date'].max()}")
#
#         # rolling 20d average and ratio (indexes will have NaN ratio)
#         # compute avg_20d separately per timeband
#         df["avg_20d"] = (
#             df.groupby("band", group_keys=False)["volume"]
#             .transform(lambda x: x.rolling(20, min_periods=1).mean())
#         )
#
#         df["ratio_to_avg_20d"] = df["volume"] / df["avg_20d"]
#
#         # output path (Dropbox-first)
#         folder = get_folder(ticker, "timebands")
#         os.makedirs(folder, exist_ok=True)
#         out_path = os.path.join(folder,
#                                 f"{ticker.lower()}_timebands_history.xlsx")
#         print(f"[run_timebands_30m] Output path: {out_path}")
#
#         # read existing and purge legacy junk rows (band empty)
#         if os.path.exists(out_path):
#             print("[run_timebands_30m] File exists, appending...")
#             try:
#                 old = pd.read_excel(out_path, sheet_name="Timebands")
#                 old["date"] = pd.to_datetime(old["date"], errors="coerce").dt.date
#                 if "band" in old.columns:
#                     old = old.dropna(subset=["band"]).copy()
#                 else:
#                     old = pd.DataFrame()
#             except Exception as e:
#                 print(f"[run_timebands_30m] Read existing failed: {e}")
#                 old = pd.DataFrame()
#             combined = pd.concat([old, df], ignore_index=True)
#         else:
#             print("[run_timebands_30m] Creating new...")
#             combined = df
#
#         # keep only real bands, dedup by day+band
#         combined = combined.dropna(subset=["band"]).copy()
#         combined = combined.drop_duplicates(subset=["date", "band"], keep="last")
#
#         # prune to last N trading days
#         recent_dates = sorted(combined["date"].unique())[-days:]
#         combined = combined[combined["date"].isin(recent_dates)].copy()
#         print(f"[run_timebands_30m] Keeping last {len(recent_dates)} days: {recent_dates[0]} → {recent_dates[-1]}")
#         print("[run_timebands_30m] Last 5 rows to save:")
#         print(combined.tail(5))
#
#         # atomic save with retries + formatting
#         # atomic save with retries + formatting
#         base, ext = os.path.splitext(out_path)
#         tmp_path = f"{base}.tmp.xlsx"  # must end with .xlsx for openpyxl
#
#         for attempt in range(10):
#             try:
#                 with pd.ExcelWriter(tmp_path, engine="openpyxl", mode="w") as w:
#                     combined.to_excel(w, sheet_name="Timebands", index=False)
#
#                 # format temp, then atomically replace
#                 try:
#                     format_timebands_book(tmp_path)
#                 except Exception as fe:
#                     print(f"[run_timebands_30m] Formatting skipped: {fe}")
#
#                 os.replace(tmp_path, out_path)
#                 print(
#                     f"[run_timebands_30m] Saved {len(combined)} rows → {out_path}")
#                 break
#             except PermissionError:
#                 print(
#                     f"[run_timebands_30m] File locked, retrying ({attempt + 1}/10)...")
#                 time.sleep(2)
#             except Exception as e:
#                 # clean up temp on any other error
#                 try:
#                     if os.path.exists(tmp_path):
#                         os.remove(tmp_path)
#                 except Exception:
#                     pass
#                 raise
#         else:
#             print(
#                 f"[run_timebands_30m] Failed to save after retries: {out_path}")
#             try:
#                 if os.path.exists(tmp_path):
#                     os.remove(tmp_path)
#             except Exception:
#                 pass
#
#
#     finally:
#         try:
#             ib.disconnect()
#         except Exception:
#             pass
#
# for ticker in CONTRACT_MAP:
#     run_timebands_30m(
#         ticker,
#         days=20,
#         include_rth=True,
#         include_eth=True
#     )