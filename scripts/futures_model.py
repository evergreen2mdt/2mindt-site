# futures_model.py
# -*- coding: utf-8 -*-

from io import BytesIO
from datetime import datetime
from zoneinfo import ZoneInfo
import numpy as np
import pandas as pd

from config import ETF_TO_FUTURES
from dropbox_utils import get_dropbox_client, read_excel as dbx_read_excel

try:
    import yfinance as yf
except Exception:
    yf = None

US_EASTERN = ZoneInfo("America/New_York")


# =========================
# Core metric computations
# =========================
def compute_basis(fut_price: float, spot_price: float) -> float:
    if np.isnan(fut_price) or np.isnan(spot_price) or spot_price == 0:
        return np.nan
    return float((fut_price - spot_price) / spot_price)


def zscore(series: pd.Series, window: int = 20) -> float:
    s = pd.to_numeric(series, errors="coerce").dropna()
    if len(s) < max(3, window):
        return np.nan
    w = s.tail(window)
    mu, sd = float(w.mean()), float(w.std(ddof=0))
    if sd == 0:
        return np.nan
    return float((s.iloc[-1] - mu) / sd)


def compute_realized_range(df_day: pd.DataFrame) -> float:
    if df_day.empty or not {"high", "low"}.issubset(df_day.columns):
        return np.nan
    hi = pd.to_numeric(df_day["high"], errors="coerce").max()
    lo = pd.to_numeric(df_day["low"], errors="coerce").min()
    if np.isnan(hi) or np.isnan(lo):
        return np.nan
    return float(hi - lo)


def estimate_implied_range_from_vix(spot: float, vix_level: float) -> float:
    if np.isnan(spot) or vix_level is None or np.isnan(vix_level):
        return np.nan
    return float(spot * (vix_level / 100.0) / np.sqrt(252))


def compute_realized_implied_ratio(realized_range: float, implied_range: float) -> float:
    if np.isnan(realized_range) or np.isnan(implied_range) or implied_range <= 0:
        return np.nan
    return float(realized_range / implied_range)


def compute_trendiness_score(open_: float, close: float, high: float, low: float) -> float:
    if any(np.isnan(x) for x in [open_, close, high, low]):
        return np.nan
    tr = max(high, open_, close) - min(low, open_, close)
    if tr <= 0:
        return np.nan
    return float(abs(close - open_) / tr)


def compute_term_structure_flag(spread_value: float) -> str:
    if np.isnan(spread_value):
        return "unknown"
    return "backwardation" if spread_value < 0 else "contango"


# =========================
# Data helpers
# =========================
def _load_etf_spot_from_options(ticker: str) -> float:
    folder = f"/{ticker.lower()}/{ticker.lower()}-options-data/"
    dbx = get_dropbox_client()
    try:
        res = dbx.files_list_folder(folder)
        entries = [e for e in res.entries if e.name.endswith(".xlsx")]
        entries.sort(key=lambda e: e.client_modified, reverse=True)
        if not entries:
            return np.nan
        path = f"{folder}{entries[0].name}"
        df_raw = dbx_read_excel(path, sheet_name="raw options")
        s = pd.to_numeric(df_raw.get("spot"), errors="coerce").dropna()
        return float(s.mode().iloc[0]) if not s.empty else np.nan
    except Exception:
        return np.nan


def _try_fetch_vix() -> float:
    if yf is None:
        return np.nan
    try:
        v = yf.download("^VIX", period="5d", interval="30m", progress=False)
        if v is None or v.empty:
            return np.nan
        return float(pd.to_numeric(v["Close"], errors="coerce").dropna().iloc[-1])
    except Exception:
        return np.nan


def _load_latest_ohlc_from_contract(ticker: str, root: str) -> pd.DataFrame:
    """Find the most recent available contract sheet with OHLC data."""
    path = f"/{ticker.lower()}/{root.lower()}-timebands-volume/{root.lower()}_timeband_volume.xlsx"
    try:
        xls = pd.ExcelFile(dbx_read_excel(path, sheet_name=None))
        candidates = []

        for name, df in xls.items():
            if name.lower() == "timebands":
                continue
            if not {"open", "high", "low", "close"}.issubset(df.columns):
                continue

            df["date"] = pd.to_datetime(df.get("date"), errors="coerce").dt.date
            df = df[df["date"].notna()]
            if df.empty:
                continue

            latest_date = df["date"].max()
            df = df[df["date"] == latest_date]
            last_close = pd.to_numeric(df["close"], errors="coerce").dropna().iloc[-1]
            candidates.append((latest_date, name, df, last_close))

        if not candidates:
            return pd.DataFrame()

        # pick the contract with the most recent date, then highest close
        candidates.sort(key=lambda x: (x[0], x[3]), reverse=True)
        selected = candidates[0][2]
        return selected.copy()
    except Exception:
        return pd.DataFrame()


def _latest_ohlc_from_rth(df: pd.DataFrame) -> tuple:
    if df.empty or not {"open", "high", "low", "close"}.issubset(df.columns):
        return (np.nan, np.nan, np.nan, np.nan)
    o = pd.to_numeric(df["open"], errors="coerce").dropna()
    h = pd.to_numeric(df["high"], errors="coerce").dropna()
    l = pd.to_numeric(df["low"], errors="coerce").dropna()
    c = pd.to_numeric(df["close"], errors="coerce").dropna()
    if any(s.empty for s in [o, h, l, c]):
        return (np.nan, np.nan, np.nan, np.nan)
    return (float(o.iloc[0]), float(h.max()), float(l.min()), float(c.iloc[-1]))


# =========================
# Regime classifier
# =========================
def classify_regime(realized_implied_ratio: float,
                    basis_z: float,
                    term_flag: str,
                    trendiness: float) -> str:
    comp = (not np.isnan(realized_implied_ratio)) and (realized_implied_ratio < 0.8)
    expa = (not np.isnan(realized_implied_ratio)) and (realized_implied_ratio > 1.2)
    basis_widen = (not np.isnan(basis_z)) and (abs(basis_z) > 1.0)
    trendy = (not np.isnan(trendiness)) and (trendiness > 0.6)

    if expa and (term_flag == "backwardation" or basis_widen or trendy):
        return "expansion-active"
    if comp and (term_flag == "contango") and not basis_widen and not trendy:
        return "compression-stable"
    if comp and (term_flag == "backwardation" or basis_widen):
        return "compression-fragile"
    if expa and not (basis_widen or trendy):
        return "expansion-forming"
    return "neutral"


# =========================
# Orchestrator
# =========================
def run_futures_model(ticker: str) -> pd.DataFrame:
    ticker = ticker.upper()
    roots = ETF_TO_FUTURES.get(ticker, [])
    if not roots:
        return pd.DataFrame()

    dbx = get_dropbox_client()
    etf_spot = _load_etf_spot_from_options(ticker)
    vix_now = _try_fetch_vix()
    out_rows = []

    for root in roots:
        df_ohlc = _load_latest_ohlc_from_contract(ticker, root)
        if df_ohlc.empty:
            print(f"  [{root}] No valid OHLC found.")
            continue

        latest_date = df_ohlc["date"].max()
        o, h, l, c = _latest_ohlc_from_rth(df_ohlc)
        fut_close = c
        realized_rng = compute_realized_range(df_ohlc)
        implied_rng = estimate_implied_range_from_vix(etf_spot, vix_now)
        rir = compute_realized_implied_ratio(realized_rng, implied_rng)
        basis_lvl = compute_basis(fut_close, etf_spot)
        trendiness = compute_trendiness_score(o, c, h, l)
        term_flag = compute_term_structure_flag(np.nan)

        basis_z = np.nan
        regime = classify_regime(rir, basis_z, term_flag, trendiness)

        out_rows.append({
            "date": latest_date,
            "ticker": ticker,
            "root": root,
            "fut_close": fut_close,
            "etf_spot": etf_spot,
            "vix": vix_now,
            "realized_range": realized_rng,
            "implied_range": implied_rng,
            "realized_implied_ratio": rir,
            "basis_level": basis_lvl,
            "trendiness": trendiness,
            "term_structure_flag": term_flag,
            "basis_zscore": basis_z,
            "regime": regime
        })

        print(f"  [{root}] ({latest_date}) Close={fut_close} | Basis={basis_lvl:.4f} | Trend={trendiness:.2f} | Regime={regime}")

    return pd.DataFrame(out_rows)


# =========================
# Run block
# =========================
if __name__ == "__main__":
    print("=== FUTURES MICROSTRUCTURE MODEL RUN ===")
    tickers = list(ETF_TO_FUTURES.keys()) or ["SPY"]
    for ticker in tickers:
        print(f"\n[{datetime.now(tz=US_EASTERN)}] Running model for {ticker}...")
        result = run_futures_model(ticker)
        if result.empty:
            print(f"  [{ticker}] No data produced.")
            continue
        print(f"\n[{ticker}] Results:")
        print(result[["root", "date", "regime", "realized_implied_ratio",
                      "basis_level", "trendiness"]])
    print("\n=== DONE ===")
f