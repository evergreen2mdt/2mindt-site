# futures_model.py — dynamic ETF → futures mapping (no hardcoded SPY/MES)


from datetime import datetime
from zoneinfo import ZoneInfo
import numpy as np
import pandas as pd
import yfinance as yf
import warnings
from config import TICKER_MAP, ETF_TO_FUTURES
from dropbox_utils import upload_file
import os
warnings.filterwarnings("ignore", category=FutureWarning)
US_EASTERN = ZoneInfo("America/New_York")


# ------------------ Core metrics ------------------
def compute_basis_zscore(series, window=20):
    s = pd.Series(series).dropna()
    if len(s) < 5:
        return np.nan
    mu, sd = s.mean(), s.std(ddof=0)
    return (s.iloc[-1] - mu) / sd if sd > 0 else np.nan


def compute_realized_range(df):
    if df.empty or not {"high", "low"}.issubset(df.columns):
        return np.nan
    return float(df["high"].max() - df["low"].min())


def estimate_implied_range_from_vix(spot, vix):
    if np.isnan(spot) or np.isnan(vix):
        return np.nan
    return float(spot * (vix / 100.0) / np.sqrt(252))


def compute_realized_implied_ratio(realized, implied):
    if np.isnan(realized) or np.isnan(implied) or implied <= 0:
        return np.nan
    return float(realized / implied)


def compute_trendiness(o, c, h, l):
    if any(np.isnan(x) for x in [o, c, h, l]):
        return np.nan
    tr = max(h, o, c) - min(l, o, c)
    return abs(c - o) / tr if tr > 0 else np.nan


def compute_term_structure_flag(spread):
    if np.isnan(spread):
        return "unknown"
    return "backwardation" if spread < 0 else "contango"


# ------------------ Data helpers ------------------
def get_ohlcv_yf(symbol, period="5d", interval="30m"):
    """Fetch recent OHLCV data for a given symbol via yfinance."""
    df = yf.download(symbol, period=period, interval=interval, progress=False)
    if df is None or df.empty:
        return pd.DataFrame()

    # Flatten MultiIndex columns if present
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
    df = df.rename(columns=str.lower)

    # ---- build date column explicitly from the index ----
    idx = pd.to_datetime(df.index, errors="coerce", utc=True)
    idx = idx.tz_convert("America/New_York").tz_localize(None)
    df.insert(0, "date", idx.date)  # ensure first column = plain date

    # ---- clean and keep ----
    keep = [c for c in ["date", "open", "high", "low", "close", "volume"] if c in df.columns]
    df = df[keep].dropna(subset=["date"]).reset_index(drop=True)
    return df





def get_vix():
    v = yf.download("^VIX", period="5d", interval="30m", progress=False)
    if v.empty:
        return np.nan
    return float(v["Close"].dropna().iloc[-1])


# ------------------ Main runner ------------------
def run_futures_model():
    all_results = []
    vix = get_vix()

    for etf, roots in ETF_TO_FUTURES.items():
        if etf not in TICKER_MAP:
            continue

        # --- Load ETF data ---
        etf_df = get_ohlcv_yf(etf)
        if etf_df.empty:
            print(f"[{etf}] No yfinance data.")
            continue

        spot = etf_df["close"].iloc[-1]
        latest_date = etf_df["date"].iloc[-1]

        # --- Loop through each mapped futures root (ES, MES, etc.) ---
        for fut in roots:
            fut_front = get_ohlcv_yf(f"{fut}=F")
            fut_next = get_ohlcv_yf(f"{fut}Z25.CME")  # adjust contract as needed

            if fut_front.empty:
                print(f"[{etf}] No data for {fut}.")
                continue

            # --- Basis & z-score ---
            joined = pd.merge(
                etf_df[["date", "close"]],
                fut_front[["date", "close"]],
                on="date",
                how="inner",
                suffixes=("_etf", "_fut"),
            )
            joined["basis"] = joined["close_fut"] - joined["close_etf"]
            basis_level = joined["basis"].iloc[-1]
            basis_z = compute_basis_zscore(joined["basis"].tail(20))

            # --- Calendar spread (front - next) ---
            spread, spread_roc = np.nan, np.nan
            if not fut_next.empty:
                spread = fut_front["close"].iloc[-1] - fut_next["close"].iloc[-1]
                if len(fut_front) > 1 and len(fut_next) > 1:
                    prev = fut_front["close"].iloc[-2] - fut_next["close"].iloc[-2]
                    spread_roc = spread - prev
            term_flag = compute_term_structure_flag(spread)

            # --- Volume / OI proxies ---
            vol_tail = fut_front["volume"].tail(5)
            volume_rank = vol_tail.iloc[-1] / vol_tail.mean() if vol_tail.mean() > 0 else np.nan
            oi_delta = vol_tail.diff().iloc[-1] if len(vol_tail) > 1 else np.nan

            # --- Volatility metrics ---
            realized = compute_realized_range(etf_df)
            implied = estimate_implied_range_from_vix(spot, vix)
            rir = compute_realized_implied_ratio(realized, implied)

            o, h, l, c = etf_df.iloc[-1][["open", "high", "low", "close"]]
            trend = compute_trendiness(o, c, h, l)

            regime = (
                "expansion-active"
                if rir > 1.2 and (term_flag == "backwardation" or trend > 0.6)
                else "compression-stable" if rir < 0.8 and term_flag == "contango"
                else "neutral"
            )

            all_results.append({
                "ticker": etf,
                "future": fut,
                "date": latest_date,
                "spot": spot,
                "vix": vix,
                "basis_level": basis_level,
                "basis_zscore": basis_z,
                "calendar_spread": spread,
                "spread_roc": spread_roc,
                "term_structure_flag": term_flag,
                "volume_rank": volume_rank,
                "oi_delta": oi_delta,
                "rir": rir,
                "trendiness": trend,
                "regime": regime,
            })

            print(
                f"[{etf}] ({latest_date}) {fut} Close={c:.2f} | Basis={basis_level:.2f} | "
                f"Z={basis_z:.2f} | Spread={spread:.2f} | Term={term_flag} | "
                f"Trend={trend:.2f} | Regime={regime}"
            )

            # --- Save each futures result separately to Dropbox ---
            df_out = pd.DataFrame([all_results[-1]])
            local_path = f"{fut.lower()}_futures_model.xlsx"
            dropbox_path = f"/{etf.lower()}/{fut.lower()}-futures-model/{fut.lower()}_futures_model.xlsx"
            df_out.to_excel(local_path, index=False)
            upload_file(local_path, dropbox_path)
            os.remove(local_path)
            print(f"[Dropbox] Uploaded → {dropbox_path}")

    return pd.DataFrame(all_results)



# ------------------ Run ------------------
if __name__ == "__main__":
    print("=== FUTURES MICROSTRUCTURE MODEL RUN ===")
    df = run_futures_model()
    if not df.empty:
        print("\nSummary:")
        print(df[["ticker", "future", "date", "regime", "basis_zscore", "trendiness", "rir"]])
    print("\n=== DONE ===")
