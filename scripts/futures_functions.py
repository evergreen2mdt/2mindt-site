# === futures_functions.py ===
from ib_insync import IB, Future
from datetime import datetime
import pandas as pd
import os
from openpyxl import load_workbook
from openpyxl.utils import get_column_letter

# --- CONFIG IMPORTS ---
from config import TICKER_MAP, CONTRACT_MAP, ETF_TO_FUTURES


# ---------- Helpers ----------

def _contracts_with_expiry(symbol: str):
    """Return sorted [(expiry, contract)] list of all CME futures (expired included)."""
    ib = IB()
    cid = abs(hash(("CONTRACTS", symbol))) % 9000
    print(f"[connect] IBKR (clientId={cid}) for {symbol}")
    ib.connect("127.0.0.1", 7496, clientId=cid)

    cds = ib.reqContractDetails(Future(symbol, exchange="CME", includeExpired=True))
    ib.disconnect()
    if not cds:
        raise RuntimeError(f"No contracts found for {symbol}")

    out = []
    for cd in cds:
        exp = pd.to_datetime(cd.contract.lastTradeDateOrContractMonth, errors="coerce")
        if pd.notna(exp):
            out.append((exp, cd.contract))
    out.sort(key=lambda x: x[0])
    print(f"[contracts] {len(out)} total for {symbol}")
    return out


def _detect_front_previous(contracts):
    """Identify front and previous based on today's date."""
    today = datetime.now()
    front, prev = None, None
    for i, (exp, c) in enumerate(contracts):
        if exp > today:
            front = c
            prev = contracts[i - 1][1] if i > 0 else None
            break
    if front is None and contracts:
        front = contracts[-1][1]
        prev = contracts[-2][1] if len(contracts) >= 2 else None
    print(f"[detect] Today={today.date()} → Front={front.localSymbol if front else 'None'} | "
          f"Previous={prev.localSymbol if prev else 'None'}")
    return front, prev


def _fetch_30m_bars(contract, days=30):
    """Fetch 30-minute bars for given contract."""
    ib = IB()
    cid = abs(hash(("BARS", contract.localSymbol))) % 9000
    ib.connect("127.0.0.1", 7496, clientId=cid)
    bars = ib.reqHistoricalData(
        contract,
        endDateTime="",
        durationStr=f"{days} D",
        barSizeSetting="30 mins",
        whatToShow="TRADES",
        useRTH=False,
        formatDate=1,
    )
    ib.disconnect()

    df = pd.DataFrame(bars)
    if df.empty:
        return df
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    try:
        df["date"] = df["date"].dt.tz_localize(None)
    except Exception:
        pass
    return df


def _strip_tz(df):
    """Remove tzinfo from all datetime columns."""
    out = df.copy()
    for col in out.columns:
        if pd.api.types.is_datetime64_any_dtype(out[col]):
            try:
                out[col] = pd.to_datetime(out[col], errors="coerce").dt.tz_localize(None)
            except Exception:
                pass
    return out


def _autosize_excel(path: str):
    """Auto-fit columns for all sheets."""
    try:
        wb = load_workbook(path)
        for ws in wb.worksheets:
            for col_cells in ws.columns:
                vals = [str(c.value) if c.value else "" for c in col_cells]
                max_len = max((len(v) for v in vals), default=0)
                ws.column_dimensions[get_column_letter(col_cells[0].column)].width = min(max_len + 2, 60)
        wb.save(path)
    except Exception as e:
        print(f"[autosize] skipped: {e}")


# ---------- Single Futures Symbol Runner ----------

def run_futures_volume(symbol="ES"):
    """
    Pull all futures contracts from previous→front→future until first 0-volume contract.
    Write to Excel with role column and summary sheet.
    """
    # 1. get full contract list
    contracts = _contracts_with_expiry(symbol)
    front, prev = _detect_front_previous(contracts)
    if not front:
        print("[error] could not detect front contract")
        return None

    # 2. restrict to previous→end
    valid_contracts = []
    started = False
    for exp, c in contracts:
        if prev and c.localSymbol == prev.localSymbol:
            started = True
        if started:
            valid_contracts.append((exp, c))
    if not started:
        valid_contracts = [(exp, c) for exp, c in contracts if exp >= datetime.now()]

    # 3. fetch sequentially until 0-volume
    tables = []
    for exp, c in valid_contracts:
        df = _fetch_30m_bars(c, 30)
        if df.empty or df["volume"].sum() == 0:
            print(f"[volume] {c.localSymbol}: 0 volume → stop")
            break
        role = (
            "previous" if prev and c.localSymbol == prev.localSymbol
            else "front" if front and c.localSymbol == front.localSymbol
            else "future"
        )
        df["contract"] = c.localSymbol
        df["expiry"] = pd.Timestamp(exp).normalize()
        df["role"] = role
        tables.append(df)
        print(f"[fetched] {c.localSymbol} ({role}): {len(df)} rows, vol={df['volume'].sum():,.0f}")

    if not tables:
        print("[result] No valid contracts with volume.")
        return None

    all_df = pd.concat(tables, ignore_index=True)
    all_df = _strip_tz(all_df)

    # 4. summary
    summary = (
        all_df.groupby(["contract", "expiry", "role"], dropna=False)
        .agg(
            rows=("date", "count"),
            start=("date", "min"),
            end=("date", "max"),
            total_volume=("volume", "sum"),
        )
        .reset_index()
        .sort_values("expiry")
    )
    for c in ["start", "end", "expiry"]:
        summary[c] = pd.to_datetime(summary[c], errors="coerce").dt.tz_localize(None)

    # 5. write excel
    ts = datetime.now().strftime("%Y-%m-%d_%H%M")
    fname = f"futures_{symbol.lower()}_{ts}.xlsx"
    print(f"[save] Writing Excel → {fname}")

    with pd.ExcelWriter(fname, engine="openpyxl", mode="w") as w:
        summary.to_excel(w, sheet_name="Summary", index=False)
        order = (
            list(summary[summary["role"] == "previous"]["contract"].unique()) +
            list(summary[summary["role"] == "front"]["contract"].unique()) +
            list(summary[summary["role"] == "future"]["contract"].unique())
        )
        seen = set()
        for contract in [c for c in order if not (c in seen or seen.add(c))]:
            sub = all_df[all_df["contract"] == contract].sort_values("date")
            sub = _strip_tz(sub)
            sub.to_excel(w, sheet_name=contract[:31], index=False)

    _autosize_excel(fname)
    abs_path = os.path.abspath(fname)
    print(f"[done] Saved: {abs_path}")
    return abs_path


# ---------- Multi-ETF Runner ----------

def run_all_etf_futures_volume():
    """
    Loop through all tickers in TICKER_MAP.
    For each ticker, find mapped futures in ETF_TO_FUTURES,
    and produce one Excel file per futures root.
    """
    for ticker in TICKER_MAP.keys():
        fut_syms = ETF_TO_FUTURES.get(ticker, [])
        if not fut_syms:
            print(f"[skip] {ticker}: no futures mapping found.")
            continue

        print(f"\n[map] {ticker} → {', '.join(fut_syms)}")
        for fut in fut_syms:
            try:
                print(f"[run] Fetching futures volume for {fut} (from {ticker})")
                path = run_futures_volume(fut)
                if path:
                    print(f"[ok] {fut}: saved → {path}")
                else:
                    print(f"[warn] {fut}: no valid data.")
            except Exception as e:
                print(f"[error] {fut}: {e}")


# ---------- CLI ----------

if __name__ == "__main__":
    # To run all ETFs → futures
    run_all_etf_futures_volume()
