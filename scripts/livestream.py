# --- stdlib
import os
import time
import base64
from io import BytesIO
from datetime import datetime
from zoneinfo import ZoneInfo

# --- third-party
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import plotly.express as px
import streamlit as st

# --- project
from config import (
    ETF_TO_FUTURES,
    TARGET_LOOP_SECONDS,
    TICKER_MAP,
    get_dropbox_path,
)
from dropbox_utils import (
    get_dropbox_client,
    read_excel as dbx_read_excel,
)


st.cache_data = lambda *a, **k: (lambda f: f)


def load_futures_timebands(ticker: str):
    """Load and merge all mapped futures roots (e.g., SPY → ES,MES)."""
    roots = ETF_TO_FUTURES.get(ticker, [])
    if not roots:
        return None

    meta = ["date", "timestamp", "generated_at", "granularity", "session", "band"]
    frames = []

    for sym in roots:
        path = f"/{ticker.lower()}/{sym.lower()}-timebands-volume/{sym.lower()}_timeband_volume.xlsx"
        try:
            df = dbx_read_excel(path, sheet_name="Timebands").copy()

            # --- normalize keys ---
            df["date"] = pd.to_datetime(df.get("date"), errors="coerce")
            df = df[df["date"].notna()].copy()
            df["timestamp"] = pd.to_datetime(df.get("timestamp"), errors="coerce")

            # keep meta + all *_volume + Total_volume if present
            vol_cols = [c for c in df.columns if c.endswith("_volume")]
            keep = [c for c in meta if c in df.columns] + vol_cols
            if "Total_volume" in df.columns:
                keep.append("Total_volume")

            frames.append(df[keep])
        except Exception as e:
            print(f"[futures] skip {sym}: {e}")

    if not frames:
        return None

    # outer-merge on available meta keys
    out = frames[0]
    for nxt in frames[1:]:
        keys = [k for k in meta if k in out.columns and k in nxt.columns]
        out = pd.merge(out, nxt, on=keys, how="outer")

    # stable sort then fill
    sort_keys = [k for k in ["date", "timestamp"] if k in out.columns]
    if sort_keys:
        out = out.sort_values(sort_keys)
    out = out.fillna(0)

    # recompute combined total across all contract volumes
    all_vol = [c for c in out.columns if c.endswith("_volume") and c.lower() != "total_volume"]
    if all_vol:
        out["Total_volume_all"] = out[all_vol].sum(axis=1)
    return out



def compute_relative_flow(current_volume: float,
                          elapsed_minutes: float,
                          avg_20d_volume: float,
                          bar_minutes: int = 30) -> float:
    """
    Compute real-time relative flow.
    current_volume : volume so far in this bar
    elapsed_minutes : minutes completed in the bar
    avg_20d_volume : 20-day average total volume for this band
    bar_minutes : bar length in minutes (default 30)
    """
    if elapsed_minutes <= 0 or avg_20d_volume <= 0:
        return float("nan")
    # Current flow rate vs. historical flow rate
    current_rate = current_volume / elapsed_minutes
    avg_rate = avg_20d_volume / bar_minutes
    return current_rate / avg_rate

def read_excel_from_dropbox(dbx, dropbox_path: str, sheet_name: str):
    """Download Excel sheet from Dropbox path to a BytesIO buffer."""
    try:
        _, res = dbx.files_download(dropbox_path)
        with BytesIO(res.content) as bio:
            df = pd.read_excel(bio, sheet_name=sheet_name)
        return df
    except Exception as e:
        st.error(f"Failed to read {sheet_name} from Dropbox: {e}")
        return pd.DataFrame()


def weighted_pin(df_pins, spot, window_pct=0.05, window_abs=None, lam=3.0, min_strength=0):
    """
    Compute weighted pin strike given strikes and pinning strengths.
    - df_pins: DataFrame with ['strike','pin_strength']
    - spot: current spot price
    - window_pct: % window around spot (default ±5%)
    - window_abs: absolute window size (overrides pct if not None)
    - lam: decay scale in dollars (None = no decay)
    - min_strength: ignore weak pins below this level
    """
    pins = df_pins[df_pins['pin_strength'] > min_strength].copy()
    if pins.empty:
        return None

    # window selection
    if window_abs is not None:
        lo, hi = spot - window_abs, spot + window_abs
    else:
        band = spot * window_pct
        lo, hi = spot - band, spot + band
    pins = pins[(pins['strike'] >= lo) & (pins['strike'] <= hi)]
    if pins.empty:
        return None

    # distance-decay weights
    if lam is not None and lam > 0:
        decay = np.exp(-np.abs(pins['strike'] - spot) / lam)
        eff_w = pins['pin_strength'] * decay
    else:
        eff_w = pins['pin_strength']

    wsum = eff_w.sum()
    if wsum == 0:
        return None
    return float((pins['strike'] * eff_w).sum() / wsum)

def get_paths(ticker: str):
    t = ticker.lower()
    return {
        "options_dir": f"/{t}/{t}-options-data/",
        "gaps_file": f"/{t}/{t}-gaps-analysis/{t} gap analysis.xlsx",
        "timebands_file": f"/{t}/{t}-timebands/{t}_timeband_volume.xlsx",
    }

def render_futures_volume_chart(df: pd.DataFrame, ticker: str):
    """
    Show est_vol_at_close from ES and MES futures with ETF-style axis:
    - All 30m bands labeled
    - Date shown only once per day
    - ETH shaded grey
    - 20d avg baseline at 1.0
    """
    import matplotlib.pyplot as plt
    import numpy as np
    import pandas as pd
    import streamlit as st

    if df.empty:
        st.info("No futures data to display.")
        return

    fut_syms = ETF_TO_FUTURES.get(ticker, [])
    if not fut_syms:
        st.info("No futures mappings found.")
        return

    # --- Load each futures file ---
    rows = []
    for sym in fut_syms:
        path = f"/{ticker.lower()}/{sym.lower()}-timebands-volume/{sym.lower()}_timeband_volume.xlsx"
        try:
            tmp = dbx_read_excel(path, sheet_name="Timebands")
            if tmp is None or tmp.empty:
                continue

            tmp["date"] = pd.to_datetime(tmp["date"], errors="coerce")
            tmp = tmp[tmp["date"].notna()].copy()
            tmp["Root"] = sym.upper()
            rows.append(tmp[["date", "band", "session", "est_vol_at_close", "Root"]])
        except Exception as e:
            st.warning(f"{sym}: {e}")

    if not rows:
        st.info("No futures timeband data found.")
        return

    df = pd.concat(rows, ignore_index=True)

    # --- Combine date + band start for ordering ---
    df["band_start"] = df["band"].str.split("–").str[0]
    df["band_dt"] = pd.to_datetime(
        df["date"].astype(str) + " " + df["band_start"], errors="coerce"
    )
    df = df.sort_values("band_dt")

    # --- Match ETF chart: last 3 trading days ---
    all_dates = sorted(df["date"].unique())
    keep_dates = all_dates[-3:] if len(all_dates) >= 3 else all_dates
    df = df[df["date"].isin(keep_dates)]

    # --- Pivot each futures root into its own column ---
    pivot = (
        df.pivot_table(
            index=["date", "band", "band_dt", "session"],
            columns="Root",
            values="est_vol_at_close",
            aggfunc="mean"
        )
        .reset_index()
        .sort_values("band_dt")
    )
    pivot.columns.name = None
    pivot = pivot.reset_index(drop=True)

    # === Build axis labels identical to ETF Timebands ===
    xticks, prev_date = [], None
    for d, b in zip(pivot["date"], pivot["band"]):
        # Ensure clean types
        d = pd.to_datetime(d, errors="coerce")
        if pd.isna(d):
            xticks.append(b.split("–")[0])
            continue
        cd = d.date()
        if cd != prev_date:
            xticks.append(f"{cd} {b.split('–')[0]}")
            prev_date = cd
        else:
            xticks.append(b.split("–")[0])

    x_vals = np.arange(len(xticks))

    # --- Plot ---
    fig, ax = plt.subplots(figsize=(14, 5))
    width = 0.4
    roots = [c for c in pivot.columns if c not in ["date", "band", "band_dt", "session"]]
    colors = ["#1f77b4", "#66b3ff"]

    for i, root in enumerate(roots):
        y = pivot[root].fillna(0).values
        offset = (i - 0.5) * width
        ax.bar(
            x_vals + offset, y, width=width,
            label=root, color=colors[i % len(colors)], alpha=0.9
        )

    # --- Baseline (1.0 = 20-day avg) ---
    ax.axhline(1.0, linestyle="--", color="gray", linewidth=1, label="20d avg (×1.0)")

    # --- Shade ETH bands ---
    for i, sess in enumerate(pivot["session"]):
        if sess == "ETH":
            ax.axvspan(i - 0.5, i + 0.5, color="grey", alpha=0.15)

    # --- Axis formatting identical to ETF chart ---
    ax.set_xticks(x_vals)
    ax.set_xticklabels(xticks, rotation=90, fontsize=7)
    ax.set_ylabel("Est. Volume Ratio (×)")
    ax.set_title(f"{ticker} Futures Estimated Volume at Close (× vs 20-Day Avg)")
    ax.grid(True, axis="y", linestyle="--", alpha=0.6)
    ax.legend(fontsize=8)
    fig.subplots_adjust(bottom=0.25)
    plt.tight_layout()

    st.pyplot(fig)
    plt.close(fig)





#
# def render_futures_volume_chart(df: pd.DataFrame, ticker: str):
#     """
#     Show est_vol_at_close from ES and MES futures with ETF-style axis:
#     - All 30m bands labeled
#     - Date shown only once per day
#     - ETH shaded grey
#     - 20d avg baseline at 1.0
#     """
#     import matplotlib.pyplot as plt
#     import numpy as np
#     import pandas as pd
#     import streamlit as st
#
#     if df.empty:
#         st.info("No futures data to display.")
#         return
#
#     fut_syms = ETF_TO_FUTURES.get(ticker, [])
#     if not fut_syms:
#         st.info("No futures mappings found.")
#         return
#
#     # --- Load each futures file ---
#     rows = []
#     for sym in fut_syms:
#         path = f"/{ticker.lower()}/{sym.lower()}-timebands-volume/{sym.lower()}_timeband_volume.xlsx"
#         try:
#             tmp = dbx_read_excel(path, sheet_name="Timebands")
#             if tmp is None or tmp.empty:
#                 continue
#             tmp["date"] = pd.to_datetime(tmp["date"], errors="coerce")
#             tmp = tmp[tmp["date"].notna()].copy()
#             tmp["Root"] = sym.upper()
#             rows.append(tmp[["date", "band", "session", "est_vol_at_close", "Root"]])
#         except Exception as e:
#             st.warning(f"{sym}: {e}")
#
#     if not rows:
#         st.info("No futures timeband data found.")
#         return
#
#     df = pd.concat(rows, ignore_index=True)
#
#     # --- Combine date + band time for ordering ---
#     df["band_start"] = df["band"].str.split("–").str[0]
#     df["band_dt"] = pd.to_datetime(
#         df["date"].astype(str) + " " + df["band_start"], errors="coerce"
#     )
#     df = df.sort_values("band_dt")
#
#     # --- Match ETF chart: last 3 trading days ---
#     all_dates = sorted(df["date"].unique())
#     keep_dates = all_dates[-3:] if len(all_dates) >= 3 else all_dates
#     df = df[df["date"].isin(keep_dates)]
#
#     # --- Align by band across all roots ---
#     pivot = (
#         df.pivot_table(
#             index=["date", "band", "band_dt", "session"],
#             columns="Root",
#             values="est_vol_at_close",
#             aggfunc="mean"
#         )
#         .reset_index()
#         .sort_values("band_dt")
#     )
#     pivot.columns.name = None
#
#     # --- Build x-axis labels (ETF-style: one date per day) ---
#     # --- Build x-axis labels using pivot (aligned to plotted bars) ---
#     x_labels = []
#     prev_date = None
#     for _, r in pivot.iterrows():
#         time_label = pd.to_datetime(r["band"].split("–")[0], format="%H:%M",
#                                     errors="coerce").strftime("%H:%M")
#         if r["date"] != prev_date:
#             label = f"{r['date'].strftime('%Y-%m-%d')}\n{time_label}"
#             prev_date = r["date"]
#         else:
#             label = time_label
#         x_labels.append(label)
#
#     x_vals = np.arange(len(x_labels))
#
#     # --- Plot ---
#     fig, ax = plt.subplots(figsize=(14, 5))
#     width = 0.4
#     roots = [c for c in pivot.columns if c not in ["date", "band", "band_dt", "session"]]
#     colors = ["#1f77b4", "#66b3ff"]
#
#     for i, root in enumerate(roots):
#         y = pivot[root].fillna(0).values
#         offset = (i - 0.5) * width
#         ax.bar(x_vals + offset, y, width=width,
#                label=root, color=colors[i % len(colors)], alpha=0.9)
#
#     # --- Baseline (1.0 = 20-day avg) ---
#     ax.axhline(1.0, linestyle="--", color="gray", linewidth=1, label="20d avg (×1.0)")
#
#     # --- Shade ETH bands ---
#     for i, sess in enumerate(pivot["session"]):
#         if sess == "ETH":
#             ax.axvspan(i - 0.5, i + 0.5, color="grey", alpha=0.15)
#
#     ax.set_xticks(x_vals)
#     ax.set_xticklabels(x_labels, rotation=90, fontsize=7)
#     ax.set_ylabel("Est. Volume Ratio (×)")
#     ax.set_title(f"{ticker} Futures Estimated Volume at Close (× vs 20-Day Avg)")
#     ax.grid(True, axis="y", linestyle="--", alpha=0.6)
#     ax.legend(fontsize=8)
#     plt.tight_layout()
#
#     st.pyplot(fig)
#     plt.close(fig)
#
#








# === Utility Functions ===
def safe_read_excel(path, sheet_name, retries=5, delay=1.0):
    for i in range(retries):
        try:
            return pd.read_excel(path, sheet_name=sheet_name, engine="openpyxl")
        except PermissionError:
            if i == retries - 1:
                raise
            time.sleep(delay)


def add_spot_and_target(ax, spot_price, today_target):
    if spot_price is not None:
        ax.axvline(
            spot_price, color="red", linestyle="--",
            linewidth=1.2, label=f"Spot {spot_price:.2f}"
        )
    if today_target is not None and not today_target.empty:
        tgt = today_target.iloc[0]["previous_close"]
        ax.axvline(tgt, color="purple", linestyle="--", alpha=0.6, linewidth=1.0)
        ax.text(
            tgt, ax.get_ylim()[1] * 0.95, "Target",
            color="purple", ha="center", va="top", fontsize=8, rotation=90
        )


def get_base64_image(image_path):
    with open(image_path, "rb") as f:
        data = f.read()
    return base64.b64encode(data).decode()


# === Dashboard Class ===
class LiveStreamDashboard:
    def __init__(self, ticker: str):
        self.ticker = ticker.upper()
        self.dbx = get_dropbox_client()
        self.paths = get_paths(ticker)
        self.snapshot_path = None  # Delay initialization

    def _format_snapshot_label(self, path: str) -> str:
        if not path:
            return "No snapshot"
        fname = os.path.basename(path)
        label = fname.replace(f"{self.ticker.lower()}_options_data_",
                              "").replace(".xlsx", "")
        parts = label.rsplit("_", 1)
        if len(parts) == 2:
            parts[1] = parts[1].replace("-", ":")
            label = " ".join(parts)
        return label

    def _get_latest_snapshot(self):
        options_dir = self.paths["options_dir"]
        try:
            all_entries = []
            res = self.dbx.files_list_folder(options_dir)
            all_entries.extend(res.entries)
            while res.has_more:
                res = self.dbx.files_list_folder_continue(res.cursor)
                all_entries.extend(res.entries)

            entries = [
                (e.client_modified, e.name)
                for e in all_entries
                if e.name.endswith(
                    ".xlsx") and f"{self.ticker.lower()}_options_data" in e.name
            ]
            entries.sort(reverse=True)
            files = [name for _, name in entries]


            if not files:
                return None

        except Exception as e:
            st.error(f"Dropbox listing failed for {self.ticker}: {e}")
            return None

        # build labels for slider
        labels = []
        for f in files:
            label = f.replace(f"{self.ticker.lower()}_options_data_",
                              "").replace(".xlsx", "")
            if "_" in label:
                parts = label.rsplit("_", 1)
                if len(parts) == 2 and "-" in parts[1]:
                    parts[1] = parts[1].replace("-", ":")
                    label = " ".join(parts)
            labels.append(label)

        chosen_label = st.select_slider(
            f"{self.ticker} snapshot",
            options=labels,
            value=labels[0],
            key=f"{self.ticker}_snapshot"
        )
        chosen = files[labels.index(chosen_label)]
        return f"{options_dir}{chosen}"

    def _get_snapshot_timestamp(self):
        if not self.snapshot_path:
            return "No snapshot"
        fname = os.path.basename(self.snapshot_path)
        label = fname.replace(f"{self.ticker.lower()}_options_data_",
                              "").replace(".xlsx", "")
        if "_" in label:
            parts = label.rsplit("_", 1)
            if len(parts) == 2 and "-" in parts[1]:
                parts[1] = parts[1].replace("-", ":")
                label = " ".join(parts)
        return label

    def render_header(self):
        st.title(f"{self.ticker} Targets")
        if self.snapshot_path:
            st.markdown(
                f"**Snapshot:** `{self._format_snapshot_label(self.snapshot_path)}`")

            if self.ticker == "SPY":
                st.info(
                    "SPY → ES (E-mini S&P 500):\n\n1 SPY pt ≈ 10 ES pts • 1 ES pt = 4 ticks = \$12.50 • 1 SPY pt ≈ \$500")

            elif self.ticker == "QQQ":
                st.info(
                    "QQQ → NQ (E-mini Nasdaq-100):\n\n1 QQQ pt ≈ 40 NQ pts • 1 NQ pt = 4 ticks = \$5 • 1 QQQ pt ≈ \$800")

            elif self.ticker == "DIA":
                st.info(
                    "DIA → YM (E-mini Dow):\n\n1 DIA pt ≈ 100 YM pts • 1 YM pt = 1 tick = \$5 • 1 DIA pt ≈ \$500")

            elif self.ticker == "IWM":
                st.info(
                    "IWM → RTY (E-mini Russell 2000):\n\n1 IWM pt ≈ 10 RTY pts • 1 RTY pt = 10 ticks = \$50 • 1 IWM pt ≈ \$500")


    def render_gap_targets(self):
        """Render today's gap target and recent unhit targets (≤10 days old)."""
        try:
            df_gap = read_excel_from_dropbox(self.dbx, self.paths["gaps_file"], "All Data")
            if df_gap.empty:
                st.info("No gap data available.")
                return None

            # --- Normalize dates ---
            df_gap["date"] = pd.to_datetime(df_gap["date"], errors="coerce").dt.date
            df_gap["target_achieved_date"] = pd.to_datetime(
                df_gap.get("target_achieved_date"), errors="coerce"
            ).dt.date

            today = pd.Timestamp.now(tz=ZoneInfo("America/New_York")).date()
            cutoff = today - pd.Timedelta(days=10)

            # --- Conditions ---
            mask_recent = df_gap["date"] >= cutoff
            mask_unhit = df_gap["target_achieved_date"].isna()
            mask_today = df_gap["date"] == today

            # --- Filter: unhit within 10 days + today's target ---
            df_filtered = df_gap[(mask_recent & mask_unhit) | mask_today].copy()

            if df_filtered.empty:
                st.info("No recent unhit or current targets to display.")
                return None

            # --- Today's target(s) ---
            today_target = df_filtered[df_filtered["date"] == today]
            if today_target.empty:
                st.info("No target row for today. Showing latest available.")
                today_target = df_filtered.sort_values("date", ascending=False).head(1).copy()

            # --- Load supporting sheets ---
            df_roll = read_excel_from_dropbox(self.dbx, self.paths["gaps_file"], "Gap Type Stats (DAYS)")
            df_days = read_excel_from_dropbox(self.dbx, self.paths["gaps_file"], "Days to Target")
            df_mam = read_excel_from_dropbox(self.dbx, self.paths["gaps_file"], "MAM (PTS)")
            df_mam_days = read_excel_from_dropbox(self.dbx, self.paths["gaps_file"], "MAM (DAYS)")

            # --- Display filtered set ---
            st.subheader("Unhit Targets (Last 10 Days)")
            st.dataframe(
                df_filtered[["date", "gap_type", "open", "previous_close", "gap value"]]
                .rename(columns={
                    "date": "Date",
                    "gap_type": "Gap Type",
                    "open": "Open",
                    "previous_close": "Previous Close",
                    "gap value": "Gap Value"
                }),
                width="stretch"
            )

            # --- Days-to-Target (Stats in Days) ---
            st.subheader("Days to Target (Stats in Days)")
            stats_table = df_days[df_days["gap_type"].isin(df_filtered["gap_type"])]
            st.dataframe(stats_table, width="stretch")

            # --- MAM (PTS) ---
            st.subheader("Max Adverse Movement Data (PTS)")
            mam_pts = df_mam[df_mam["gap_type"].isin(df_filtered["gap_type"])]
            st.dataframe(mam_pts, width="stretch")

            # --- MAM (DAYS) ---
            st.subheader("Max Adverse Movement Data (DAYS)")
            mam_days = df_mam_days[df_mam_days["gap_type"].isin(df_filtered["gap_type"])]
            st.dataframe(mam_days, width="stretch")

            return today_target if not today_target.empty else None

        except Exception as e:
            st.error(f"Gap load failed: {e}")
            return None

    def render_narratives(self, today_target):
        try:
            dropbox_file = self.snapshot_path
            df_mc = read_excel_from_dropbox(self.dbx, dropbox_file,
                                            "monte carlo")
            df_final = read_excel_from_dropbox(self.dbx, dropbox_file,
                                               "final probs")
            df_pin = read_excel_from_dropbox(self.dbx, dropbox_file,
                                             "pinning metrics")
            df_greeks = read_excel_from_dropbox(self.dbx, dropbox_file,
                                                "greeks")
            df_raw = read_excel_from_dropbox(self.dbx, dropbox_file,
                                             "raw options")

            spot_price = df_raw["spot"].mode().iloc[0]

            st.subheader("Probability Table")
            strike_near = df_mc.iloc[(df_mc["strike"] - today_target["previous_close"].iloc[0]).abs().argsort()[:1]]["strike"].values[0]
            mc_row = df_mc[df_mc["strike"] == strike_near].iloc[0]
            touch_row = df_final[df_final["strike"] == strike_near]

            prob_table = pd.DataFrame([{
                "Date": today_target["date"].iloc[0],
                "Gap Type": today_target["gap_type"].iloc[0],
                "Target": f"{today_target['previous_close'].iloc[0]:.2f}",
                "Spot": f"{spot_price:.2f}",
                "MC Day0": f"{mc_row.get('prob_hit_day_0', np.nan):.0%}",
                "MC Adj": f"{mc_row.get('prob_hit_day_0_adjusted', np.nan):.0%}",
                "Final Touch": f"{touch_row['prob_hit_day_0'].max():.0%}" if not touch_row.empty else "—",
            }])
            st.dataframe(prob_table, width="stretch")


        except Exception as e:
            st.error(f"Narratives failed: {e}")

    def render_charts(self, today_target):
        try:
            dropbox_file = self.snapshot_path
            df_mc = read_excel_from_dropbox(self.dbx, dropbox_file,
                                            "monte carlo")
            df_final = read_excel_from_dropbox(self.dbx, dropbox_file,
                                               "final probs")
            df_pin = read_excel_from_dropbox(self.dbx, dropbox_file,
                                             "pinning metrics")
            df_greeks = read_excel_from_dropbox(self.dbx, dropbox_file,
                                                "greeks")
            df_raw = read_excel_from_dropbox(self.dbx, dropbox_file,
                                             "raw options")

            spot_price = df_raw["spot"].mode().iloc[0]

            strikes = sorted(df_pin["strike"].unique())
            pin_df = df_pin.groupby("strike")[
                "pinning_strength"].max().reset_index().sort_values("strike")
            pin_df["pinning_strength_k"] = pin_df["pinning_strength"] / 1000

            gamma = df_greeks.groupby("strike")[
                ["call_gamma_exposure", "put_gamma_exposure"]].sum()
            gamma["net"] = gamma["call_gamma_exposure"] - gamma[
                "put_gamma_exposure"]
            gamma = gamma.reindex(strikes).fillna(0) / 1e6

            delta = df_greeks.groupby("strike")[
                        ["delta_exposure"]].sum().reindex(strikes).fillna(
                0) / 1e6
            vega = df_greeks.groupby("strike")[["vega_exposure"]].sum().reindex(
                strikes).fillna(0) / 1e6
            theta = df_greeks.groupby("strike")[
                        ["theta_exposure"]].sum().reindex(strikes).fillna(
                0) / 1e6

            col1, col2, col3 = st.columns(3)
            col4, col5 = st.columns(2)

            # ----- PINNING STRENGTH -----
            with col1:
                st.subheader("Pinning Strength")
                st.markdown(
                    "<p style='font-size:14px;'>aggregate intensity of dealer positioning</p>",
                    unsafe_allow_html=True
                )
                pin_plot = pin_df.copy()
                fig = px.bar(
                    pin_plot,
                    x="strike", y="pinning_strength_k",
                    labels={"pinning_strength_k": "×1k", "strike": "Strike"},
                    # title="Pinning Strength",
                    color_discrete_sequence=["blue"]
                )

                wp = weighted_pin(pin_df.rename(
                    columns={"pinning_strength_k": "pin_strength"}),
                                  spot_price, window_abs=10, lam=3.0)
                if wp is not None:
                    fig.add_vline(x=wp, line_dash="dash", line_color="orange")
                                  # annotation_text=f"Weighted Pin {wp:.2f}",
                                  # annotation_position="top",
                                  # annotation=dict(textangle=-90))
                if spot_price is not None:
                    fig.add_vline(x=spot_price, line_dash="dash")
                    #               line_color="red",
                    #               annotation_text=f"Spot {spot_price:.2f}",
                    #               annotation_position="top",
                    #               annotation=dict(textangle=-90))
                if today_target is not None and not today_target.empty:
                    tgt = today_target.iloc[0]["previous_close"]
                    fig.add_vline(x=tgt, line_dash="dash", line_color="purple")
                                  # annotation_text="Target",
                                  # annotation_position="top",
                                  # annotation=dict(textangle=-90))

                # --- legend for vlines ---
                # --- legend for vlines with numeric values ---
                fig.add_scatter(
                    x=[None], y=[None],
                    mode="lines",
                    line=dict(color="purple", dash="dash"),
                    name=f"Target ({tgt:.2f})"
                )
                fig.add_scatter(
                    x=[None], y=[None],
                    mode="lines",
                    line=dict(color="red", dash="dash"),
                    name=f"Spot ({spot_price:.2f})"
                )
                if wp is not None:
                    fig.add_scatter(
                        x=[None], y=[None],
                        mode="lines",
                        line=dict(color="orange", dash="dash"),
                        name=f"Weighted Pin ({wp:.2f})"
                    )

                fig.update_layout(bargap=0.05,
                                  xaxis=dict(tickmode="linear",
                                             tick0=pin_plot["strike"].min(),
                                             dtick=1),
                                  yaxis=dict(showgrid=True))
                st.plotly_chart(fig, use_container_width=True)

            # ----- GAMMA EXPOSURE -----
            with col2:
                st.subheader("Gamma Exposure ")
                st.markdown(
                    "<p style='font-size:14px;'>sensitivity of delta to underlying price changes</p>",
                    unsafe_allow_html=True
                )

                gamma_plot = gamma.reset_index().rename(
                    columns={"index": "strike"})
                fig = px.bar(gamma_plot, x="strike", y="net",
                             labels={"net": "×1M", "strike": "Strike"}
                             # ,
                             # title="Gamma Exposure"
                )
                if spot_price is not None:
                    fig.add_vline(x=spot_price, line_dash="dash",
                                  line_color="red",
                                  annotation_text=f"Spot {spot_price:.2f}",
                                  annotation_position="top",
                                  annotation=dict(textangle=-90))
                if today_target is not None and not today_target.empty:
                    tgt = today_target.iloc[0]["previous_close"]
                    fig.add_vline(x=tgt, line_dash="dash", line_color="purple",
                                  annotation_text="Target",
                                  annotation_position="top",
                                  annotation=dict(textangle=-90))

                fig.add_scatter(x=[None], y=[None], mode="lines",
                                line=dict(color="purple", dash="dash"),
                                name="Target")
                fig.add_scatter(x=[None], y=[None], mode="lines",
                                line=dict(color="red", dash="dash"),
                                name="Spot")

                fig.update_layout(bargap=0.05,
                                  xaxis=dict(tickmode="linear",
                                             tick0=min(gamma.index), dtick=1),
                                  yaxis=dict(showgrid=True))
                st.plotly_chart(fig, use_container_width=True)

            # ----- DELTA EXPOSURE -----
            with col3:
                st.subheader("Delta Exposure")
                st.markdown(
                    "<p style='font-size:14px;'>sensitivity of option value to underlying price changes</p>",
                    unsafe_allow_html=True
                )




                delta_plot = delta.reset_index().rename(
                    columns={"index": "strike"})
                fig = px.bar(delta_plot, x="strike", y="delta_exposure",
                             labels={"delta_exposure": "×1M",
                                     "strike": "Strike"},
                             # title="Delta Exposure",
                             color_discrete_sequence=["green"])
                if spot_price is not None:
                    fig.add_vline(x=spot_price, line_dash="dash",
                                  line_color="red",
                                  annotation_text=f"Spot {spot_price:.2f}",
                                  annotation_position="top",
                                  annotation=dict(textangle=-90))
                if today_target is not None and not today_target.empty:
                    tgt = today_target.iloc[0]["previous_close"]
                    fig.add_vline(x=tgt, line_dash="dash", line_color="purple",
                                  annotation_text="Target",
                                  annotation_position="top",
                                  annotation=dict(textangle=-90))

                fig.add_scatter(x=[None], y=[None], mode="lines",
                                line=dict(color="purple", dash="dash"),
                                name="Target")
                fig.add_scatter(x=[None], y=[None], mode="lines",
                                line=dict(color="red", dash="dash"),
                                name="Spot")

                fig.update_layout(bargap=0.05,
                                  xaxis=dict(tickmode="linear", dtick=1),
                                  yaxis=dict(showgrid=True))
                st.plotly_chart(fig, use_container_width=True)

            # ----- VEGA EXPOSURE -----
            with col4:
                st.subheader("Vega Exposure")

                st.markdown(
                    "<p style='font-size:14px;'>sensitivity of option value to implied volatility changes</p>",
                    unsafe_allow_html=True
                )

                vega_plot = vega.reset_index().rename(
                    columns={"index": "strike"})
                fig = px.bar(vega_plot, x="strike", y="vega_exposure",
                             labels={"vega_exposure": "×1M",
                                     "strike": "Strike"},
                             # title="Vega Exposure",
                             color_discrete_sequence=["purple"])
                if spot_price is not None:
                    fig.add_vline(x=spot_price, line_dash="dash",
                                  line_color="red",
                                  annotation_text=f"Spot {spot_price:.2f}",
                                  annotation_position="top",
                                  annotation=dict(textangle=-90))
                if today_target is not None and not today_target.empty:
                    tgt = today_target.iloc[0]["previous_close"]
                    fig.add_vline(x=tgt, line_dash="dash", line_color="purple",
                                  annotation_text="Target",
                                  annotation_position="top",
                                  annotation=dict(textangle=-90))

                fig.add_scatter(x=[None], y=[None], mode="lines",
                                line=dict(color="purple", dash="dash"),
                                name="Target")
                fig.add_scatter(x=[None], y=[None], mode="lines",
                                line=dict(color="red", dash="dash"),
                                name="Spot")

                fig.update_layout(bargap=0.05,
                                  xaxis=dict(tickmode="linear", dtick=1),
                                  yaxis=dict(showgrid=True))
                st.plotly_chart(fig, use_container_width=True)

            # ----- THETA EXPOSURE -----
            with col5:
                st.subheader("Theta Exposure")
                st.markdown(
                    "<p style='font-size:14px;'>sensitivity of option value to time decay</p>",
                    unsafe_allow_html=True
                )




                theta_plot = theta.reset_index().rename(
                    columns={"index": "strike"})
                fig = px.bar(theta_plot, x="strike", y="theta_exposure",
                             labels={"theta_exposure": "×1M",
                                     "strike": "Strike"},
                             # title="Theta Exposure",
                             color_discrete_sequence=["red"])
                if spot_price is not None:
                    fig.add_vline(x=spot_price, line_dash="dash",
                                  line_color="red",
                                  annotation_text=f"Spot {spot_price:.2f}",
                                  annotation_position="top",
                                  annotation=dict(textangle=-90))
                if today_target is not None and not today_target.empty:
                    tgt = today_target.iloc[0]["previous_close"]
                    fig.add_vline(x=tgt, line_dash="dash", line_color="purple",
                                  annotation_text="Target",
                                  annotation_position="top",
                                  annotation=dict(textangle=-90))

                fig.add_scatter(x=[None], y=[None], mode="lines",
                                line=dict(color="purple", dash="dash"),
                                name="Target")
                fig.add_scatter(x=[None], y=[None], mode="lines",
                                line=dict(color="red", dash="dash"),
                                name="Spot")

                fig.update_layout(bargap=0.05,
                                  xaxis=dict(tickmode="linear", dtick=1),
                                  yaxis=dict(showgrid=True))
                st.plotly_chart(fig, use_container_width=True)

            # --- touch probability plots unchanged ---
            df_bs = read_excel_from_dropbox(self.dbx, self.snapshot_path,
                                            "black scholes probs")
            df_jump = read_excel_from_dropbox(self.dbx, self.snapshot_path,
                                              "jump diffusion probs")
            df_heston = read_excel_from_dropbox(self.dbx, self.snapshot_path,
                                                "heston probs")

            c1, c2 = st.columns(2)
            with c1:
                st.subheader("Touch Probabilities DTE=0")
                fig, ax = plt.subplots(figsize=(6, 4))
                for label, dfp in [("Final", df_final), ("MC", df_mc),
                                   ("BS", df_bs), ("Jump", df_jump),
                                   ("Heston", df_heston)]:
                    ax.plot(dfp["strike"], dfp["prob_hit_day_0"], label=label)
                ax.set_ylim(0, 1)
                ax.grid(True, linestyle="--", alpha=0.6)
                ax.legend(fontsize=7)
                add_spot_and_target(ax, spot_price, today_target)
                st.pyplot(fig)
                plt.close(fig)
            with c2:
                st.subheader("Touch Probabilities DTE=9")
                fig, ax = plt.subplots(figsize=(6, 4))
                for label, dfp in [("Final", df_final), ("MC", df_mc),
                                   ("BS", df_bs), ("Jump", df_jump),
                                   ("Heston", df_heston)]:
                    ax.plot(dfp["strike"], dfp["prob_hit_day_9"], label=label)
                ax.set_ylim(0, 1)
                ax.grid(True, linestyle="--", alpha=0.6)
                ax.legend(fontsize=7)
                add_spot_and_target(ax, spot_price, today_target)
                st.pyplot(fig)
                plt.close(fig)
        except Exception as e:
            st.error(f"Charts failed: {e}")

    def render_timebands(self):
        try:
            self.paths["timebands_file"] = get_dropbox_path(
                self.ticker,
                "timebands",
                f"{self.ticker.lower()}_timeband_volume.xlsx",
            )
            tb = dbx_read_excel(self.paths["timebands_file"], sheet_name="Timebands")
            tb["date"] = pd.to_datetime(tb["date"], errors="coerce")
            tb = tb[tb["date"].notna()].copy()

            dates = sorted(tb["date"].dt.date.unique())
            recent_dates = dates[-3:] if len(dates) >= 3 else dates

            df_plot = (
                tb[tb["date"].dt.date.isin(recent_dates)]
                .copy()
                .sort_values(["date", "band"])
                .reset_index(drop=True)
            )
            if df_plot.empty:
                st.info("No timebands available.")
                return

            # ===== Top chart
            fig, ax1 = plt.subplots(figsize=(14, 5))
            x_vals = np.arange(len(df_plot))
            y_ratio = pd.to_numeric(df_plot["ratio_to_avg_20d"],
                                    errors="coerce").fillna(0).values
            ax1.bar(x_vals, y_ratio, alpha=0.6, label="Vol / 20d Avg")
            ax1.axhline(1.0, linestyle="--", linewidth=1, label="20d avg")
            ax1.set_ylabel("Volume Ratio")
            ax1.set_xticks(x_vals)

            xticks, prev_date = [], None
            for d, b in zip(df_plot["date"], df_plot["band"]):
                cd = d.date()
                xticks.append(f"{cd} {b.split('–')[0]}" if cd != prev_date else
                              b.split("–")[0])
                prev_date = cd
            ax1.set_xticklabels(xticks, rotation=90, fontsize=7)
            ax1.grid(True, linestyle="--", alpha=0.6)
            ax1.legend(fontsize=8)

            if "session" in df_plot.columns:
                for i, sess in enumerate(df_plot["session"]):
                    if sess == "ETH":
                        ax1.axvspan(i - 0.5, i + 0.5, color="grey", alpha=0.2)

            ax2 = ax1.twinx()
            candle_width = 0.6
            for i, r in df_plot.iterrows():
                o, h, l, c = r["open"], r["high"], r["low"], r["close"]
                color = "g" if c >= o else "r"
                ax2.vlines(i, l, h, color=color, linewidth=1)
                ax2.add_patch(plt.Rectangle((i - candle_width / 2, min(o, c)),
                                            candle_width, abs(c - o),
                                            facecolor=color, edgecolor=color))
            ax2.set_ylabel(f"{self.ticker} Price")
            st.pyplot(fig)
            plt.close(fig)

            # ===== Preview table (separate frame)
            df_prev = (
                tb[tb["date"].dt.date.isin(recent_dates)]
                .copy()
                .sort_values(["date", "band"], ascending=[False, False])
                .reset_index(drop=True)
            )
            df_prev["date"] = pd.to_datetime(df_prev["date"]).dt.date

            tz = ZoneInfo("America/New_York")
            now = datetime.now(tz)
            df_prev["est_vol_at_close"] = df_prev["ratio_to_avg_20d"]

            def _dt_on_day(d, hhmm):
                hh, mm = map(int, str(hhmm).split(":"))
                return datetime(d.year, d.month, d.day, hh, mm, tzinfo=tz)

            for i, r in df_prev.iterrows():
                start_dt = _dt_on_day(pd.to_datetime(r["date"]).to_pydatetime(),
                                      r["start"])
                end_dt = _dt_on_day(pd.to_datetime(r["date"]).to_pydatetime(),
                                    r["end"])
                if start_dt <= now < end_dt:
                    elapsed = max(0.01, (now - start_dt).total_seconds() / 60)
                    df_prev.at[i, "est_vol_at_close"] = compute_relative_flow(
                        r["volume"], elapsed, r["avg_20d"])
                    break

            keep_cols = [
                "date", "timestamp", "generated_at", "granularity",
                "session", "open", "high", "low", "close", "start", "end", "band", "barCount", "volume",
                "avg_20d", "ratio_to_avg_20d", "est_vol_at_close"
            ]
            df_prev = df_prev[[c for c in keep_cols if c in df_prev.columns]]
            df_prev = df_prev.sort_values(["date", "timestamp"],
                                          ascending=[False, False])

            st.subheader("Timebands")
            st.dataframe(df_prev, width="stretch")

        # ===== Futures Volume Comparison (Side-by-Side)
            try:
                fut_syms = ETF_TO_FUTURES.get(self.ticker, [])
                if fut_syms:
                    fut_frames = []
                    for sym in fut_syms:
                        path = f"/{self.ticker.lower()}/{sym.lower()}-timebands-volume/{sym.lower()}_timeband_volume.xlsx"
                        fdf = dbx_read_excel(path, sheet_name="Timebands")
                        if fdf is not None and not fdf.empty:
                            fut_frames.append(fdf)
                    if fut_frames:
                        fut_df = pd.concat(fut_frames, ignore_index=True)
                        render_futures_volume_chart(fut_df, self.ticker)
                    else:
                        st.info("No futures volume data available.")
            except Exception as e:
                st.warning(f"Futures volume chart skipped: {e}")

        except Exception as e:
            st.error(f"Timebands render failed: {e}")

        # ===== Futures Timebands Table (aggregate only, no OHLC or per-contract columns) =====
        # ===== Futures Timebands: Separate Tables for ES and MES =====
        try:
            fut_syms = ETF_TO_FUTURES.get(self.ticker, [])
            if fut_syms:
                for sym in fut_syms:
                    path = f"/{self.ticker.lower()}/{sym.lower()}-timebands-volume/{sym.lower()}_timeband_volume.xlsx"
                    fdf = dbx_read_excel(path, sheet_name="Timebands")

                    if fdf is None or fdf.empty:
                        st.info(f"No {sym} timeband data available.")
                        continue

                    fdf["date"] = pd.to_datetime(fdf.get("date"),
                                                 errors="coerce")
                    fdf = fdf[fdf["date"].notna()].copy()
                    fdf["date"] = fdf["date"].dt.date

                    # Keep only aggregate columns (no OHLC, no per-contract)
                    keep_cols = [
                        "date", "timestamp", "generated_at", "granularity",
                        "session", "band", "Total_barCount", "Total_volume",
                        "avg_20d", "ratio_to_avg_20d", "est_vol_at_close"
                    ]
                    fdf = fdf[[c for c in keep_cols if c in fdf.columns]]
                    fdf = fdf.sort_values(["date", "timestamp"],
                                          ascending=[False, False])

                    st.subheader(f"{sym} Futures Timebands")
                    st.dataframe(fdf, width="stretch")
            else:
                st.info("No futures mappings found for this ticker.")
        except Exception as e:
            st.warning(f"Futures timebands table skipped: {e}")

    def render_all(self):
        self.snapshot_path = self._get_latest_snapshot()
        st.write("Using snapshot file:", self.snapshot_path)  # ← move here
        self.render_header()
        today_target = self.render_gap_targets()
        if today_target is not None:
            self.render_narratives(today_target)
            self.render_charts(today_target)
        self.render_timebands()


# === Page Config ===
st.set_page_config(layout="wide")



#=== Banner ===

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
BANNER_PATH = os.path.join(PROJECT_ROOT, "images", "qma_banner.png")



if os.path.exists(BANNER_PATH):
    img_b64 = get_base64_image(BANNER_PATH)
    st.markdown(
        f"""
        <style>.banner-img {{width: 100%; height: 200px; object-fit: cover;}}</style>
        <img src="data:image/png;base64,{img_b64}" class="banner-img">
        """,
        unsafe_allow_html=True,
    )
else:
    st.markdown(
        """
        <div style="
            background:linear-gradient(to right,#2b9348,#55a630);
            padding:20px;
            border-radius:10px;
            width:100%;
            margin:0;
            text-align:center;
        ">
            <h1 style="color:white;margin:0;">Quantitative Market Analysis</h1>
            <p style="color:white;margin:0;">
                Options • Gamma • Pinning • Touch Probabilities
            </p>
        </div>
        """,
        unsafe_allow_html=True,
    )

st.title("Multi-Ticker Dashboard")

# === Tabs ===
tabs = st.tabs(list(TICKER_MAP.keys()) + ["Glossary"])

for tab, ticker in zip(tabs, list(TICKER_MAP.keys()) + ["Glossary"]):
    with tab:
        if ticker == "Glossary":
            st.title("Glossary")
            st.markdown("""
            ### Index ETFs and Futures
            - **SPY → ES (E-mini S&P 500)** – 1 SPY pt ≈ 10 ES pts. 1 ES pt = 4 ticks = $12.50. 1 SPY pt ≈ $500.  
              [CME contract specs](https://www.cmegroup.com/markets/equities/sp/e-mini-sandp500.contractSpecs.html)  
            - **QQQ → NQ (E-mini Nasdaq-100)** – 1 QQQ pt ≈ 40 NQ pts. 1 NQ pt = 4 ticks = $5. 1 QQQ pt ≈ $800.  
              [CME contract specs](https://www.cmegroup.com/markets/equities/nasdaq/nasdaq-100.contractSpecs.html)  
            - **DIA → YM (E-mini Dow)** – 1 DIA pt ≈ 100 YM pts. 1 YM pt = 1 tick = $5. 1 DIA pt ≈ $500.  
              [CME contract specs](https://www.cmegroup.com/markets/equities/dow-jones/dow-jones-industrial-average.contractSpecs.html)  
            - **IWM → RTY (E-mini Russell 2000)** – 1 IWM pt ≈ 10 RTY pts. 1 RTY pt = 10 ticks = $50. 1 IWM pt ≈ $500.  
              [CME contract specs](https://www.cmegroup.com/markets/equities/russell/russell-2000.contractSpecs.html)  

            ### Futures Basics
            - **Futures Contract** – Standardized agreement to buy/sell underlying at a future date. [CME](https://www.cmegroup.com/education/courses/introduction-to-futures/what-are-futures.html)  
            - **Notional Value** – *Contract Multiplier × Futures Price*. Example: ES multiplier = $50; at 5,000 index level, notional = $250,000.  
            - **Tick** – Minimum price fluctuation for a futures contract.  
            - **Tick Value** – Dollar value of one tick. Example: ES tick = 0.25 pts = $12.50.  
            - **Leverage** – Large notional exposure for small margin.  
            - **Initial Margin** – Amount required to open a position. [CME](https://www.cmegroup.com/clearing/margins/)  
            - **Maintenance Margin** – Minimum equity required to keep a position open.  
            - **Intraday Margin** – Lower broker-set requirement for day-only positions. [NinjaTrader](https://ninjatrader.com/futures/blogs/understanding-margin-in-futures-trading/?utm_source=chatgpt.com)  
            - **Expiration** – Date contract ceases trading.  
            - **Settlement** – How position closes: **Cash-settled** (e.g., ES, NQ, YM, RTY) or **Physical** (commodity delivery).  
            - **Roll** – Close near-term contract, open further out.  
            - **Hedger vs. Speculator** – Hedgers reduce risk, speculators trade for profit.  

            ### Margin & Account Terms (NinjaTrader)
            - **Intraday Margin** – Minimum balance to hold a futures position intraday.  
            - **Initial Margin (Overnight)** – Exchange/broker requirement to hold overnight.  
            - **Excess Intraday Margin** – Equity above intraday requirement.  
            - **Excess Initial Margin** – Equity above overnight requirement.  
            - **Commissions** – Broker execution fees.  
              [NinjaTrader Margin Docs](https://ninjatrader.com/pricing/margins-position-management/?utm_source=chatgpt.com)  

            ### Options Basics
            - **Call Option** – Right (not obligation) to buy underlying at strike.  
            - **Put Option** – Right (not obligation) to sell underlying at strike.  
            - **Strike Price** – Price at which option may be exercised.  
            - **Expiration Date** – Last day option can be exercised.  
            - **Premium** – Price paid (buyer) or received (seller).  
            - **In the Money (ITM)** – Call: underlying > strike; Put: underlying < strike.  
            - **Out of the Money (OTM)** – Call: underlying < strike; Put: underlying > strike.  
            - **At the Money (ATM)** – Underlying ≈ strike.  
            - **Intrinsic Value** – Immediate exercise value.  
            - **Extrinsic Value (Time Value)** – Premium minus intrinsic value.  
            - **Exercise** – Using option right.  
            - **Assignment** – Seller’s obligation when option is exercised.  
            - **American vs. European Style** – American = exercisable any time; European = only at expiration.  
            - **Volatility** – Historical (HV) vs Implied (IV).  

            ### Options Positions (with Payoff Formulas & Diagrams)
            - **Long Call** – Buy call; bullish, limited risk, unlimited upside. Payoff = max(0, S − K) − premium.  
              ```

            - **Short Call** – Sell call; bearish/neutral, limited profit, unlimited risk. Payoff = premium − max(0, S − K).  
              ```


            - **Long Put** – Buy put; bearish, limited risk, upside if underlying falls. Payoff = max(0, K − S) − premium.  

            - **Short Put** – Sell put; bullish/neutral, limited profit, large downside risk. Payoff = premium − max(0, K − S).  
              ```
  

            ### Greeks (Exposures)
            - **Delta** – Sensitivity of option price to $1 change in underlying.  
            - **Gamma** – Sensitivity of delta to $1 change in underlying.  
            - **Theta** – Sensitivity of option price to time decay.  
            - **Vega** – Sensitivity of option price to 1% change in IV.  
              [CBOE: Learning the Greeks](https://www.cboe.com/insights/posts/learning-the-greeks-an-experts-perspective/)  

            - **Delta Exposure** – Aggregate delta exposure.  
            - **Gamma Exposure** – Aggregate gamma exposure.  
            - **Theta Exposure** – Aggregate theta exposure.  
            - **Vega Exposure** – Aggregate vega exposure.  
            - **Call Delta Exposure** – Delta from calls only.  
            - **Put Delta Exposure** – Delta from puts only.  
            - **Net Delta Exposure** – Call delta − Put delta.  
            - **Net Gamma Exposure (GEX)** – Call gamma − Put gamma. Indicator of long vs short gamma regime.  
            - **Net Theta Exposure** – Call theta − Put theta.  
            - **Net Vega Exposure** – Call vega − Put vega.  

            ### Gap Analysis / MAM
            - **MAM Date** – Date of max adverse move.  
            - **Days to MAM** – Days until max adverse move.  
            - **Mean / Std / Median MAM (Pts)** – Statistics of adverse move.  
            - **Q1 / Q3 MAM (Pts)** – 25th and 75th percentiles of adverse move.  
            - **Mean / Std / Median Days to MAM** – Statistics of days until MAM.  
            - **1st / 2nd / 3rd Quartile** – Distribution of days-to-target.  
            - **IQR / Upper Bound** – Spread and statistical bound of days-to-target.  

            ### Options Metrics
            - **Open Interest** – Number of outstanding option contracts.  
            - **In the Money** – Option strike favorable vs spot.  
            - **Implied Volatility (IV)** – Market-implied volatility.  

            ### Pinning Metrics
            - **Total Activity** – Combined option activity at strike.  
            - **Pinning Strength** – Strike pinning strength.  
            - **Volume-Based Pin Rank / Influence-Based Pin Rank** – Rank of strike by volume or influence.  
            - **Volume-Based Pin Candidate / Influence-Based Pin Candidate** – Whether strike is a pin candidate.  

            ### Other Columns
            - **Put/Call OI Ratio** – Ratio of put open interest vs call open interest.  
            - **Vega Concentration / Vega Crush Zone / Theta Gravity** – Advanced positioning metrics.  
            - **Strike Wall Tag / OI Gap Zone** – Strikes with large OI clusters or gaps.  
            - **IV Slope** – Implied volatility skew across strikes.  
            - **Gamma Regime** – Market regime by gamma exposure.  
            - **Target vs Expected Move** – Gap target vs implied move.  
            - **Day of Week OI Bias** – Open interest skew by weekday.  

            ### Dealers
            - **Dealer** – Financial institution or trading firm that provides liquidity by taking the other side of client option and futures trades. Dealers hedge inventory using underlying securities/futures, making hedging flows an important driver of price dynamics.  
            - **Major Dealers / Market Makers** – Goldman Sachs, Citadel Securities, Jane Street, SIG, Optiver, IMC Trading, Barclays, Morgan Stanley, Bank of America.  

            ### Dealer Positioning
            - **Long Gamma** – Dealers hedge by selling into rallies and buying into dips (stabilizes).  
            - **Short Gamma** – Dealers hedge by buying into rallies and selling into dips (destabilizes).  
            - **Vanna Exposure** – Sensitivity of delta to changes in IV; vol shocks drive hedge adjustments.  
            - **Charm Exposure** – Delta decay with time; drives intraday rebalancing.  
            - **Vega Positioning** – Long vega = benefit from rising vol; short vega = losses when vol rises.  
            - **Skew Exposure** – Short puts force futures selling into declines.  
            - **Gamma Flip Levels** – Thresholds where dealers switch from long to short gamma regimes.  

            ### Probability Models
            - **Black-Scholes (1973)** – [JSTOR](https://www.jstor.org/stable/1831029)  
            - **Heston (1993)** – [ScienceDirect](https://www.sciencedirect.com/science/article/abs/pii/0304405X9390022N)  
            - **Merton Jump Diffusion (1976)** – [JSTOR](https://www.jstor.org/stable/2326921)  
            - **Monte Carlo** – [Glasserman, *Monte Carlo Methods in Financial Engineering*](https://link.springer.com/book/10.1007/978-0-387-21617-1)  
            """)
        else:
            dash = LiveStreamDashboard(ticker)
            dash.render_all()


# === Auto-refresh ===
REFRESH_INTERVAL = int(TARGET_LOOP_SECONDS)
countdown_placeholder = st.empty()
for i in range(REFRESH_INTERVAL, 0, -1):
    countdown_placeholder.markdown(f"**Refresh in {i} sec**")
    time.sleep(1)
st.rerun()

