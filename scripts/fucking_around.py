# livestream.py — Full multi-ticker Streamlit dashboard

import streamlit as st
import matplotlib.pyplot as plt
import os
import time
import pandas as pd
import numpy as np
import base64
from zoneinfo import ZoneInfo

from main import TARGET_LOOP_SECONDS
from gap_functions import CONTRACT_MAP

# === Path helpers ===
def get_paths(ticker: str):
    t = ticker.lower()
    base = r"C:\2mdt\2mindt-site\public"
    return {
        "options_dir": os.path.join(base, t, f"{t}-options-data"),
        "gaps_file": os.path.join(base, t, f"{t}-gaps-analysis", f"{t} gap analysis.xlsx"),
        "timebands_file": os.path.join(base, t, f"{t}-timebands", f"{t}_timebands_history.xlsx"),
    }

# === Utility functions ===
def get_base64_image(image_path):
    with open(image_path, "rb") as f:
        return base64.b64encode(f.read()).decode()

def add_spot_and_target(ax, spot_price, today_target):
    if spot_price is not None:
        ax.axvline(spot_price, color="red", linestyle="--",
                   linewidth=1.2, label=f"Spot {spot_price:.2f}")
    if today_target is not None and not today_target.empty:
        tgt = today_target.iloc[0]["previous_close"]
        ax.axvline(tgt, color="purple", linestyle="--", alpha=0.6, linewidth=1.0)
        ax.text(tgt, ax.get_ylim()[1]*0.95, "Target", color="purple",
                ha="center", va="top", fontsize=8, rotation=90)

def safe_read_excel(path, sheet_name, retries=5, delay=1.0):
    for i in range(retries):
        try:
            return pd.read_excel(path, sheet_name=sheet_name, engine="openpyxl")
        except PermissionError:
            if i == retries - 1:
                raise
            time.sleep(delay)

# === Narrative generators ===
def generate_interpretive_narrative(df_pin, df_greeks, df_mc, df_final, spot_price, today_target):
    try:
        if today_target is None or today_target.empty:
            return pd.DataFrame([{ "Message": "No target for today" }])
        row = today_target.iloc[0]
        date = row["date"]; gap_type = row["gap_type"]; target = row["previous_close"]
        strike_near = df_mc.iloc[(df_mc["strike"] - target).abs().argsort()[:1]]["strike"].values[0]
        mc_row = df_mc[df_mc["strike"] == strike_near].iloc[0]
        touch_row = df_final[df_final["strike"] == strike_near]
        return pd.DataFrame([{
            "Date": date,
            "Gap Type": gap_type,
            "Target": f"{target:.2f}",
            "Spot": f"{spot_price:.2f}",
            "MC Day0": f"{mc_row.get('prob_hit_day_0', np.nan):.0%}",
            "MC Adj": f"{mc_row.get('prob_hit_day_0_adjusted', np.nan):.0%}",
            "Final Touch": f"{touch_row['prob_hit_day_0'].max():.0%}" if not touch_row.empty else "—",
        }])
    except Exception as e:
        return pd.DataFrame([{ "Error": str(e) }])

def generate_greeks_narrative(df_greeks, spot_price, window=3):
    agg = (
        df_greeks.groupby("strike")[["net_gamma_exposure","net_delta_exposure",
                                     "vega_exposure","theta_exposure"]]
        .sum().reset_index()
    )
    strikes = range(int(spot_price) - window, int(spot_price) + window + 1)
    focus = agg[agg["strike"].isin(strikes)].copy()
    for col in ["net_gamma_exposure","net_delta_exposure","vega_exposure","theta_exposure"]:
        focus[col] = focus[col].round(0).astype(int)

    def sign_label(val): return "+" if val > 0 else "−" if val < 0 else "0"
    regime_map = {
        ("+", "+", "+", "+"): "Stable, bullish, IV-up helps, decay supports.",
        ("+", "+", "+", "−"): "Stable, bullish, IV-up helps, decay fights.",
        ("+", "+", "−", "+"): "Stable, bullish, IV-down helps, decay supports.",
        ("+", "+", "−", "−"): "Stable, bullish, IV-down helps, decay fights.",
        ("+", "−", "+", "+"): "Stable, bearish, IV-up helps shorts, decay supports.",
        ("+", "−", "+", "−"): "Stable, bearish, IV-up helps, decay fights.",
        ("+", "−", "−", "+"): "Stable, bearish, IV-down hurts shorts, decay supports.",
        ("+", "−", "−", "−"): "Stable, bearish, IV-down helps shorts, decay fights.",
        ("−", "+", "+", "+"): "Unstable, bullish, IV-up helps, decay supports.",
        ("−", "+", "+", "−"): "Unstable, bullish, IV-up helps, decay fights.",
        ("−", "+", "−", "+"): "Unstable, bullish, IV-down hurts, decay supports.",
        ("−", "+", "−", "−"): "Unstable, bullish, IV-down helps, decay fights.",
        ("−", "−", "+", "+"): "Unstable, bearish, IV-up helps shorts, decay supports.",
        ("−", "−", "+", "−"): "Unstable, bearish, IV-up helps, decay fights.",
        ("−", "−", "−", "+"): "Unstable, bearish, IV-down hurts shorts, decay supports.",
        ("−", "−", "−", "−"): "Unstable, bearish, IV-down helps shorts, decay fights.",
    }
    rows=[]
    for _,r in focus.iterrows():
        g,d,v,t = map(sign_label,[r["net_gamma_exposure"],r["net_delta_exposure"],
                                  r["vega_exposure"],r["theta_exposure"]])
        combo=(g,d,v,t); regime=regime_map.get(combo,"Unclassified")
        rows.append({"Strike":int(r["strike"]),
                     "Net Gamma":f"{r['net_gamma_exposure']/1e6:.0f}M",
                     "Net Delta":f"{r['net_delta_exposure']/1e6:.2f}M",
                     "Net Vega":f"{r['vega_exposure']:.0f}",
                     "Net Theta":f"{r['theta_exposure']:.0f}",
                     "Combo (G,D,V,T)":"".join(combo),
                     "Regime":regime})
    return pd.DataFrame(rows)

# === Per-ticker dashboard ===
def render_ticker_dashboard(ticker: str, paths: dict):
    st.subheader(f"{ticker} Dashboard")

    # Discover latest snapshot
    files = sorted([f for f in os.listdir(paths["options_dir"])
                    if f.endswith(".xlsx") and f"{ticker.lower()}_options_data" in f and not f.startswith("~$")],
                   reverse=True)
    if not files:
        st.error(f"No snapshot files for {ticker}")
        return
    snapshot_file = files[0]
    snapshot_path = os.path.join(paths["options_dir"], snapshot_file)

    # Gap targets and stats
    try:
        df_gap=pd.read_excel(paths["gaps_file"],sheet_name="All Data")
        df_gap["date"]=pd.to_datetime(df_gap["date"],errors="coerce").dt.date
        today=pd.Timestamp.now(tz=ZoneInfo("America/New_York")).date()
        today_target=df_gap[df_gap["date"]==today].copy()
        if today_target.empty:
            today_target=df_gap.sort_values("date",ascending=False).head(1).copy()
            st.info("No target for today. Showing latest available.")
        st.subheader("Today's Target Data")
        target_data=today_target[["date","gap_type","open","previous_close","gap value"]]
        st.dataframe(target_data)

        df_roll=pd.read_excel(paths["gaps_file"],sheet_name="Rolling Stats (DAYS)")
        df_days=pd.read_excel(paths["gaps_file"],sheet_name="Days to Target")
        df_mam=pd.read_excel(paths["gaps_file"],sheet_name="MAM (PTS)")
        df_mam_days=pd.read_excel(paths["gaps_file"],sheet_name="MAM (DAYS)")
        st.subheader("Days to Target Stats"); st.dataframe(df_roll.head())
        st.subheader("MAM (PTS)"); st.dataframe(df_mam.head())
        st.subheader("MAM (DAYS)"); st.dataframe(df_mam_days.head())
    except Exception as e:
        st.error(f"Gap analysis failed: {e}")
        today_target=None

    # Load options sheets
    try:
        df_mc=pd.read_excel(snapshot_path,"monte carlo")
        df_final=pd.read_excel(snapshot_path,"final probs")
        df_pin=pd.read_excel(snapshot_path,"pinning metrics")
        df_greeks=pd.read_excel(snapshot_path,"greeks")
        df_raw=pd.read_excel(snapshot_path,"raw options")
        spot_price=df_raw["spot"].mode().iloc[0]
    except Exception as e:
        st.error(f"Options load failed: {e}")
        return

    # Narratives
    st.subheader("Probability Table")
    st.dataframe(generate_interpretive_narrative(df_pin,df_greeks,df_mc,df_final,spot_price,today_target))
    st.subheader("Greeks Narrative")
    st.dataframe(generate_greeks_narrative(df_greeks,spot_price))

    # Charts
    try:
        df_bs=pd.read_excel(snapshot_path,"black scholes probs")
        df_jump=pd.read_excel(snapshot_path,"jump diffusion probs")
        df_heston=pd.read_excel(snapshot_path,"heston probs")

        strikes=sorted(df_final["strike"].unique())

        # Pinning chart
        pin_df=df_pin.groupby("strike")["pinning_strength"].max().reset_index().sort_values("strike")
        pin_df["pinning_strength_k"]=pin_df["pinning_strength"]/1000
        fig,ax=plt.subplots(figsize=(6,4))
        ax.bar(pin_df["strike"],pin_df["pinning_strength_k"])
        add_spot_and_target(ax,spot_price,today_target); st.pyplot(fig)

        # Gamma chart
        gamma=df_greeks.groupby("strike")[["call_gamma_exposure","put_gamma_exposure"]].sum()
        gamma["net"]=gamma["call_gamma_exposure"]-gamma["put_gamma_exposure"]
        fig,ax=plt.subplots(figsize=(6,4))
        ax.bar(gamma.index,gamma["net"]/1e6,color="orange")
        add_spot_and_target(ax,spot_price,today_target); st.pyplot(fig)

        # Touch probabilities
        c1,c2=st.columns(2)
        with c1:
            fig,ax=plt.subplots(figsize=(6,4))
            ax.plot(strikes,df_final.set_index("strike")["prob_hit_day_0"],label="Final")
            ax.plot(strikes,df_mc.set_index("strike")["prob_hit_day_0"],label="MC")
            ax.plot(strikes,df_bs.set_index("strike")["prob_hit_day_0"],label="BS")
            ax.plot(strikes,df_jump.set_index("strike")["prob_hit_day_0"],label="Jump")
            ax.plot(strikes,df_heston.set_index("strike")["prob_hit_day_0"],label="Heston")
            ax.legend(); st.pyplot(fig)
        with c2:
            fig,ax=plt.subplots(figsize=(6,4))
            ax.plot(strikes,df_final.set_index("strike")["prob_hit_day_9"],label="Final")
            ax.plot(strikes,df_mc.set_index("strike")["prob_hit_day_9"],label="MC")
            ax.plot(strikes,df_bs.set_index("strike")["prob_hit_day_9"],label="BS")
            ax.plot(strikes,df_jump.set_index("strike")["prob_hit_day_9"],label="Jump")
            ax.plot(strikes,df_heston.set_index("strike")["prob_hit_day_9"],label="Heston")
            ax.legend(); st.pyplot(fig)

        # Timebands
        try:
            tb=pd.read_excel(paths["timebands_file"],sheet_name="Timebands")
            tb["date"]=pd.to_datetime(tb["date"],errors="coerce").dt.date
            today_et=pd.Timestamp.now(tz=ZoneInfo("America/New_York")).date()
            prev_et=(pd.Timestamp.now(tz=ZoneInfo("America/New_York"))-pd.Timedelta(days=1)).date()
            df=tb[tb["date"].isin([prev_et,today_et])].copy()
            fig,ax=plt.subplots(figsize=(12,4))
            x_vals=np.arange(len(df))
            y_ratio=pd.to_numeric(df["ratio_to_avg_20d"],errors="coerce").fillna(0)
            ax.bar(x_vals,y_ratio,alpha=0.6); ax.axhline(1.0,linestyle="--")
            st.pyplot(fig)
        except Exception as e:
            st.info(f"No timebands for {ticker}: {e}")

    except Exception as e:
        st.error(f"Charts failed: {e}")

# === Page layout ===
st.set_page_config(layout="wide")
BANNER_PATH=r"C:\2mdt\2mindt-site\images\qma banner.png"
if os.path.exists(BANNER_PATH):
    img_b64=get_base64_image(BANNER_PATH)
    st.markdown(f"<img src='data:image/png;base64,{img_b64}' style='width:100%;height:200px;object-fit:cover;'>",unsafe_allow_html=True)
else:
    st.markdown("<h1>Quantitative Market Analysis</h1>",unsafe_allow_html=True)
st.title("Multi-Ticker Dashboard")

# Tabs
tabs=st.tabs(list(CONTRACT_MAP.keys()))
for tab,ticker in zip(tabs,CONTRACT_MAP.keys()):
    with tab:
        render_ticker_dashboard(ticker,get_paths(ticker))

# Auto-refresh
REFRESH_INTERVAL=int(TARGET_LOOP_SECONDS)
countdown_placeholder=st.empty()
for i in range(REFRESH_INTERVAL,0,-1):
    countdown_placeholder.markdown(f"**Refresh in {i} sec**")
    time.sleep(1)
st.rerun()
