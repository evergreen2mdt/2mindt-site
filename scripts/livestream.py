import streamlit as st
import matplotlib.pyplot as plt
import os
import time
import pandas as pd
import numpy as np
import base64
from zoneinfo import ZoneInfo

from config import TARGET_LOOP_SECONDS
from gap_functions import CONTRACT_MAP


# === Path helpers ===
from config import get_folder

def get_paths(ticker: str):
    t = ticker.lower()
    return {
        "options_dir": get_folder(ticker, "options"),
        "gaps_file": os.path.join(get_folder(ticker, "gaps"), f"{t} gap analysis.xlsx"),
        "timebands_file": os.path.join(get_folder(ticker, "timebands"), f"{t}_timebands_history.xlsx"),
    }



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
        self.paths = get_paths(ticker)
        self.snapshot_path = self._get_latest_snapshot()
        self.snapshot_timestamp = self._get_snapshot_timestamp()

    def _get_latest_snapshot(self):
        options_dir = self.paths["options_dir"]
        if not os.path.exists(options_dir):
            return None
        files = sorted(
            [f for f in os.listdir(options_dir)
             if f.endswith(
                ".xlsx") and f"{self.ticker.lower()}_options_data" in f and not f.startswith(
                "~$")],
            reverse=True
        )
        if not files:
            return None

        # --- slider with labels, fixing time formatting ---
        labels = []
        for f in files:
            label = f.replace(f"{self.ticker.lower()}_options_data_",
                              "").replace(".xlsx", "")
            # turn "..._16-16" into "... 16:16"
            if "_" in label:
                parts = label.rsplit("_", 1)
                if len(parts) == 2 and "-" in parts[1]:
                    parts[1] = parts[1].replace("-", ":")
                    label = " ".join(parts)
            labels.append(label)

        chosen_label = st.select_slider(
            f"{self.ticker} snapshot",
            options=labels,
            value=labels[0]  # default = most recent
        )
        chosen = files[labels.index(chosen_label)]
        return os.path.join(options_dir, chosen)

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
        if self.snapshot_timestamp:
            st.markdown(f"**Snapshot:** `{self.snapshot_timestamp}`")
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
        try:
            df_gap = pd.read_excel(self.paths["gaps_file"], sheet_name="All Data")
            df_gap["date"] = pd.to_datetime(df_gap["date"], errors="coerce").dt.date
            today = pd.Timestamp.now(tz=ZoneInfo("America/New_York")).date()
            today_target = df_gap[df_gap["date"] == today].copy()
            if today_target.empty:
                st.info("No target row for today. Showing latest available.")
                today_target = df_gap.sort_values("date", ascending=False).head(1).copy()

            # Load supporting sheets
            df_roll = pd.read_excel(self.paths["gaps_file"], sheet_name="Gap Type Stats (DAYS)")
            df_days = pd.read_excel(self.paths["gaps_file"], sheet_name="Days to Target")
            df_mam = pd.read_excel(self.paths["gaps_file"], sheet_name="MAM (PTS)")
            df_mam_days = pd.read_excel(self.paths["gaps_file"], sheet_name="MAM (DAYS)")

            st.subheader("Today's Target Data")
            st.dataframe(today_target[
                ["date", "gap_type", "open", "previous_close", "gap value"]
            ].rename(columns={
                "date": "Date", "gap_type": "Gap Type", "open": "Open",
                "previous_close": "Previous Close", "gap value": "Gap Value"
            }), width="stretch")

            st.subheader("Days to Target (Stats in Days)")
            stats_table = df_days[
                df_days["gap_type"].isin(today_target["gap_type"])]
            st.dataframe(stats_table, width="stretch")

            st.subheader("Max Adverse Movement Data (PTS)")
            mam_pts = df_mam[df_mam["gap_type"].isin(today_target["gap_type"])]
            st.dataframe(mam_pts, width="stretch")

            st.subheader("Max Adverse Movement Data (DAYS)")
            mam_days = df_mam_days[
                df_mam_days["gap_type"].isin(today_target["gap_type"])]
            st.dataframe(mam_days, width="stretch")

            return today_target
        except Exception as e:
            st.error(f"Gap load failed: {e}")
            return None

    def render_narratives(self, today_target):
        try:
            df_mc = safe_read_excel(self.snapshot_path, "monte carlo")
            df_final = safe_read_excel(self.snapshot_path, "final probs")
            df_pin = safe_read_excel(self.snapshot_path, "pinning metrics")
            df_greeks = safe_read_excel(self.snapshot_path, "greeks")
            df_raw = safe_read_excel(self.snapshot_path, "raw options")
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

            st.subheader("Greeks Narrative")
            agg = df_greeks.groupby("strike")[["net_gamma_exposure", "net_delta_exposure", "vega_exposure", "theta_exposure"]].sum().reset_index()
            window = 3
            strikes = range(int(spot_price) - window, int(spot_price) + window + 1)
            focus = agg[agg["strike"].isin(strikes)].copy()
            for col in ["net_gamma_exposure", "net_delta_exposure", "vega_exposure", "theta_exposure"]:
                focus[col] = focus[col].round(0).astype(int)
            st.dataframe(focus, width="stretch")
        except Exception as e:
            st.error(f"Narratives failed: {e}")

    def render_charts(self, today_target):
        try:
            df_pin = safe_read_excel(self.snapshot_path, "pinning metrics")
            df_greeks = safe_read_excel(self.snapshot_path, "greeks")
            df_mc = safe_read_excel(self.snapshot_path, "monte carlo")
            df_final = safe_read_excel(self.snapshot_path, "final probs")
            df_raw = safe_read_excel(self.snapshot_path, "raw options")
            spot_price = df_raw["spot"].mode().iloc[0]

            strikes = sorted(df_pin["strike"].unique())
            pin_df = df_pin.groupby("strike")["pinning_strength"].max().reset_index().sort_values("strike")
            pin_df["pinning_strength_k"] = pin_df["pinning_strength"] / 1000

            gamma = df_greeks.groupby("strike")[["call_gamma_exposure", "put_gamma_exposure"]].sum()
            gamma["net"] = gamma["call_gamma_exposure"] - gamma["put_gamma_exposure"]
            gamma = gamma.reindex(strikes).fillna(0) / 1e6

            delta = df_greeks.groupby("strike")[["delta_exposure"]].sum().reindex(strikes).fillna(0) / 1e6
            vega = df_greeks.groupby("strike")[["vega_exposure"]].sum().reindex(strikes).fillna(0) / 1e6
            theta = df_greeks.groupby("strike")[["theta_exposure"]].sum().reindex(strikes).fillna(0) / 1e6

            col1, col2, col3 = st.columns(3)
            col4, col5 = st.columns(2)

            def render_bar_chart(ax, x_vals, y_vals, title, ylabel, color):
                ax.bar(x_vals, y_vals, color=color, alpha=0.6)
                ax.set_title(title)
                ax.set_ylabel(ylabel)
                ax.set_xticks(x_vals)
                ax.set_xticklabels([str(int(s)) for s in x_vals], rotation=90, fontsize=8)
                ax.grid(True, axis="y", linestyle="--", alpha=0.7)

            with col1:
                fig, ax = plt.subplots(figsize=(6, 4))
                render_bar_chart(ax, pin_df["strike"], pin_df["pinning_strength_k"], "Pinning Strength", "×1k", "blue")
                add_spot_and_target(ax, spot_price, today_target)
                st.pyplot(fig)
                plt.close(fig)

            with col2:
                fig, ax = plt.subplots(figsize=(6, 4))
                ax.bar(gamma.index, gamma["net"], label="Net", color="orange", alpha=0.4)
                ax.set_title("Gamma Exposure"); ax.set_ylabel("×1M"); ax.grid(True)
                add_spot_and_target(ax, spot_price, today_target)
                st.pyplot(fig)
                plt.close(fig)

            with col3:
                fig, ax = plt.subplots(figsize=(6, 4))
                render_bar_chart(ax, delta.index, delta["delta_exposure"], "Delta Exposure", "×1M", "green")
                add_spot_and_target(ax, spot_price, today_target)
                st.pyplot(fig)
                plt.close(fig)

            with col4:
                fig, ax = plt.subplots(figsize=(6, 4))
                render_bar_chart(ax, vega.index, vega["vega_exposure"], "Vega Exposure", "×1M", "purple")
                add_spot_and_target(ax, spot_price, today_target)
                st.pyplot(fig)
                plt.close(fig)

            with col5:
                fig, ax = plt.subplots(figsize=(6, 4))
                render_bar_chart(ax, theta.index, theta["theta_exposure"], "Theta Exposure", "×1M", "red")
                add_spot_and_target(ax, spot_price, today_target)
                st.pyplot(fig)
                plt.close(fig)

            # Touch Probs
            df_bs = safe_read_excel(self.snapshot_path, "black scholes probs")
            df_jump = safe_read_excel(self.snapshot_path, "jump diffusion probs")
            df_heston = safe_read_excel(self.snapshot_path, "heston probs")

            c1, c2 = st.columns(2)
            with c1:
                st.subheader("Touch Probabilities DTE=0")
                fig, ax = plt.subplots(figsize=(6, 4))
                for label, dfp in [("Final", df_final), ("MC", df_mc), ("BS", df_bs), ("Jump", df_jump), ("Heston", df_heston)]:
                    ax.plot(dfp["strike"], dfp["prob_hit_day_0"], label=label)
                ax.set_ylim(0, 1); ax.grid(True, linestyle="--", alpha=0.6); ax.legend(fontsize=7)
                add_spot_and_target(ax, spot_price, today_target)
                st.pyplot(fig)
                plt.close(fig)
            with c2:
                st.subheader("Touch Probabilities DTE=9")
                fig, ax = plt.subplots(figsize=(6, 4))
                for label, dfp in [("Final", df_final), ("MC", df_mc), ("BS", df_bs), ("Jump", df_jump), ("Heston", df_heston)]:
                    ax.plot(dfp["strike"], dfp["prob_hit_day_9"], label=label)
                ax.set_ylim(0, 1); ax.grid(True, linestyle="--", alpha=0.6); ax.legend(fontsize=7)
                add_spot_and_target(ax, spot_price, today_target)
                st.pyplot(fig)
                plt.close(fig)
        except Exception as e:
            st.error(f"Charts failed: {e}")

    def render_timebands(self):
        try:
            tb = pd.read_excel(self.paths["timebands_file"],
                               sheet_name="Timebands")
            # Force conversion to datetime, drop invalid
            tb["date"] = pd.to_datetime(tb["date"], errors="coerce")

            # Make sure we only use valid datetimes
            tb = tb[tb["date"].notna()].copy()

            # Use most recent 1–2 available dates
            dates = sorted(tb["date"].dt.date.unique())
            if len(dates) >= 2:
                recent_dates = dates[-2:]
            else:
                recent_dates = dates
            df = tb[tb["date"].dt.date.isin(recent_dates)].copy().sort_values(
                ["date", "band"]).reset_index(drop=True)

            if df.empty:
                st.info("No timebands available.")
                return

            # Chart (candles + vol ratio vs 20d avg)
            fig, ax1 = plt.subplots(figsize=(14, 5))
            x_vals = np.arange(len(df))
            y_ratio = pd.to_numeric(df["ratio_to_avg_20d"],
                                    errors="coerce").fillna(0).values
            ax1.bar(x_vals, y_ratio, alpha=0.6, label="Vol / 20d Avg")
            ax1.axhline(1.0, linestyle="--", linewidth=1, label="20d avg")
            ax1.set_ylabel("Volume Ratio")
            ax1.set_xticks(x_vals)
            ax1.set_xticklabels(
                [f"{d.date()} {b}" for d, b in zip(df["date"], df["band"])],
                rotation=90, fontsize=7
            )
            ax1.grid(True, linestyle="--", alpha=0.6)
            ax1.legend(fontsize=8)

            # Overlay candles
            ax2 = ax1.twinx()
            candle_width = 0.6
            for i, r in df.iterrows():
                o, h, l, c = r["open"], r["high"], r["low"], r["close"]
                color = "g" if c >= o else "r"
                ax2.vlines(i, l, h, color=color, linewidth=1)
                ax2.add_patch(
                    plt.Rectangle((i - candle_width / 2, min(o, c)),
                                  candle_width, abs(c - o),
                                  facecolor=color, edgecolor=color)
                )
            ax2.set_ylabel(f"{self.ticker} Price")
            st.pyplot(fig)
            plt.close(fig)

            # Raw preview
            st.subheader("Timebands (raw preview)")
            st.dataframe(tb.head(), width="stretch")

        except Exception as e:
            st.info(f"Timebands not available: {e}")

    def render_all(self):
        self.render_header()
        today_target = self.render_gap_targets()
        if today_target is not None:
            self.render_narratives(today_target)
            self.render_charts(today_target)
        self.render_timebands()


# === Page Config ===
st.set_page_config(layout="wide")

# === Banner ===
BANNER_PATH = r"C:\2mdt\2mindt-site\images\qma banner.png"
if os.path.exists(BANNER_PATH):
    img_b64 = get_base64_image(BANNER_PATH)
    st.markdown(
        f"""
        <style>.banner-img {{width: 100%; height: 200px; object-fit: cover;}}</style>
        <img src="data:image/png;base64,{img_b64}" class="banner-img">
        """,
        unsafe_allow_html=True
    )
else:
    st.markdown(
        """
        <div style="background: linear-gradient(90deg, #4CAF50, #2E7D32);
                    padding: 20px; border-radius: 8px; text-align: center;">
            <h1 style="color:white; margin:0;">Quantitative Market Analysis</h1>
            <p style="color:white; margin:0;">Options • Gamma • Pinning • Touch Probabilities</p>
        </div>
        """,
        unsafe_allow_html=True
    )

st.title("Multi-Ticker Dashboard")
# === Tabs ===
tabs = st.tabs(list(CONTRACT_MAP.keys()) + ["Glossary"])

for tab, ticker in zip(tabs, list(CONTRACT_MAP.keys()) + ["Glossary"]):
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
