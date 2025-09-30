import os
import pandas as pd
from openpyxl import Workbook
from datetime import datetime
from collections import defaultdict

# === Config Paths ===
GAP_ANALYSIS_PATH = r"C:\2mdt\2mindt-site\public\spy gap analysis.xlsx"
COMPARISON_DIR = r"C:\2mdt\2mindt-site\public\spy-options\comparison"
STRATEGY_LOG_PATH = r"C:\2mdt\2mindt-site\public\spy-options\gap_target_strategies.xlsx"
TARGET_PREVIEW_PATH = r"C:\2mdt\2mindt-site\public\spy-options\recent_gap_targets.xlsx"

def get_latest_timestamp_column(df):
    cols = [col for col in df.columns if '_' in col and any(x in col for x in ['2025', '20'])]
    return sorted(cols)[-1] if cols else None

def load_gap_targets():
    df_all_data = pd.read_excel(GAP_ANALYSIS_PATH, sheet_name="All Data")
    df_all_data.columns = df_all_data.columns.str.strip()

    df_days_to_hit = pd.read_excel(GAP_ANALYSIS_PATH, sheet_name="Days to Target Gap Types")
    df_days_to_hit = df_days_to_hit.copy()
    df_days_to_hit['gap_type'] = df_days_to_hit.index
    df_days_to_hit.reset_index(drop=True, inplace=True)

    recent_targets = df_all_data.tail(10).copy()

    # Write to Excel for inspection
    wb = Workbook()
    ws = wb.active
    ws.title = "Recent Targets"
    ws.append(recent_targets.columns.tolist())
    for _, row in recent_targets.iterrows():
        ws.append(row.tolist())
    wb.save(TARGET_PREVIEW_PATH)
    print(f"✅ Saved recent_targets to: {TARGET_PREVIEW_PATH}")
    return recent_targets, df_days_to_hit

def load_comparison_data():
    def clean(df):
        df.columns = df.columns.str.strip()
        return df

    df_mc = clean(pd.read_excel(os.path.join(COMPARISON_DIR, "monte_carlo_comparison.xlsx")))
    df_greeks = clean(pd.read_excel(os.path.join(COMPARISON_DIR, "greeks_comparison.xlsx")))
    df_touch = clean(pd.read_excel(os.path.join(COMPARISON_DIR, "touch_probs_comparison.xlsx")))
    df_options = clean(pd.read_excel(os.path.join(COMPARISON_DIR, "options_and_pinning_comparison.xlsx")))
    print("✅ Loaded comparison data files.")
    return df_mc, df_greeks, df_touch, df_options

def build_gap_target_strategies():
    print("Building gap target strategies (latest snapshot)...")
    targets, stats = load_gap_targets()
    targets.columns = targets.columns.str.strip()
    df_mc, df_greeks, df_touch, df_options = load_comparison_data()

    # print("== Column Check ==")
    # print("Options:", df_options.columns.tolist())
    # print("Monte Carlo:", df_mc.columns.tolist())
    # print("Greeks:", df_greeks.columns.tolist())
    # print("Touch Probs:", df_touch.columns.tolist())
    # print("Targets:", targets.columns.tolist())
    # print("== First 2 Target Rows ==")
    # print(targets.head(2).to_dict(orient="records"))
    # print("== End Column Check ==")

    key_cols = ["strike"]
    df = df_options.merge(df_mc, on=key_cols, how="left", suffixes=("", "_mc"))
    if all(col in df_greeks.columns for col in ["strike", "type", "expiration"]):
        df = df.merge(df_greeks, on=["strike", "type", "expiration"], how="left", suffixes=("", "_greeks"))
    else:
        print("WARNING: Missing columns in df_greeks for merge. Skipping merge with df_greeks.")
    if all(col in df_touch.columns for col in ["strike", "type", "expiration"]):
        df = df.merge(df_touch, on=["strike", "type", "expiration"], how="left", suffixes=("", "_touch"))
    else:
        print("WARNING: Missing columns in df_touch for merge. Skipping merge with df_touch.")

    latest_mc = get_latest_timestamp_column(df_mc)
    latest_touch = get_latest_timestamp_column(df_touch)

    required_cols = ['previous_close', 'gap_type']
    for col in required_cols:
        if col not in targets.columns:
            raise KeyError(f"Missing required column: '{col}' in target data.")

    strategies = []

    for i, row in targets.iterrows():
        try:
            if "gap_type" not in row or "previous_close" not in row:
                print(f"⚠️ Row {i} missing required keys. Keys: {row.keys()}")
                continue
            if pd.isna(row["gap_type"]) or pd.isna(row["previous_close"]):
                print(f"⚠️ Skipping row {i} due to missing values in gap_type or previous_close")
                continue
            target = row["previous_close"]
            gap_type = row["gap_type"]
        except KeyError as e:
            print("Row keys:", row.keys())
            raise e

        print(f"\nEvaluating target {target} (gap type: {gap_type})")

        matching_rows = stats[stats['gap_type'] == gap_type]
        if matching_rows.empty:
            print(f"  No stats for gap_type {gap_type} — skipping")
            continue

        days_to_hit_row = matching_rows.iloc[0]

        df_sub = df[(df['dte'] <= 9) & (df['strike'].between(target - 1.0, target + 1.0))].copy()
        if df_sub.empty:
            print(f"  No options near target {target} — skipping")
            continue

        score = 0
        total = 0

        for _, opt in df_sub.iterrows():
            if latest_mc:
                mc_prob = opt.get(latest_mc, 0)
                if mc_prob >= 0.5:
                    score += 1
                total += 1

            pin = opt.get('pinning_strength', 0)
            if pin > 200000:
                score += 1
            total += 1

            delta_exp = abs(opt.get('net_delta_exposure', 0))
            gamma_exp = abs(opt.get('net_gamma_exposure', 0))
            if delta_exp > 10000:
                score += 1
            total += 1
            if gamma_exp > 50000:
                score += 1
            total += 1

            if latest_touch:
                touch_prob = opt.get(latest_touch, 0)
                if touch_prob > 0.4:
                    score += 1
                total += 1

        if total == 0:
            print(f"  No metrics to score — skipping")
            continue

        ratio = score / total
        confidence = (
            "High" if ratio >= 0.75 else
            "Medium" if ratio >= 0.5 else
            "Low"
        )

        strategies.append({
            'target': target,
            'gap_type': gap_type,
            'days_to_hit_mean': days_to_hit_row.get('mean_days', 'N/A'),
            'days_to_hit_median': days_to_hit_row.get('median_days', 'N/A'),
            'options_near_target': len(df_sub),
            'support_ratio': round(ratio, 2),
            'confidence': confidence,
        })

        print(f"  Confidence: {confidence} | Support: {round(ratio, 2)}")

    print("\nWriting strategy results to Excel...")
    wb = Workbook()
    ws = wb.active
    ws.title = "Gap Target Strategies"

    if strategies:
        headers = list(strategies[0].keys())
        ws.append(headers)
        for strat in strategies:
            ws.append([strat[h] for h in headers])
    else:
        ws.append(["No strategies generated."])

    wb.save(STRATEGY_LOG_PATH)
    print(f"✅ Strategy export complete: {STRATEGY_LOG_PATH}")
    return strategies

if __name__ == "__main__":
    print("Starting gap target strategy analysis...")
    build_gap_target_strategies()
    print("Done.")
