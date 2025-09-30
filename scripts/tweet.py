import tweepy
import os
from dotenv import load_dotenv
import pandas as pd
from datetime import datetime
import requests
import pandas_market_calendars as mcal

# === Function to build tweet text ===
def build_targets_tweet(gap_path: str) -> str:
    df_gap = pd.read_excel(gap_path, sheet_name="All Data")
    df_days_to_target = pd.read_excel(gap_path, sheet_name="Days to Target")

    df_gap["date"] = pd.to_datetime(df_gap["date"], errors="coerce")
    latest_index = df_gap["index"].max()

    recent = df_gap.sort_values("date", ascending=False).head(10)
    unhit = recent[recent["target_achieved_date"].isna()].copy()
    if unhit.empty:
        return None

    unhit = unhit.merge(
        df_days_to_target[["gap_type", "Days 10"]],
        on="gap_type",
        how="left"
    )

    # Adjust: use previous_close day (so add +1 to index difference)
    unhit["days_elapsed"] = (latest_index - unhit["index"]) + 1
    unhit["days_elapsed"] = unhit["days_elapsed"].clip(lower=0, upper=10)

    lines = [f"SPY has {len(unhit)} active target(s)."]
    for j, (_, row) in enumerate(unhit.iterrows(), 1):
        price = f"{row['previous_close']:.2f}"
        days_elapsed = int(row["days_elapsed"])
        hit_rate_val = row.get("Days 10", None)
        hit_rate = f"{hit_rate_val:.0%}" if pd.notna(hit_rate_val) else "N/A"
        lines.append(f"T{j}: {price} • {days_elapsed}d/10d • {hit_rate} hit rate")

    lines.append("")
    lines.append("#SPY #Trading #StockMarket")

    return "\n".join(lines)





# === Helper to check quota via raw request ===
def check_quota(bearer_token: str, label=""):
    url = "https://api.twitter.com/2/users/me"
    headers = {"Authorization": f"Bearer {bearer_token}"}
    r = requests.get(url, headers=headers)
    if "x-rate-limit-remaining" in r.headers:
        limit = int(r.headers.get("x-rate-limit-limit"))
        remaining = int(r.headers.get("x-rate-limit-remaining"))
        reset = r.headers.get("x-rate-limit-reset")
        reset_time = datetime.fromtimestamp(int(reset)) if reset else "unknown"
        print(f"Quota {label}: {remaining}/{limit} remaining. Resets at {reset_time}.")
        return remaining
    else:
        print(f"Quota {label}: headers not available.")
        return None

# def main():
#     gap_path = r"C:\2mdt\2mindt-site\public\spy-gaps-analysis\spy gap analysis.xlsx"
#     tweet_text = build_targets_tweet(gap_path)
#
#     if not tweet_text:
#         print("No active SPY targets. Exiting.")
#         return
#
#     print("=== Tweet Preview ===\n")
#     print(tweet_text)
#     print(f"\nCharacter count: {len(tweet_text)}")
#
#
#
# if __name__ == "__main__":
#     main()

# === Main script ===
if __name__ == "__main__":
    load_dotenv()

    api_key = os.getenv("TWITTER_API_KEY")
    api_secret = os.getenv("TWITTER_API_SECRET")
    access_token = os.getenv("TWITTER_ACCESS_TOKEN")
    access_secret = os.getenv("TWITTER_ACCESS_SECRET")
    bearer_token = os.getenv("TWITTER_BEARER_TOKEN")

    client = tweepy.Client(
        consumer_key=api_key,
        consumer_secret=api_secret,
        access_token=access_token,
        access_token_secret=access_secret
    )

    gap_path = r"C:\2mdt\2mindt-site\public\spy-gaps-analysis\spy gap analysis.xlsx"
    tweet_text = build_targets_tweet(gap_path)

    if not tweet_text:
        print("No active SPY targets. Exiting.")
        exit()

    print("Tweet preview:\n")
    print(tweet_text)
    print(f"\nCharacter count: {len(tweet_text)}")

    if len(tweet_text) > 280:
        print("Tweet exceeds 280 chars. Exiting.")
        exit()

    # === Quota BEFORE posting ===
    if bearer_token:
        remaining = check_quota(bearer_token, label="BEFORE posting")
        if remaining is not None and remaining <= 0:
            print("Quota exhausted. Exiting.")
            exit()
    else:
        print("No BEARER TOKEN set, skipping quota check.")

    # === Post tweet ===
    resp = client.create_tweet(text=tweet_text)
    print("\nTweet sent. ID:", resp.data["id"])

    # === Quota AFTER posting ===
    if bearer_token:
        check_quota(bearer_token, label="AFTER posting")
