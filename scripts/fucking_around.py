# === daily_close_lines.py ===
from ib_insync import *
import pandas as pd
import matplotlib.pyplot as plt

# --- Load your spreadsheet ---
path = r"C:\Users\colby\Dropbox\Apps\qma_live\spy\spy-gaps-analysis\spy gap analysis.xlsx"
df = pd.read_excel(path, sheet_name="All Data", usecols=["close"])
df = df.dropna().reset_index(drop=True)

# --- Plot each closing value as a horizontal line ---
fig, ax = plt.subplots(figsize=(10, 6))

for i, v in enumerate(df["close"]):
    ax.axhline(y=v, color="white", linewidth=0.8, alpha=0.7)

ax.set_facecolor("black")
fig.patch.set_facecolor("black")
ax.tick_params(colors="white")
ax.set_title("SPY Daily Close Lines", color="white")
plt.show()
