import pandas as pd
import matplotlib.pyplot as plt

# Load the latest workbook
file_path = "/mnt/data/spy_options_data_2025-08-19_tue_11-51.xlsx"

# Load relevant sheets
greeks = pd.read_excel(file_path, sheet_name="greeks")
options = pd.read_excel(file_path, sheet_name="options and pinning")
mc = pd.read_excel(file_path, sheet_name="monte carlo")

# Prepare gamma data (net gamma exposure by strike)
gamma_data = greeks.groupby("strike", as_index=False)["net_gamma_exposure"].sum()

# Prepare pinning strength data (take max pinning strength per strike)
pins = options.groupby("strike", as_index=False)["pinning_strength"].max()

# Monte Carlo cumulative probability (touch)
mc_probs = mc.groupby("strike", as_index=False)["cumulative_prob"].mean()

# Create figure
plt.figure(figsize=(12, 7))

# Plot gamma exposures
plt.bar(gamma_data["strike"], gamma_data["net_gamma_exposure"], color="gray", alpha=0.5, label="Gamma")

# Overlay pinning strengths
plt.plot(pins["strike"], pins["pinning_strength"]/pins["pinning_strength"].max() * gamma_data["net_gamma_exposure"].abs().max(),
         color="blue", label="Pinning Strength (scaled)", linewidth=2)

# Overlay Monte Carlo probabilities
plt.plot(mc_probs["strike"], mc_probs["cumulative_prob"]*gamma_data["net_gamma_exposure"].abs().max(),
         color="red", linestyle="--", label="Monte Carlo Probabilities (scaled)", linewidth=2)

# Mark user trade levels
plt.axvline(641.54, color="black", linestyle="--", linewidth=2, label="Entry 641.54")
plt.axvline(635.89, color="green", linestyle="--", linewidth=2, label="Target 635.89")
plt.axvline(644, color="red", linestyle=":", linewidth=2, label="Invalidation 644")

plt.title("Market Positioning Around User's Trade (Aug 19, 11:51)", fontsize=14)
plt.xlabel("Strike")
plt.ylabel("Gamma Exposure / Scaled Metrics")
plt.legend()
plt.grid(alpha=0.3)

plt.tight_layout()
plt.show()
