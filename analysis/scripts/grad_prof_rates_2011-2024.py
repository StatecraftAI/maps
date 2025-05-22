import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Load data
df = pd.read_csv("data/grad_prof_rates_2011-2024.csv")
df["year"] = df["year"].astype(int)

# Calculate simple "Average Achievement" measure
df["avg_proficiency"] = df[["ela", "math", "science"]].mean(axis=1)

# Identify imputed years
imputed_years = df.loc[
    df[["ela", "math", "science", "graduation"]].isna().any(axis=1), "year"
].tolist()
imputed_years = [2020, 2021]

# Impute missing values for continuity
df.set_index("year", inplace=True)
df = df.reindex(range(df.index.min(), df.index.max() + 1))
df.interpolate(method="linear", inplace=True)
df.reset_index(inplace=True)

# Plot settings: simplified for social media/news
custom_params = {
    "grid.linestyle": ":",
}
sns.set_theme(style="whitegrid", context="talk", rc=custom_params, font_scale=1.0)
plt.rcParams["font.family"] = "Open Sans"

fig, ax = plt.subplots(figsize=(10, 8))

# Plot Avg Proficiency
ax.plot(
    df["year"],
    df["avg_proficiency"],
    label="Avg. Proficiency (ELA, Math, Science)",
    color="#D55E00",
    linewidth=3,
    marker="o",
)

# Plot Graduation Rates
ax.plot(
    df["year"],
    df["graduation"],
    label="Graduation Rate",
    color="#0072B2",
    linewidth=3,
    marker="o",
    linestyle="--",
)

# Titles, labels, and annotations
ax.set_title("Portland Schools: Graduating More, Learning Less?")
ax.set_xlabel("School Year Ending")
ax.set_ylabel("Percentage (%)")
ax.set_xticks(df["year"])
ax.set_xticklabels(df["year"], rotation=45)

# Manually draw arrows from text box to imputed years
for year in imputed_years:
    y_value = 1.01 * df.loc[df["year"] == year, "avg_proficiency"].values[0]
    ax.annotate(
        "",
        xy=(year, y_value),
        xytext=(2021, 65),
        arrowprops=dict(arrowstyle="->", color="black", lw=1.5),
    )

# Add a single annotation text box
annotation_text = "2020 & 2021 values imputed due\nto pandemic-related test disruptions"
ax.annotate(
    annotation_text,
    xy=(2021, 65),
    ha="center",
    va="center",
    fontsize=11,
    color="gray",
    bbox=dict(boxstyle="round,pad=0.3", edgecolor="black", facecolor="white"),
)

# Highlight imputed points with open squares
for year in imputed_years:
    ax.scatter(
        df.loc[df["year"] == year, "year"],
        df.loc[df["year"] == year, "avg_proficiency"],
        facecolors="none",
        edgecolors="black",
        marker="s",
        s=250,
        linewidth=2,
    )

# Clear legend placement below figure
ax.legend(loc="upper center", bbox_to_anchor=(0.5, -0.2), ncol=2, frameon=False)
sns.despine()
plt.tight_layout()
plt.savefig("images/pps_grad_prof_rates_2011-2024.png", dpi=300, bbox_inches="tight")
plt.close()
