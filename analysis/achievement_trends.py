import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

# Load data
df = pd.read_csv("data/grad_prof_rates_2011-2024.csv")

# Convert data types
df["year"] = df["year"].astype(int)
metrics = ["ela", "math", "science", "graduation"]
df[metrics] = df[metrics].apply(pd.to_numeric, errors="coerce")

# Impute missing data
df.set_index("year", inplace=True)
df = df.reindex(range(df.index.min(), df.index.max() + 1))
df_imputed = df.isna()
df.interpolate(method="linear", inplace=True)
df.reset_index(inplace=True)

# Subject names and colors (color-blind friendly)
subject_labels = {
    "ela": "English Language Arts",
    "math": "Mathematics",
    "science": "Science",
    "graduation": "Graduation Rate",
}
colors = sns.color_palette("colorblind", 4)
subject_colors = dict(zip(metrics, colors))

# Plot style
sns.set_theme(style="white", context="talk", font_scale=1.1)
plt.rcParams.update({"axes.spines.right": False, "axes.spines.top": False})
plt.rcParams["font.family"] = "Open Sans"

# Create figure and axes
fig, ax1 = plt.subplots(figsize=(20, 12))
ax2 = ax1.twinx()

# Plot proficiency metrics
for metric in ["ela", "math", "science"]:
    ax1.plot(
        df["year"],
        df[metric],
        marker="o",
        linewidth=2,
        label=subject_labels[metric],
        color=subject_colors[metric],
    )

    # Plot imputed data explicitly
    imputed_mask = df_imputed[metric].values
    ax1.scatter(
        df["year"][imputed_mask],
        df[metric][imputed_mask],
        facecolors="none",
        edgecolors=subject_colors[metric],
        marker="*",
        s=300,
        linewidth=2,
        label=f"Imputed {subject_labels[metric]}",
    )

# Plot graduation on secondary axis
ax2.plot(
    df["year"],
    df["graduation"],
    marker="o",
    linewidth=2,
    linestyle="--",
    color=subject_colors["graduation"],
    label=subject_labels["graduation"],
)

# Imputed graduation values
imputed_grad_mask = df_imputed["graduation"].values
ax2.scatter(
    df["year"][imputed_grad_mask],
    df["graduation"][imputed_grad_mask],
    facecolors="none",
    edgecolors=subject_colors["graduation"],
    marker="*",
    s=200,
    linewidth=2,
)

# Axis labels
ax1.set_xlabel("Year")
ax1.set_ylabel("Proficiency Rates (%)")
ax2.set_ylabel("Graduation Rate (%)")

# X-axis ticks rotated 45 degrees
ax1.set_xticks(df["year"])
ax1.set_xticklabels(df["year"], rotation=45)

# Simplified horizontal legend below figure
lines, labels = ax1.get_legend_handles_labels()
lines2, labels2 = ax2.get_legend_handles_labels()
ax1.legend(
    lines + lines2,
    labels + labels2,
    loc="upper center",
    bbox_to_anchor=(0.5, -0.12),
    ncol=4,
    frameon=False,
)

plt.title("Portland SD 1J Academic Performance All Students All Grades", fontsize=16)
plt.tight_layout(rect=[0, 0.05, 1, 1])
plt.savefig("images/pps_achievement_trends.png", dpi=300, bbox_inches="tight")
plt.close()
