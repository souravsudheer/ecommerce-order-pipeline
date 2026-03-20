"""
generate_charts.py
Generates all four business insight charts from gold CSVs.
Run this from the project root: python notebooks/generate_charts.py
Charts are saved to notebooks/charts/ for use in the notebook and README.
"""

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent))

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker

GOLD_DIR = Path(__file__).resolve().parent.parent / "data" / "gold"
CHARTS_DIR = Path(__file__).resolve().parent / "charts"
CHARTS_DIR.mkdir(exist_ok=True)

plt.rcParams.update({
    "figure.figsize":   (11, 5),
    "axes.spines.top":  False,
    "axes.spines.right": False,
    "axes.grid":        True,
    "grid.alpha":       0.25,
    "grid.linestyle":   "--",
    "font.size":        11,
    "axes.titlesize":   13,
    "axes.titleweight": "bold",
    "axes.labelsize":   11,
    "xtick.labelsize":  10,
    "ytick.labelsize":  10,
})

PALETTE = {
    "North": "#2563EB",
    "South": "#16A34A",
    "East":  "#DC2626",
    "West":  "#D97706",
}


def chart_revenue_by_region():
    rev = pd.read_csv(GOLD_DIR / "gold_revenue_by_region.csv", parse_dates=["order_month"])
    rev = rev.sort_values("order_month")

    fig, ax = plt.subplots()
    for region, grp in rev.groupby("region"):
        ax.plot(
            grp["order_month"], grp["total_revenue"],
            label=region, color=PALETTE[region],
            linewidth=2.2, marker="o", markersize=3.5,
        )

    ax.set_title("Monthly Revenue by Region")
    ax.set_xlabel("Month")
    ax.set_ylabel("Revenue (AUD)")
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: "${:,.0f}".format(x)))
    ax.legend(title="Region", framealpha=0.4)
    fig.tight_layout()
    fig.savefig(CHARTS_DIR / "revenue_by_region.png", dpi=150, bbox_inches="tight")
    plt.close()
    print("  Saved: revenue_by_region.png")


def chart_return_rate_by_category():
    ret = pd.read_csv(GOLD_DIR / "gold_return_rate_by_category.csv")
    ret = ret.sort_values("return_rate_pct", ascending=True)

    fig, ax = plt.subplots(figsize=(9, 4))
    bars = ax.barh(
        ret["category"], ret["return_rate_pct"],
        color=["#93C5FD", "#60A5FA", "#3B82F6", "#1D4ED8"],
        edgecolor="white", height=0.55,
    )

    for bar, val in zip(bars, ret["return_rate_pct"]):
        ax.text(
            bar.get_width() + 0.05,
            bar.get_y() + bar.get_height() / 2,
            "{:.1f}%".format(val),
            va="center", fontsize=10, color="#374151",
        )

    ax.set_title("Return Rate by Product Category")
    ax.set_xlabel("Return Rate (%)")
    ax.set_xlim(0, ret["return_rate_pct"].max() + 1.5)
    ax.grid(axis="y", alpha=0)
    fig.tight_layout()
    fig.savefig(CHARTS_DIR / "return_rate_by_category.png", dpi=150, bbox_inches="tight")
    plt.close()
    print("  Saved: return_rate_by_category.png")


def chart_top_products():
    top = pd.read_csv(GOLD_DIR / "gold_top_products.csv")
    top["short_name"] = top["product_name"].str[:28] + "..."

    fig, ax = plt.subplots(figsize=(12, 5))
    bar_colors = ["#DC2626" if r > 8 else "#2563EB" for r in top["return_rate_pct"]]
    bars = ax.bar(
        range(len(top)), top["total_revenue"],
        color=bar_colors, alpha=0.85, edgecolor="white", width=0.6,
    )

    ax.set_ylabel("Total Revenue (AUD)")
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: "${:,.0f}".format(x)))
    ax.set_xticks(range(len(top)))
    ax.set_xticklabels(top["short_name"], rotation=35, ha="right", fontsize=9)
    ax.set_title("Top 10 Products by Revenue  |  Red = Return Rate > 8%")

    for i, (rev_val, ret_val) in enumerate(zip(top["total_revenue"], top["return_rate_pct"])):
        ax.text(
            i, rev_val + 3000,
            "{:.1f}%".format(ret_val),
            ha="center", va="bottom", fontsize=8.5,
            color="#374151", fontweight="bold",
        )

    fig.tight_layout()
    fig.savefig(CHARTS_DIR / "top_products.png", dpi=150, bbox_inches="tight")
    plt.close()
    print("  Saved: top_products.png")


def chart_customer_segments():
    seg = pd.read_csv(GOLD_DIR / "gold_customer_segments.csv")
    summary = (
        seg.groupby("ltv_segment")
        .agg(
            customer_count=("customer_id", "count"),
            avg_ltv=("lifetime_value", "mean"),
            avg_aov=("avg_order_value", "mean"),
        )
        .reindex(["High", "Mid", "Low"])
        .reset_index()
    )

    fig, ax = plt.subplots(figsize=(8, 5))
    seg_colors = {"High": "#16A34A", "Mid": "#2563EB", "Low": "#9CA3AF"}
    bars = ax.bar(
        summary["ltv_segment"],
        summary["customer_count"],
        color=[seg_colors[s] for s in summary["ltv_segment"]],
        edgecolor="white", width=0.5,
    )

    for bar, row in zip(bars, summary.itertuples()):
        label = "Avg LTV: ${:,.0f}\nAvg Order: ${:,.0f}".format(row.avg_ltv, row.avg_aov)
        ax.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height() + 4,
            label,
            ha="center", va="bottom",
            fontsize=9, color="#1F2937",
            linespacing=1.6,
        )

    ax.set_title("Customer Count by LTV Segment")
    ax.set_xlabel("LTV Segment")
    ax.set_ylabel("Number of Customers")
    ax.set_ylim(0, summary["customer_count"].max() + 60)
    ax.grid(axis="x", alpha=0)
    fig.tight_layout()
    fig.savefig(CHARTS_DIR / "customer_segments.png", dpi=150, bbox_inches="tight")
    plt.close()
    print("  Saved: customer_segments.png")


if __name__ == "__main__":
    print("Generating business insight charts...")
    chart_revenue_by_region()
    chart_return_rate_by_category()
    chart_top_products()
    chart_customer_segments()
    print("\nAll charts saved to: notebooks/charts/")
