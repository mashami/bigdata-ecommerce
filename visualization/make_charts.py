"""
make_charts.py  -  Part 4: static charts for the report (PNG, 200 dpi) from the CSVs in results/.

  1_daily_revenue.png        sales performance over time
  2_conversion_funnel.png    funnel stages, session -> order
  3_clv_tiers.png            customer segmentation by CLV (small multiples)
  4_views_vs_sales.png       product performance: most viewed vs best selling
  5_category_profit.png      revenue vs estimated profit per category

Run after: mongodb/aggregations.py, spark/spark_sql.py, integration/clv_integration.py
Usage:  python visualization/make_charts.py
"""
import os

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt          # noqa: E402
import matplotlib.ticker as mtick         # noqa: E402
import pandas as pd                       # noqa: E402

R, OUT = "results", "visualization/charts"
os.makedirs(OUT, exist_ok=True)

# palette (light mode): one accent hue, an ordinal blue ramp, neutral context colours
SURFACE, INK, INK2, MUTED, GRID = "#fcfcfb", "#0b0b0b", "#52514e", "#8a8984", "#e6e5e1"
BLUE, ORANGE = "#2a78d6", "#eb6834"
RAMP = ["#184f95", "#256abf", "#3987e5", "#6da7ec", "#86b6ef"]     # dark -> light, ordinal

plt.rcParams.update({
    "figure.facecolor": SURFACE, "axes.facecolor": SURFACE, "savefig.facecolor": SURFACE,
    "font.family": "DejaVu Sans", "font.size": 10, "text.color": INK,
    "axes.edgecolor": GRID, "axes.labelcolor": INK2, "axes.titlesize": 13,
    "axes.titleweight": "bold", "axes.titlelocation": "left", "axes.titlepad": 22,
    "xtick.color": INK2, "ytick.color": INK2, "axes.spines.top": False,
    "axes.spines.right": False, "axes.grid": True, "grid.color": GRID, "grid.linewidth": 0.8,
    "axes.axisbelow": True,
})
money = mtick.FuncFormatter(lambda v, _: f"${v / 1e6:.1f}M" if abs(v) >= 1e6 else f"${v / 1e3:.0f}K")


def subtitle(ax, text):
    ax.text(0, 1.02, text, transform=ax.transAxes, color=INK2, fontsize=9.5, va="bottom")


def save(fig, name):
    fig.tight_layout()
    fig.savefig(os.path.join(OUT, name), dpi=200)
    plt.close(fig)
    print("saved", os.path.join(OUT, name))


# ---------------------------------------------------------------- 1. daily revenue
d = pd.read_csv(f"{R}/sql_daily_revenue.csv", parse_dates=["day"])
fig, ax = plt.subplots(figsize=(9, 4.2))
ax.plot(d["day"], d["revenue"], color=MUTED, lw=1, alpha=0.8, label="Daily revenue")
ax.plot(d["day"], d["revenue_7d_avg"], color=BLUE, lw=2.2, label="7-day moving average")
full = d.iloc[1:-1]                                   # first and last day are partial days
avg = full["revenue"].mean()
ax.axhline(avg, color=INK2, lw=0.8, ls=(0, (4, 3)))
ax.text(d["day"].iloc[3], avg * 1.05, f"average \\${avg / 1e3:,.0f}K per full day", color=INK2, fontsize=9, va="bottom")
ax.yaxis.set_major_formatter(money)
ax.set_ylim(0, d["revenue"].max() * 1.15)
ax.set_title(f"Revenue is flat at about \\${avg / 1e3:,.0f}K a day: no trend or seasonality")
subtitle(ax, f"Net revenue per day, {d['day'].min():%d %b} - {d['day'].max():%d %b %Y}; "
             f"total \\${d['revenue'].sum() / 1e6:,.1f}M (first and last days are partial)")
ax.legend(frameon=False, loc="lower center", ncol=2)
save(fig, "1_daily_revenue.png")

# ---------------------------------------------------------------- 2. conversion funnel
f = pd.read_csv(f"{R}/sql_funnel_by_device.csv")
row = f[(f.device_type == "ALL") & (f.referrer == "ALL")].iloc[0]
stages = [("Sessions", row.sessions), ("Viewed a product", row.viewed_product),
          ("Opened the cart", row.visited_cart), ("Reached checkout", row.reached_checkout),
          ("Placed an order", row.placed_order)]
fig, ax = plt.subplots(figsize=(9, 4.2))
ax.grid(False)
for i, (label, n) in enumerate(stages):
    y = len(stages) - 1 - i
    ax.barh(y, n, color=RAMP[min(i, 4)], height=0.62)
    pct_total = n / stages[0][1]
    step = "" if i == 0 else f"   ({n / stages[i - 1][1]:.0%} of previous step)"
    ax.text(n + stages[0][1] * 0.01, y, f"{n:,.0f}  ·  {pct_total:.1%}{step}", va="center",
            color=INK, fontsize=9.5)
ax.set_yticks(range(len(stages)), [s[0] for s in reversed(stages)])
ax.set_xticks([])
ax.spines["bottom"].set_visible(False)
ax.set_xlim(0, stages[0][1] * 1.55)
ax.set_title("The biggest leak is at checkout: only 29% of checkouts become orders")
subtitle(ax, "Sessions reaching each step (page views from HBase sessions, orders from MongoDB)")
save(fig, "2_conversion_funnel.png")

# ---------------------------------------------------------------- 3. CLV tiers
t = pd.read_csv(f"{R}/clv_tiers.csv").set_index("tier").loc[["Platinum", "Gold", "Silver", "Bronze"]]
panels = [("share_of_clv_pct", "Share of 12-month CLV", "{:.0f}%"),
          ("avg_recency_days", "Days since last order", "{:.0f}"),
          ("avg_orders", "Orders in 90 days", "{:.1f}"),
          ("avg_minutes", "Minutes browsing in 90 days", "{:,.0f}")]
fig, axes = plt.subplots(1, 4, figsize=(11, 3.8), sharey=True)
for ax, (col, label, fmt) in zip(axes, panels):
    ax.barh(range(4), t[col], color=RAMP[:4], height=0.6)
    for y, v in enumerate(t[col]):
        ax.text(v, y, " " + fmt.format(v), va="center", fontsize=9, color=INK)
    ax.set_xlim(0, t[col].max() * 1.35)
    ax.set_xticks([])
    ax.grid(False)
    ax.spines["bottom"].set_visible(False)
    ax.set_title(label, fontsize=10, pad=6, color=INK2, fontweight="normal")
axes[0].set_yticks(range(4), [f"{tier}\n({n:,} users)" for tier, n in t["users"].items()])
axes[0].invert_yaxis()
fig.suptitle("Top 25% of customers hold 42% of future value; it comes from recency and frequency,\n"
             "not from browsing time", x=0.01, ha="left", fontsize=13, fontweight="bold")
save(fig, "3_clv_tiers.png")

# ---------------------------------------------------------------- 4. views vs sales
p = pd.read_csv(f"{R}/sql_views_vs_sales.csv")
instock = p[p.is_active & (p.current_stock > 0)]
soldout = p[p.is_active & (p.current_stock == 0)]
inact = p[~p.is_active]
fig, ax = plt.subplots(figsize=(9, 5))
ax.scatter(instock["views"], instock["units_sold"], s=9, color=BLUE, alpha=0.35, linewidths=0,
           label=f"In stock ({len(instock):,}): median {instock.units_sold.median():.0f} units sold")
ax.scatter(soldout["views"], soldout["units_sold"], s=12, color=ORANGE, alpha=0.8, linewidths=0,
           label=f"Sold out ({len(soldout):,}): median {soldout.units_sold.median():.0f} units sold")
ax.scatter(inact["views"], inact["units_sold"], s=22, color=MUTED, linewidths=0,
           label=f"Inactive ({len(inact):,}): never shown, never sold")
low = instock[instock.sales_rank <= 20].sort_values("current_stock").iloc[0]
ax.annotate(f"{low.product_id}: #{int(low.sales_rank)} best seller,\nonly {int(low.current_stock)} units left",
            (low.views, low.units_sold), xytext=(low.views - 95, low.units_sold + 4),
            fontsize=9, color=INK, va="center",
            arrowprops=dict(arrowstyle="-", color=INK2, lw=0.8))
ax.set_ylim(-5, p["units_sold"].max() * 1.15)
ax.set_xlabel("Product page views (HBase sessions)")
ax.set_ylabel("Units sold (MongoDB transactions)")
ax.set_title(f"{len(soldout)} products sold out, then vanished from browsing and stopped selling")
subtitle(ax, "Each dot is a product. Once stock hits zero the site stops showing it, "
             "so views and sales stall at about half")
ax.legend(frameon=False, loc="lower right")
save(fig, "4_views_vs_sales.png")

# ---------------------------------------------------------------- 5. category revenue vs profit
c = (pd.read_csv(f"{R}/sql_category_monthly.csv").groupby("category_name")[["revenue", "est_profit"]]
       .sum().sort_values("est_profit"))
c["margin"] = c["est_profit"] / c["revenue"]
fig, ax = plt.subplots(figsize=(9, 6.5))
y = range(len(c))
ax.barh(y, c["revenue"], color="#dcdad5", height=0.7, label="Net revenue")
ax.barh(y, c["est_profit"], color=BLUE, height=0.7, label="Estimated profit")
for i, (rev, prof, m) in enumerate(zip(c["revenue"], c["est_profit"], c["margin"])):
    ax.text(rev, i, f"  {m:.0%} margin", va="center", fontsize=8, color=INK2)
ax.set_yticks(list(y), c.index, fontsize=8.5)
ax.xaxis.set_major_formatter(money)
ax.set_xlim(0, c["revenue"].max() * 1.18)
ax.grid(axis="y", visible=False)
ax.set_title("Categories earn similar revenue but very different profit")
subtitle(ax, f"Revenue \\${c.revenue.min() / 1e6:.1f}M-{c.revenue.max() / 1e6:.1f}M per category, "
             f"profit \\${c.est_profit.min() / 1e6:.2f}M-{c.est_profit.max() / 1e6:.2f}M "
             f"(revenue x subcategory margin)")
ax.legend(frameon=False, loc="lower right", bbox_to_anchor=(1.0, -0.12), ncol=2)
save(fig, "5_category_profit.png")
