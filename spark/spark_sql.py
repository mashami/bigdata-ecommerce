"""
spark_sql.py  -  Part 2: Spark SQL analytics.

The clean Parquet tables are registered as SQL views named after the system where that data
lives in our architecture, so the queries read like a federated query across both databases:

    mongo_users, mongo_products, mongo_transactions, mongo_items   (MongoDB data)
    hbase_sessions, hbase_page_views                               (HBase data)

  Q1  Conversion funnel by device and referrer          CTE + conditional aggregation
  Q2  Daily revenue + 7-day moving average              window function (ROWS BETWEEN)
  Q3  Category revenue by month, ranked, with growth    RANK() and LAG() window functions
  Q4  Most viewed vs best selling products              HBase views JOIN MongoDB sales
  Q5  Does browsing engagement go with spending?        MongoDB users JOIN HBase sessions, NTILE()

Each result is printed and saved to results/sql_*.csv (used for the charts).
Usage:  python spark/spark_sql.py
"""
import os
import time

import common

t0 = time.time()
spark = common.get_spark("ecommerce-sql")
C, R = common.CLEAN_DIR, common.RESULTS_DIR
os.makedirs(R, exist_ok=True)

for view, table in [("mongo_users", "users"), ("mongo_products", "products"),
                    ("mongo_transactions", "transactions"), ("mongo_items", "transaction_items"),
                    ("hbase_sessions", "sessions"), ("hbase_page_views", "page_views")]:
    spark.read.parquet(f"{C}/{table}").createOrReplaceTempView(view)


def run(name, title, sql, show=12):
    print(f"\n=== {title} ===")
    df = spark.sql(sql)
    pdf = df.toPandas()
    print(pdf.head(show).to_string(index=False))
    if len(pdf) > show:
        print(f"... ({len(pdf)} rows)")
    pdf.to_csv(f"{R}/sql_{name}.csv", index=False)
    return pdf


# ------------------------------------------------------------------------------ Q1
run("funnel_by_device", "Q1  Conversion funnel by device and referrer", """
WITH session_steps AS (               -- one row per session: which funnel steps did it reach?
    SELECT s.session_id, s.device_type, s.referrer,
           MAX(CASE WHEN p.page_type = 'product_detail' THEN 1 ELSE 0 END) AS viewed_product,
           MAX(CASE WHEN p.page_type = 'cart'           THEN 1 ELSE 0 END) AS visited_cart,
           MAX(CASE WHEN p.page_type = 'checkout'       THEN 1 ELSE 0 END) AS reached_checkout,
           MAX(CASE WHEN s.has_order                    THEN 1 ELSE 0 END) AS placed_order
    FROM hbase_sessions s
    JOIN hbase_page_views p ON p.session_id = s.session_id
    GROUP BY s.session_id, s.device_type, s.referrer
)
SELECT COALESCE(device_type, 'ALL')            AS device_type,
       COALESCE(referrer, 'ALL')               AS referrer,
       COUNT(*)                                AS sessions,
       SUM(viewed_product)                     AS viewed_product,
       SUM(visited_cart)                       AS visited_cart,
       SUM(reached_checkout)                   AS reached_checkout,
       SUM(placed_order)                       AS placed_order,
       ROUND(100 * SUM(placed_order) / COUNT(*), 2) AS conversion_pct
FROM session_steps
GROUP BY ROLLUP(device_type, referrer)          -- subtotals per device + a grand total
ORDER BY device_type, referrer
""", show=8)

# ------------------------------------------------------------------------------ Q2
run("daily_revenue", "Q2  Daily revenue with 7-day moving average", """
SELECT day, orders, revenue,
       ROUND(AVG(revenue) OVER (ORDER BY day ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2)
           AS revenue_7d_avg,
       ROUND(SUM(revenue) OVER (ORDER BY day), 2) AS cumulative_revenue
FROM (
    SELECT TO_DATE(timestamp) AS day, COUNT(*) AS orders, ROUND(SUM(total), 2) AS revenue
    FROM mongo_transactions
    GROUP BY TO_DATE(timestamp)
)
ORDER BY day
""", show=8)

# ------------------------------------------------------------------------------ Q3
run("category_monthly", "Q3  Category revenue per month: rank and month-over-month growth", """
WITH monthly AS (
    SELECT DATE_FORMAT(timestamp, 'yyyy-MM')            AS month,
           COUNT(DISTINCT TO_DATE(timestamp))           AS days_in_data,
           category_name,
           SUM(net_revenue)                             AS revenue,
           SUM(net_revenue * profit_margin)             AS est_profit
    FROM mongo_items
    GROUP BY DATE_FORMAT(timestamp, 'yyyy-MM'), category_name
)
SELECT month, category_name,
       ROUND(revenue, 2)                                        AS revenue,
       ROUND(revenue / days_in_data, 2)                         AS revenue_per_day,   -- fair for partial months
       ROUND(est_profit, 2)                                     AS est_profit,
       RANK() OVER (PARTITION BY month ORDER BY revenue DESC)   AS rank_in_month,
       ROUND(100 * (revenue / days_in_data
             / LAG(revenue / days_in_data) OVER (PARTITION BY category_name ORDER BY month) - 1), 1)
                                                                AS growth_per_day_pct
FROM monthly
ORDER BY month, rank_in_month
""", show=10)

# ------------------------------------------------------------------------------ Q4
run("views_vs_sales", "Q4  Most viewed vs best selling products (HBase views JOIN MongoDB sales)", """
WITH views AS (
    SELECT product_id, COUNT(*) AS views, COUNT(DISTINCT session_id) AS viewing_sessions
    FROM hbase_page_views
    WHERE page_type = 'product_detail'
    GROUP BY product_id
),
sales AS (
    SELECT product_id, SUM(quantity) AS units_sold, COUNT(*) AS orders,
           ROUND(SUM(net_revenue), 2) AS revenue,
           SUM(CASE WHEN channel = 'web' THEN 1 ELSE 0 END) AS web_orders
    FROM mongo_items
    GROUP BY product_id
)
SELECT p.product_id, p.name, p.category_name, p.base_price, p.current_stock, p.is_active,
       COALESCE(v.views, 0)      AS views,
       COALESCE(s.units_sold, 0) AS units_sold,
       COALESCE(s.web_orders, 0) AS web_orders,
       COALESCE(s.revenue, 0)    AS revenue,
       ROUND(100 * COALESCE(s.web_orders, 0) / NULLIF(v.viewing_sessions, 0), 2) AS view_to_order_pct,
       RANK() OVER (ORDER BY COALESCE(v.views, 0) DESC)      AS views_rank,
       RANK() OVER (ORDER BY COALESCE(s.units_sold, 0) DESC) AS sales_rank
FROM mongo_products p
LEFT JOIN views v ON v.product_id = p.product_id
LEFT JOIN sales s ON s.product_id = p.product_id
ORDER BY units_sold DESC
""", show=10)

spark.sql("""
SELECT is_active, COUNT(*) AS products, ROUND(AVG(views), 1) AS avg_views,
       ROUND(AVG(units_sold), 1) AS avg_units_sold, ROUND(CORR(views, units_sold), 3) AS corr_views_units
FROM (SELECT p.product_id, p.is_active,
             (SELECT COUNT(*) FROM hbase_page_views v
              WHERE v.product_id = p.product_id AND v.page_type = 'product_detail') AS views,
             COALESCE((SELECT SUM(quantity) FROM mongo_items i
                       WHERE i.product_id = p.product_id), 0) AS units_sold
      FROM mongo_products p)
GROUP BY is_active
""").show()

# ------------------------------------------------------------------------------ Q5
run("engagement_vs_spend", "Q5  Engagement quartile (HBase) vs spending (MongoDB)", """
WITH engagement AS (
    SELECT user_id, COUNT(*) AS sessions, SUM(duration_seconds) / 60.0 AS minutes_on_site,
           SUM(n_pages) AS pages
    FROM hbase_sessions GROUP BY user_id
),
spend AS (
    SELECT user_id, COUNT(*) AS orders, SUM(total) AS spent
    FROM mongo_transactions GROUP BY user_id
),
joined AS (
    SELECT u.user_id, u.country,
           COALESCE(e.sessions, 0) AS sessions, COALESCE(e.minutes_on_site, 0) AS minutes_on_site,
           COALESCE(s.orders, 0) AS orders, COALESCE(s.spent, 0) AS spent,
           NTILE(4) OVER (ORDER BY COALESCE(e.minutes_on_site, 0)) AS engagement_quartile
    FROM mongo_users u
    LEFT JOIN engagement e ON e.user_id = u.user_id
    LEFT JOIN spend s      ON s.user_id = u.user_id
)
SELECT engagement_quartile,
       COUNT(*)                         AS users,
       ROUND(AVG(sessions), 1)          AS avg_sessions,
       ROUND(AVG(minutes_on_site), 0)   AS avg_minutes,
       ROUND(AVG(orders), 2)            AS avg_orders,
       ROUND(AVG(spent), 2)             AS avg_spent,
       ROUND(CORR(minutes_on_site, spent), 3) AS corr_minutes_spent
FROM joined
GROUP BY engagement_quartile
ORDER BY engagement_quartile
""")

# Show the optimiser at work: partition pruning on the date-partitioned sessions table
print("\n=== Physical plan: one week of sessions (note PartitionFilters - only 7 folders read) ===")
plan = spark.sql("""SELECT device_type, COUNT(*) FROM hbase_sessions
                    WHERE session_date BETWEEN '2026-10-01' AND '2026-10-07'
                    GROUP BY device_type""")._jdf.queryExecution().executedPlan().toString()
print("\n".join(line for line in plan.splitlines() if "PartitionFilters" in line or "FileScan" in line)[:600])

print(f"\nDone in {time.time() - t0:,.0f}s")
spark.stop()
