"""
spark_clean.py  -  Part 2 (batch): clean and normalise the raw JSON dataset with Spark.

Input : data/*.json  (raw generator output: JSON arrays)
Output: data/clean/<table>/  as Parquet (columnar, compressed, typed - far faster to re-read than JSON)
          users, categories, products, transactions, transaction_items, sessions (partitioned by date),
          page_views
        results/data_quality_report.csv

Cleaning steps
  1. Explicit schemas (spark/common.py) so every field has a fixed type.
  2. Parse ISO-8601 strings into real timestamps; count anything that fails to parse.
  3. Standardise categorical text (trim + lower case; country codes upper case).
  4. Remove duplicate primary keys.
  5. Handle missing values: transactions without a session -> channel 'offline';
     page views without product/category are expected for home/search/cart pages (kept as null).
  6. Validate amounts: quantity > 0, line subtotal = qty x unit price, order subtotal = sum of lines,
     total = subtotal - discount. Invalid rows are dropped and counted.
  7. Referential integrity: drop transactions/sessions for unknown users, items for unknown products.
  8. Fix generator inconsistencies found during exploration:
       - sessions marked 'converted' with no matching transaction -> 'converted_no_order'
       - users.last_active earlier than their real last activity -> recomputed from sessions + orders

Usage:  python spark/spark_clean.py
"""
import os
import time

from pyspark.sql import functions as F

import common

t0 = time.time()
spark = common.get_spark("ecommerce-clean")
D, OUT = common.DATA_DIR, common.CLEAN_DIR
os.makedirs(common.RESULTS_DIR, exist_ok=True)
report = []   # (table, check, rows_affected, action)


def read(name, schema):
    return spark.read.schema(schema).option("multiLine", True).json(os.path.join(D, name))


def ts(col):
    return F.to_timestamp(F.col(col))


def norm(col):
    return F.lower(F.trim(F.col(col)))


def note(table, check, n, action):
    report.append((table, check, int(n), action))
    print(f"  [{table}] {check}: {n:,} -> {action}")


def dedupe(df, key, table):
    before = df.count()
    df = df.dropDuplicates([key]).filter(F.col(key).isNotNull())
    note(table, f"duplicate or null {key}", before - df.count(), "removed")
    return df, before


def bad_ts(df, cols, table):
    for c in cols:
        note(table, f"unparseable/missing {c}", df.filter(F.col(c).isNull()).count(), "reported")


print("Reading raw JSON ...")
# ---------------------------------------------------------------- categories (flattened hierarchy)
categories = read("categories.json", common.CATEGORIES)
subcats = (categories
           .select("category_id", F.trim("name").alias("category_name"),
                   F.explode("subcategories").alias("s"))
           .select("category_id", "category_name",
                   F.col("s.subcategory_id").alias("subcategory_id"),
                   F.trim("s.name").alias("subcategory_name"),
                   F.col("s.profit_margin").alias("profit_margin")))
subcats.write.mode("overwrite").parquet(f"{OUT}/categories")
note("categories", "subcategories (flattened)", subcats.count(), "written")

# ---------------------------------------------------------------- users
users = read("users.json", common.USERS)
users, raw_users = dedupe(users, "user_id", "users")
users = users.select(
    "user_id",
    F.trim("geo_data.city").alias("city"),
    F.upper(F.trim("geo_data.state")).alias("state"),
    F.upper(F.trim("geo_data.country")).alias("country"),
    ts("registration_date").alias("registration_date"),
    ts("last_active").alias("last_active_raw"))
bad_ts(users, ["registration_date"], "users")
user_ids = users.select("user_id")

# ---------------------------------------------------------------- products
products = read("products.json", common.PRODUCTS)
products, _ = dedupe(products, "product_id", "products")
products = (products
            .withColumn("n_prices", F.size("price_history"))
            .withColumn("min_price", F.array_min("price_history.price"))
            .withColumn("max_price", F.array_max("price_history.price"))
            .select("product_id", F.trim("name").alias("name"), "category_id", "subcategory_id",
                    "base_price", "current_stock", "is_active", "n_prices", "min_price", "max_price",
                    ts("creation_date").alias("creation_date"))
            .join(subcats, ["category_id", "subcategory_id"], "left"))
note("products", "missing subcategory/category link",
     products.filter(F.col("category_name").isNull()).count(), "kept, category_name='unknown'")
products = products.fillna({"category_name": "unknown", "subcategory_name": "unknown",
                            "profit_margin": 0.0})
note("products", "non-positive price", products.filter(F.col("base_price") <= 0).count(), "removed")
products = products.filter(F.col("base_price") > 0)
products.write.mode("overwrite").parquet(f"{OUT}/products")
product_ids = products.select("product_id")

# ---------------------------------------------------------------- transactions + items
tx = read("transactions.json", common.TRANSACTIONS)
tx, _ = dedupe(tx, "transaction_id", "transactions")
tx = (tx.withColumn("timestamp", ts("timestamp"))
        .withColumn("status", norm("status"))
        .withColumn("payment_method", norm("payment_method"))
        .withColumn("channel", F.when(F.col("session_id").isNull(), "offline").otherwise("web")))
bad_ts(tx, ["timestamp"], "transactions")
note("transactions", "no session_id (missing value)", tx.filter(F.col("session_id").isNull()).count(),
     "kept, channel='offline'")

n = tx.count()
tx = tx.join(user_ids, "user_id", "left_semi")
note("transactions", "unknown user_id", n - tx.count(), "removed")

items = (tx.select("transaction_id", F.posexplode("items").alias("line_no", "i"))
           .select("transaction_id", "line_no", "i.product_id", "i.quantity", "i.unit_price", "i.subtotal"))
bad_lines = items.filter((F.col("quantity") <= 0) | (F.col("unit_price") <= 0) |
                         (F.abs(F.col("quantity") * F.col("unit_price") - F.col("subtotal")) > 0.01))
note("transaction_items", "invalid quantity/price/line subtotal", bad_lines.count(), "order removed")
n = items.count()
orphan_lines = items.join(product_ids, "product_id", "left_anti")
note("transaction_items", "unknown product_id", orphan_lines.count(), "order removed")

# order-level checks: subtotal = sum(lines), total = subtotal - discount
line_sums = items.groupBy("transaction_id").agg(F.round(F.sum("subtotal"), 2).alias("lines_sum"),
                                               F.count("*").alias("n_items"),
                                               F.sum("quantity").alias("n_units"))
tx = tx.join(line_sums, "transaction_id", "left")
bad_orders = tx.filter((F.col("n_items").isNull()) |
                       (F.abs(F.col("lines_sum") - F.col("subtotal")) > 0.01) |
                       (F.abs(F.col("subtotal") - F.col("discount") - F.col("total")) > 0.01) |
                       (F.col("total") < 0))
note("transactions", "empty order or amounts that don't add up", bad_orders.count(), "removed")
drop_ids = (bad_orders.select("transaction_id")
            .union(bad_lines.select("transaction_id"))
            .union(orphan_lines.select("transaction_id")).distinct())
tx = tx.join(drop_ids, "transaction_id", "left_anti").drop("items", "lines_sum")
tx = tx.withColumn("discount_rate", F.round(F.col("discount") / F.col("subtotal"), 2))
tx.write.mode("overwrite").parquet(f"{OUT}/transactions")

items = (items.join(drop_ids, "transaction_id", "left_anti")
              .join(tx.select("transaction_id", "user_id", "timestamp", "channel",
                              (F.col("total") / F.col("subtotal")).alias("paid_ratio")),
                    "transaction_id")
              .join(products.select("product_id", "category_id", "category_name", "profit_margin"),
                    "product_id", "left")
              .withColumn("net_revenue", F.round(F.col("subtotal") * F.col("paid_ratio"), 2))
              .drop("paid_ratio"))
items.write.mode("overwrite").parquet(f"{OUT}/transaction_items")

# ---------------------------------------------------------------- sessions + page views
sess = read("sessions_*.json", common.SESSIONS)
sess, _ = dedupe(sess, "session_id", "sessions")
sess = (sess.withColumn("start_time", ts("start_time"))
            .withColumn("end_time", ts("end_time"))
            .withColumn("conversion_status", norm("conversion_status"))
            .withColumn("referrer", norm("referrer")))
bad_ts(sess, ["start_time", "end_time"], "sessions")
note("sessions", "end_time before start_time",
     sess.filter(F.col("end_time") < F.col("start_time")).count(), "reported")
n = sess.count()
sess = sess.join(user_ids, "user_id", "left_semi")
note("sessions", "unknown user_id", n - sess.count(), "removed")

pv = (sess.select("session_id", "user_id", F.posexplode("page_views").alias("seq", "p"))
          .select("session_id", "user_id", "seq",
                  F.to_timestamp("p.timestamp").alias("timestamp"),
                  norm("p.page_type").alias("page_type"),
                  F.col("p.product_id").alias("product_id"),
                  F.col("p.category_id").alias("category_id"),
                  F.col("p.view_duration").alias("view_duration")))
note("page_views", "missing product_id on product_detail page",
     pv.filter((F.col("page_type") == "product_detail") & F.col("product_id").isNull()).count(),
     "reported")
pv.write.mode("overwrite").parquet(f"{OUT}/page_views")

web_orders = tx.filter(F.col("session_id").isNotNull()).groupBy("session_id").agg(
    F.count("*").alias("orders_in_session"), F.sum("total").alias("order_total"))
cart_value = F.aggregate(F.map_values("cart_contents"), F.lit(0.0),
                         lambda acc, x: acc + x["quantity"] * x["price"])
sess = (sess.join(web_orders, "session_id", "left")
            .withColumn("has_order", F.col("orders_in_session").isNotNull())
            .withColumn("conversion_status_clean",
                        F.when((F.col("conversion_status") == "converted") & ~F.col("has_order"),
                               "converted_no_order").otherwise(F.col("conversion_status"))))
note("sessions", "'converted' but no matching transaction",
     sess.filter(F.col("conversion_status_clean") == "converted_no_order").count(),
     "relabelled 'converted_no_order'")

sess_flat = sess.select(
    "session_id", "user_id", "start_time", "end_time", "duration_seconds",
    F.to_date("start_time").alias("session_date"),
    F.upper(F.trim("geo_data.country")).alias("country"),
    norm("device_profile.type").alias("device_type"),
    norm("device_profile.os").alias("os"),
    norm("device_profile.browser").alias("browser"),
    "referrer", "conversion_status", "conversion_status_clean", "has_order",
    F.coalesce("order_total", F.lit(0.0)).alias("order_total"),
    F.size("page_views").alias("n_pages"),
    F.size("viewed_products").alias("n_products_viewed"),
    F.size(F.map_keys("cart_contents")).alias("n_cart_items"),
    F.round(cart_value, 2).alias("cart_value"),
    "viewed_products",
    F.map_keys("cart_contents").alias("cart_products"))
# Partition by day: queries filtering on a date range only read the matching folders
(sess_flat.repartition("session_date")
          .write.mode("overwrite").partitionBy("session_date").parquet(f"{OUT}/sessions"))

# ---------------------------------------------------------------- users: fix last_active
sess_flat = spark.read.parquet(f"{OUT}/sessions")
last_seen = (sess_flat.groupBy("user_id").agg(F.max("end_time").alias("t"))
             .union(tx.groupBy("user_id").agg(F.max("timestamp").alias("t")))
             .groupBy("user_id").agg(F.max("t").alias("last_seen")))
users = (users.join(last_seen, "user_id", "left")
              .withColumn("last_active", F.greatest("last_active_raw", "last_seen")))
note("users", "last_active earlier than real last activity",
     users.filter(F.col("last_seen") > F.col("last_active_raw")).count(),
     "recomputed from sessions + orders")
users.drop("last_seen").write.mode("overwrite").parquet(f"{OUT}/users")

# ---------------------------------------------------------------- summary
report_df = spark.createDataFrame(report, ["table", "check", "rows_affected", "action"])
report_df.toPandas().to_csv(f"{common.RESULTS_DIR}/data_quality_report.csv", index=False)
print("\nClean tables written to", OUT)
for name in ["users", "categories", "products", "transactions", "transaction_items",
             "sessions", "page_views"]:
    print(f"  {name:<18} {spark.read.parquet(f'{OUT}/{name}').count():>10,} rows")
print(f"Done in {time.time() - t0:,.0f}s")
spark.stop()
