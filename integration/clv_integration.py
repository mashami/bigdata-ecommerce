"""
clv_integration.py  -  Part 3: integrated analytics - Customer Lifetime Value (CLV).

Business question
  Which customers are worth the most over the next 12 months, how does their browsing behaviour
  differ, and which high-engagement / low-value customers should marketing target?

Workflow (each system does what it is best at)
  1. MongoDB  -> user profiles + the computed purchase_summary, and per-user profit computed by an
                 aggregation pipeline that runs INSIDE MongoDB (only 10k summary rows travel).
  2. HBase    -> per-session engagement (family 's' only: duration, pages, status, cart value),
                 read with a full-table scan through Thrift (narrow: 6 columns of 1 family).
  3. Spark    -> joins the two sources, aggregates sessions per user, computes RFM features,
                 the CLV estimate and the segments.
  4. MongoDB  <- the CLV score and tier are written back into each user document, so the
                 application can show them with a normal profile read (closing the loop).

CLV model (simple, explainable - assumptions stated in the report)
  observed window      = 90 days of data
  monthly_profit       = estimated gross profit in the window / 3   (profit = net revenue x margin)
  retention_prob       = 0.9 if last order <= 14 days ago, 0.7 if <= 30, 0.4 if <= 60, else 0.15
  CLV_12m              = monthly_profit x 12 x retention_prob
  tier                 = CLV quartile (Platinum / Gold / Silver / Bronze)

Usage:
  python integration/clv_integration.py                    # live MongoDB + HBase
  python integration/clv_integration.py --sessions-from parquet   # if HBase is down: same data from Parquet
"""
import argparse
import os
import sys
import time
from datetime import datetime, timezone

from pymongo import MongoClient, UpdateOne
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.types import (DoubleType, IntegerType, StringType, StructField, StructType,
                               TimestampType)

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "spark"))
import common  # noqa: E402

parser = argparse.ArgumentParser()
parser.add_argument("--sessions-from", choices=["hbase", "parquet"], default="hbase")
parser.add_argument("--no-writeback", action="store_true", help="Don't write CLV back to MongoDB")
args = parser.parse_args()

MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27018")
HBASE_HOST = os.environ.get("HBASE_HOST", "localhost")
R = common.RESULTS_DIR
os.makedirs(R, exist_ok=True)
t0 = time.time()
spark = common.get_spark("ecommerce-clv")


def step(msg):
    print(f"[{time.time() - t0:6.1f}s] {msg}")


# ------------------------------------------------------------------ 1. MongoDB extract
mongo = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
db = mongo["ecommerce"]
step(f"MongoDB {mongo.server_info()['version']}: reading users + per-user profit")

users_rows = [(u["_id"], u["geo_data"]["country"], u["registration_date"],
               u["purchase_summary"]["order_count"], float(u["purchase_summary"]["total_spent"]),
               float(u["purchase_summary"]["avg_order_value"]), u["purchase_summary"]["last_purchase"])
              for u in db.users.find({}, {"geo_data.country": 1, "registration_date": 1,
                                          "purchase_summary": 1})]

# Pushed-down aggregation: profit per user is computed inside MongoDB,
# using the margin copied into each line item at load time (no join needed).
profit_pipeline = [
    {"$addFields": {"paid_ratio": {"$cond": [{"$gt": ["$subtotal", 0]},
                                              {"$divide": ["$total", "$subtotal"]}, 1]}}},
    {"$unwind": "$items"},
    {"$group": {"_id": "$user_id",
                "est_profit": {"$sum": {"$multiply": ["$items.subtotal", "$paid_ratio",
                                                      "$items.profit_margin"]}},
                "units": {"$sum": "$items.quantity"},
                "categories": {"$addToSet": "$items.category_id"},
                "discounted_orders": {"$addToSet": {"$cond": [{"$gt": ["$discount", 0]}, "$_id", None]}}}},
    {"$project": {"est_profit": 1, "units": 1, "n_categories": {"$size": "$categories"},
                  "n_discounted": {"$size": {"$setDifference": ["$discounted_orders", [None]]}}}},
]
profit_rows = [(r["_id"], float(r["est_profit"]), r["units"], r["n_categories"], r["n_discounted"])
               for r in db.transactions.aggregate(profit_pipeline, allowDiskUse=True)]
window_end = db.transactions.find_one(sort=[("timestamp", -1)])["timestamp"]

users_df = spark.createDataFrame(users_rows, StructType([
    StructField("user_id", StringType()), StructField("country", StringType()),
    StructField("registration_date", TimestampType()), StructField("orders", IntegerType()),
    StructField("total_spent", DoubleType()), StructField("avg_order_value", DoubleType()),
    StructField("last_purchase", TimestampType())]))
profit_df = spark.createDataFrame(profit_rows, StructType([
    StructField("user_id", StringType()), StructField("est_profit", DoubleType()),
    StructField("units", IntegerType()), StructField("n_categories", IntegerType()),
    StructField("n_discounted_orders", IntegerType())]))
step(f"  {len(users_rows):,} users, {len(profit_rows):,} buyer profit rows; data ends {window_end:%Y-%m-%d}")

# ------------------------------------------------------------------ 2. HBase extract
session_schema = StructType([
    StructField("user_id", StringType()), StructField("start_time", StringType()),
    StructField("duration_seconds", IntegerType()), StructField("n_pages", IntegerType()),
    StructField("status", StringType()), StructField("cart_value", DoubleType())])

if args.sessions_from == "hbase":
    import happybase
    step(f"HBase {HBASE_HOST}:9090: scanning user_sessions (family 's', 5 columns)")
    hb = happybase.Connection(HBASE_HOST, port=9090, timeout=300000)
    cols = [b"s:start_time", b"s:duration", b"s:page_count", b"s:status", b"s:cart_value"]
    rows = []
    for key, d in hb.table("user_sessions").scan(columns=cols, batch_size=5000):
        rows.append((key.split(b"#", 1)[0].decode(), d[b"s:start_time"].decode(),
                     int(d[b"s:duration"]), int(d[b"s:page_count"]), d[b"s:status"].decode(),
                     float(d[b"s:cart_value"])))
        if len(rows) % 50000 == 0:
            step(f"  {len(rows):,} sessions read")
    hb.close()
    sessions_df = spark.createDataFrame(rows, session_schema) \
        .withColumn("start_time", F.to_timestamp("start_time"))
    del rows
else:
    step("Reading sessions from Parquet (HBase fallback)")
    sessions_df = spark.read.parquet(f"{common.CLEAN_DIR}/sessions").select(
        "user_id", "start_time", F.col("duration_seconds").cast("int"),
        F.col("n_pages").cast("int"), F.col("conversion_status").alias("status"), "cart_value")
step(f"  {sessions_df.count():,} sessions loaded into Spark")

# ------------------------------------------------------------------ 3. Spark: join + model
engagement = sessions_df.groupBy("user_id").agg(
    F.count("*").alias("sessions"),
    F.round(F.sum("duration_seconds") / 60, 1).alias("minutes_on_site"),
    F.round(F.avg("n_pages"), 1).alias("avg_pages"),
    F.sum(F.when(F.col("status") == "abandoned", 1).otherwise(0)).alias("abandoned_carts"),
    F.round(F.sum(F.when(F.col("status") == "abandoned", F.col("cart_value"))), 2)
     .alias("abandoned_cart_value"),
    F.max("start_time").alias("last_session"))

end = F.lit(window_end)
clv = (users_df.join(profit_df, "user_id", "left").join(engagement, "user_id", "left")
       .fillna(0, ["est_profit", "units", "n_categories", "n_discounted_orders", "sessions",
                   "minutes_on_site", "avg_pages", "abandoned_carts", "abandoned_cart_value"])
       .withColumn("recency_days", F.datediff(end, F.col("last_purchase")))
       .withColumn("retention_prob",
                   F.when(F.col("recency_days") <= 14, 0.9).when(F.col("recency_days") <= 30, 0.7)
                    .when(F.col("recency_days") <= 60, 0.4).when(F.col("recency_days").isNotNull(), 0.15)
                    .otherwise(0.0))
       .withColumn("monthly_profit", F.col("est_profit") / 3)
       .withColumn("clv_12m", F.round(F.col("monthly_profit") * 12 * F.col("retention_prob"), 2))
       .withColumn("orders_per_session",
                   F.round(F.col("orders") / F.when(F.col("sessions") > 0, F.col("sessions")), 3)))

w = Window.orderBy(F.desc("clv_12m"))
clv = (clv.withColumn("clv_quartile", F.ntile(4).over(w))
          .withColumn("tier", F.element_at(F.array(*[F.lit(t) for t in
                                                     ["Platinum", "Gold", "Silver", "Bronze"]]),
                                           F.col("clv_quartile")))
          .drop("clv_quartile")
          .cache())

# Opportunity segment: browse a lot, leave carts behind, but sit in the bottom half of value
eng_q75 = clv.approxQuantile("minutes_on_site", [0.75], 0.01)[0]
clv = clv.withColumn("retarget_candidate",
                     (F.col("tier").isin("Silver", "Bronze")) & (F.col("minutes_on_site") >= eng_q75)
                     & (F.col("abandoned_carts") >= 3))

tiers = (clv.groupBy("tier").agg(
            F.count("*").alias("users"),
            F.round(F.sum("clv_12m"), 0).alias("total_clv_12m"),
            F.round(F.avg("clv_12m"), 0).alias("avg_clv_12m"),
            F.round(F.avg("total_spent"), 0).alias("avg_spent_90d"),
            F.round(F.avg("orders"), 1).alias("avg_orders"),
            F.round(F.avg("avg_order_value"), 0).alias("avg_order_value"),
            F.round(F.avg("recency_days"), 1).alias("avg_recency_days"),
            F.round(F.avg("sessions"), 1).alias("avg_sessions"),
            F.round(F.avg("minutes_on_site"), 0).alias("avg_minutes"),
            F.round(F.avg("abandoned_cart_value"), 0).alias("avg_abandoned_value"),
            F.sum(F.col("retarget_candidate").cast("int")).alias("retarget_candidates"))
         .withColumn("share_of_clv_pct",
                     F.round(100 * F.col("total_clv_12m") / F.sum("total_clv_12m").over(Window.partitionBy()), 1))
         .orderBy(F.desc("avg_clv_12m")))
print("\n=== CLV tiers ===")
tiers_pd = tiers.toPandas()
print(tiers_pd.to_string(index=False))
tiers_pd.to_csv(f"{R}/clv_tiers.csv", index=False)

corr = clv.filter("orders > 0").select(F.corr("minutes_on_site", "clv_12m"),
                                       F.corr("sessions", "orders"),
                                       F.corr("recency_days", "clv_12m")).first()
print(f"\ncorrelation  minutes_on_site~CLV = {corr[0]:.3f}   sessions~orders = {corr[1]:.3f}   "
      f"recency~CLV = {corr[2]:.3f}")

opp = clv.filter("retarget_candidate").agg(F.count("*"), F.sum("abandoned_cart_value")).first()
print(f"retargeting opportunity: {opp[0]:,} Silver/Bronze users with top-25% engagement and 3+ "
      f"abandoned carts, ${opp[1] or 0:,.0f} left in abandoned carts")

print("\n=== Top 10 customers by CLV ===")
clv.orderBy(F.desc("clv_12m")).select(
    "user_id", "country", "orders", F.round("total_spent", 0).alias("spent_90d"),
    "recency_days", "sessions", "minutes_on_site", "clv_12m", "tier").show(10)

clv_pd = clv.drop("registration_date").toPandas()
clv_pd.to_csv(f"{R}/clv_users.csv", index=False)
step(f"saved {R}/clv_users.csv and {R}/clv_tiers.csv")

# ------------------------------------------------------------------ 4. write back to MongoDB
if not args.no_writeback:
    ops = [UpdateOne({"_id": r.user_id},
                     {"$set": {"clv": {"clv_12m": float(r.clv_12m), "tier": r.tier,
                                       "retention_prob": float(r.retention_prob),
                                       "retarget_candidate": bool(r.retarget_candidate),
                                       "computed_at": datetime.now(timezone.utc)}}})
           for r in clv_pd.itertuples()]
    res = db.users.bulk_write(ops, ordered=False)
    db.users.create_index("clv.tier")
    step(f"wrote CLV back to MongoDB users ({res.modified_count:,} documents updated)")

spark.stop()
step("done")
