"""
spark_recommendations.py  -  Part 2 (batch): product recommendation indicators.

  A. "Users who bought X also bought Y"   - user-level co-purchase (market-basket association rules)
  B. "Products frequently viewed together" - session-level co-view
  C. Top-5 recommendations per product     - A and B combined, ranked with a window function
  D. Category affinity                     - the same rules one level up (25 categories)
  E. Random-shuffle baseline               - is the co-occurrence real signal or chance?

Association-rule metrics for a rule X -> Y (N = number of baskets):
  support    = baskets with X and Y / N
  confidence = baskets with X and Y / baskets with X          (P(Y | X))
  lift       = confidence / (baskets with Y / N)              (>1: bought together more than chance)

Input : data/clean/  (run spark_clean.py first)
Output: data/clean/recommendations/ (Parquet), results/rec_*.csv

Usage:  python spark/spark_recommendations.py [--product prod_02076]
"""
import argparse
import os
import time

from pyspark.sql import Window
from pyspark.sql import functions as F

import common

parser = argparse.ArgumentParser()
parser.add_argument("--product", default="prod_02076", help="Product to show recommendations for")
parser.add_argument("--min-copurchase", type=int, default=2)
parser.add_argument("--min-coview", type=int, default=3)
args = parser.parse_args()

t0 = time.time()
spark = common.get_spark("ecommerce-recommendations")
C, R = common.CLEAN_DIR, common.RESULTS_DIR
os.makedirs(R, exist_ok=True)

items = spark.read.parquet(f"{C}/transaction_items")
sessions = spark.read.parquet(f"{C}/sessions")
products = spark.read.parquet(f"{C}/products").select("product_id", "name", "category_name")


def pair_rules(baskets, basket_col, item_col, min_count):
    """
    baskets: DataFrame(basket_col, item_col) with one row per (basket, distinct item).
    Returns directional rules X -> Y with co_count, support, confidence, lift.
    """
    baskets = baskets.select(basket_col, item_col).distinct().cache()
    n = baskets.select(basket_col).distinct().count()
    item_counts = baskets.groupBy(item_col).agg(F.count("*").alias("n_item"))
    a = baskets.select(basket_col, F.col(item_col).alias("x"))
    b = baskets.select(basket_col, F.col(item_col).alias("y"))
    # self-join on the basket id; x < y keeps each unordered pair once (no (A,A), no duplicates)
    pairs = (a.join(b, basket_col).filter(F.col("x") < F.col("y"))
              .groupBy("x", "y").agg(F.count("*").alias("co_count"))
              .filter(F.col("co_count") >= min_count))
    # make the rules directional: X -> Y and Y -> X
    rules = pairs.union(pairs.select(F.col("y").alias("x"), F.col("x").alias("y"), "co_count"))
    rules = (rules
             .join(item_counts.select(F.col(item_col).alias("x"), F.col("n_item").alias("n_x")), "x")
             .join(item_counts.select(F.col(item_col).alias("y"), F.col("n_item").alias("n_y")), "y")
             .withColumn("support", F.col("co_count") / n)
             .withColumn("confidence", F.col("co_count") / F.col("n_x"))
             .withColumn("lift", F.col("confidence") / (F.col("n_y") / n)))
    return rules, n


def save(df, name, n=None):
    pdf = (df.limit(n) if n else df).toPandas()
    pdf.to_csv(f"{R}/{name}.csv", index=False)
    print(f"  -> saved {R}/{name}.csv ({len(pdf)} rows)")


# ---------------------------------------------------------------- A. co-purchase (user level)
# A "basket" = everything one user ever bought. Per-order baskets are too small here
# (most orders have 1-3 random items), so the user level gives far more co-occurrences.
print("\nA. Users who bought X also bought Y")
buy_rules, n_buyers = pair_rules(items, "user_id", "product_id", args.min_copurchase)
buy_rules = buy_rules.cache()
print(f"  {n_buyers:,} buyers, {buy_rules.count() // 2:,} product pairs bought by >= "
      f"{args.min_copurchase} common users")
buy_rules.orderBy(F.desc("co_count"), F.desc("lift")).select(
    "x", "y", "co_count", F.round("confidence", 3).alias("confidence"),
    F.round("lift", 1).alias("lift")).show(8, truncate=False)

# ---------------------------------------------------------------- B. co-view (session level)
print("B. Products frequently viewed together (same session)")
views = sessions.select("session_id", F.explode("viewed_products").alias("product_id"))
view_rules, n_sessions = pair_rules(views, "session_id", "product_id", args.min_coview)
view_rules = view_rules.cache()
print(f"  {n_sessions:,} sessions with views, {view_rules.count() // 2:,} pairs co-viewed in >= "
      f"{args.min_coview} sessions")
view_rules.orderBy(F.desc("co_count")).select(
    "x", "y", "co_count", F.round("confidence", 3).alias("confidence"),
    F.round("lift", 1).alias("lift")).show(8, truncate=False)

# ---------------------------------------------------------------- C. combined top-5 per product
print("C. Top-5 recommendations per product (co-purchase first, then co-view)")
combined = (buy_rules.select("x", "y", F.col("co_count").alias("co_purchase"),
                             F.col("lift").alias("purchase_lift"))
            .join(view_rules.select("x", "y", F.col("co_count").alias("co_view"),
                                    F.col("lift").alias("view_lift")), ["x", "y"], "full_outer")
            .fillna(0, ["co_purchase", "co_view", "purchase_lift", "view_lift"])
            # simple blended score: a shared purchase is worth more than a shared view
            .withColumn("score", F.col("co_purchase") * 3 + F.col("co_view")))
w = Window.partitionBy("x").orderBy(F.desc("score"), F.desc("purchase_lift"), F.desc("view_lift"), "y")
top5 = (combined.withColumn("rank", F.row_number().over(w)).filter("rank <= 5")
        .join(products.select(F.col("product_id").alias("y"), F.col("name").alias("recommended_name"),
                              F.col("category_name").alias("recommended_category")), "y")
        .select(F.col("x").alias("product_id"), "rank", F.col("y").alias("recommended_id"),
                "recommended_name", "recommended_category", "co_purchase", "co_view",
                F.round("purchase_lift", 1).alias("purchase_lift"), "score"))
top5.write.mode("overwrite").parquet(f"{C}/recommendations")
covered = top5.select("product_id").distinct().count()
print(f"  {covered:,} products have at least one recommendation")
print(f"  Recommendations for {args.product}:")
example = top5.filter(F.col("product_id") == args.product).orderBy("rank")
example.show(truncate=40)
save(example, f"rec_{args.product}")
save(top5.orderBy("product_id", "rank"), "rec_top5_all_products")

# ---------------------------------------------------------------- D. category affinity
print("D. Category affinity (users who buy in category A also buy in category B)")
cat_rules, _ = pair_rules(items, "user_id", "category_name", 1)
cat_rules = cat_rules.select(F.col("x").alias("category_a"), F.col("y").alias("category_b"),
                             "co_count", F.round("confidence", 3).alias("confidence"),
                             F.round("lift", 3).alias("lift"))
cat_rules.orderBy(F.desc("lift")).show(5, truncate=30)
stats = cat_rules.agg(F.min("lift"), F.avg("lift"), F.max("lift")).first()
print(f"  category lift range: {stats[0]:.3f} - {stats[2]:.3f} (mean {stats[1]:.3f})")
save(cat_rules.orderBy(F.desc("lift")), "rec_category_affinity")

# ---------------------------------------------------------------- E. random-shuffle baseline
# Keep every basket's size but give it random products, then count pairs again.
# If the real data had genuine affinities, it would have many more repeated pairs than this.
print("E. Baseline: same basket sizes, products shuffled at random")
user_products = items.select("user_id", "product_id").distinct()
rows = user_products.withColumn("i", F.row_number().over(Window.orderBy("user_id", "product_id")))
shuffled_products = (user_products.select("product_id")
                     .withColumn("i", F.row_number().over(Window.orderBy(F.rand(seed=7)))))
shuffled = rows.select("user_id", "i").join(shuffled_products, "i")
shuf_rules, _ = pair_rules(shuffled, "user_id", "product_id", args.min_copurchase)
real_pairs, shuf_pairs = buy_rules.count() // 2, shuf_rules.count() // 2
print(f"  pairs bought by >= {args.min_copurchase} common users: real = {real_pairs:,}, "
      f"random shuffle = {shuf_pairs:,}  (ratio {real_pairs / max(shuf_pairs, 1):.2f})")
spark.createDataFrame([("real", real_pairs), ("random_shuffle", shuf_pairs)],
                      ["dataset", "pairs"]).toPandas().to_csv(f"{R}/rec_baseline.csv", index=False)

print(f"\nDone in {time.time() - t0:,.0f}s")
spark.stop()
