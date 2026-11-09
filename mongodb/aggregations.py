"""
aggregations.py  -  Part 1: analytical aggregation pipelines on MongoDB.

  1. revenue_by_category_month : net revenue and estimated profit per category per month
  2. top_selling_products      : best sellers by units, joined to the catalog with $lookup
  3. customer_segments         : purchase-frequency and spend segmentation with $facet

Each result is printed and saved as CSV in results/ (used later for the charts).

Usage:  python mongodb/aggregations.py
Env:    MONGO_URI (default mongodb://localhost:27018)
"""
import csv
import os

from pymongo import MongoClient

MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27018")
db = MongoClient(MONGO_URI)["ecommerce"]
os.makedirs("results", exist_ok=True)


def save_csv(name, rows):
    if not rows:
        return
    path = os.path.join("results", name)
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)
    print(f"  -> saved {path}")


def show(title, rows, limit=10):
    print(f"\n=== {title} ({len(rows)} rows) ===")
    for r in rows[:limit]:
        print("  ", r)


# ------------------------------------------------------------------------------
# 1. Revenue analytics: net revenue & estimated profit by category and month
#    Business question: which categories drive revenue and profit, and how does it move over time?
#    Techniques: $unwind on the embedded items array, discount allocated to items
#    pro rata, $dateToString for monthly buckets, no join needed thanks to the
#    category fields copied into each line item at load time.
# ------------------------------------------------------------------------------
revenue_pipeline = [
    {"$match": {"status": {"$in": ["completed", "shipped", "delivered"]}}},   # exclude still-processing orders
    {"$addFields": {  # share of the order total actually paid after discount
        "paid_ratio": {"$cond": [{"$gt": ["$subtotal", 0]}, {"$divide": ["$total", "$subtotal"]}, 1]}
    }},
    {"$unwind": "$items"},
    {"$project": {
        "month": {"$dateToString": {"format": "%Y-%m", "date": "$timestamp"}},
        "category_id": "$items.category_id",
        "category_name": "$items.category_name",
        "units": "$items.quantity",
        "net_revenue": {"$multiply": ["$items.subtotal", "$paid_ratio"]},
        "est_profit": {"$multiply": ["$items.subtotal", "$paid_ratio", "$items.profit_margin"]},
        "order_id": "$_id",
    }},
    {"$group": {
        "_id": {"month": "$month", "category_id": "$category_id"},
        "category_name": {"$first": "$category_name"},
        "units": {"$sum": "$units"},
        "net_revenue": {"$sum": "$net_revenue"},
        "est_profit": {"$sum": "$est_profit"},
        "orders": {"$addToSet": "$order_id"},
    }},
    {"$project": {
        "_id": 0,
        "month": "$_id.month",
        "category_id": "$_id.category_id",
        "category_name": 1,
        "units": 1,
        "orders": {"$size": "$orders"},
        "net_revenue": {"$round": ["$net_revenue", 2]},
        "est_profit": {"$round": ["$est_profit", 2]},
    }},
    {"$sort": {"month": 1, "net_revenue": -1}},
]
rows = list(db.transactions.aggregate(revenue_pipeline, allowDiskUse=True))
show("1. Revenue & profit by category and month", rows)
save_csv("mongo_revenue_by_category_month.csv", rows)

# ------------------------------------------------------------------------------
# 2. Product popularity: top 20 products by units sold, enriched from the catalog
#    Business question: which products sell most, and are any best-sellers running low on stock?
#    Techniques: $unwind + $group, then $lookup (a join) to the products collection
#    for name, category and remaining stock.
# ------------------------------------------------------------------------------
top_products_pipeline = [
    {"$unwind": "$items"},
    {"$group": {
        "_id": "$items.product_id",
        "units_sold": {"$sum": "$items.quantity"},
        "gross_revenue": {"$sum": "$items.subtotal"},
        "orders": {"$sum": 1},
        "buyers": {"$addToSet": "$user_id"},
    }},
    {"$sort": {"units_sold": -1}},
    {"$limit": 20},
    {"$lookup": {"from": "products", "localField": "_id", "foreignField": "_id", "as": "product"}},
    {"$unwind": "$product"},
    {"$project": {
        "_id": 0,
        "product_id": "$_id",
        "name": "$product.name",
        "category": "$product.category.name",
        "price": "$product.base_price",
        "units_sold": 1,
        "orders": 1,
        "unique_buyers": {"$size": "$buyers"},
        "gross_revenue": {"$round": ["$gross_revenue", 2]},
        "stock_left": "$product.current_stock",
        "is_active": "$product.is_active",
    }},
]
rows = list(db.transactions.aggregate(top_products_pipeline, allowDiskUse=True))
show("2. Top-selling products", rows)
save_csv("mongo_top_products.csv", rows)

# ------------------------------------------------------------------------------
# 3. User segmentation by purchasing frequency and spend
#    Business question: how are customers distributed by loyalty, and how much is each group worth?
#    (The dataset has no age field, so segmentation is behavioural, not demographic.)
#    Techniques: reads the pre-computed purchase_summary embedded in each user,
#    $facet runs two independent groupings in one pass, $bucket and $switch.
# ------------------------------------------------------------------------------
segments_pipeline = [
    {"$facet": {
        "by_frequency": [
            {"$bucket": {
                "groupBy": "$purchase_summary.order_count",
                "boundaries": [0, 1, 3, 6, 11, 10_000],
                "output": {
                    "users": {"$sum": 1},
                    "total_revenue": {"$sum": "$purchase_summary.total_spent"},
                    "avg_spent": {"$avg": "$purchase_summary.total_spent"},
                    "avg_order_value": {"$avg": "$purchase_summary.avg_order_value"},
                },
            }},
            {"$project": {
                "_id": 0,
                "segment": {"$switch": {"branches": [
                    {"case": {"$eq": ["$_id", 0]}, "then": "0 orders (never bought)"},
                    {"case": {"$eq": ["$_id", 1]}, "then": "1-2 orders (occasional)"},
                    {"case": {"$eq": ["$_id", 3]}, "then": "3-5 orders (regular)"},
                    {"case": {"$eq": ["$_id", 6]}, "then": "6-10 orders (frequent)"},
                ], "default": "11+ orders (loyal)"}},
                "users": 1,
                "total_revenue": {"$round": ["$total_revenue", 2]},
                "avg_spent": {"$round": ["$avg_spent", 2]},
                "avg_order_value": {"$round": ["$avg_order_value", 2]},
            }},
        ],
        "by_spend_tier": [
            {"$match": {"purchase_summary.order_count": {"$gt": 0}}},
            {"$bucketAuto": {"groupBy": "$purchase_summary.total_spent", "buckets": 4,
                             "output": {"users": {"$sum": 1},
                                        "revenue": {"$sum": "$purchase_summary.total_spent"}}}},
            {"$project": {"_id": 0,
                          "spend_from": {"$round": ["$_id.min", 2]},
                          "spend_to": {"$round": ["$_id.max", 2]},
                          "users": 1,
                          "revenue": {"$round": ["$revenue", 2]}}},
        ],
    }},
]
result = list(db.users.aggregate(segments_pipeline))[0]
show("3a. Users by purchase frequency", result["by_frequency"])
save_csv("mongo_segments_frequency.csv", result["by_frequency"])
show("3b. Users by spend quartile", result["by_spend_tier"])
save_csv("mongo_segments_spend.csv", result["by_spend_tier"])

# Show that the indexes are actually used (useful evidence for the report)
plan = db.transactions.find({"user_id": "user_000042"}).sort("timestamp", -1).explain()
stage = plan["queryPlanner"]["winningPlan"]
print("\nQuery plan for one user's orders:", stage.get("stage"), "->",
      stage.get("inputStage", {}).get("stage"), stage.get("inputStage", {}).get("indexName", ""))
