"""
load_mongo.py  -  Part 1: load the e-commerce dataset into MongoDB.

Collections created (database `ecommerce`):
  categories    - the category -> subcategory hierarchy (small lookup collection)
  products      - catalog; each product embeds its category & subcategory info
  transactions  - one document per purchase; line items embedded as an array,
                  each item carries a copy of its category (extended-reference pattern)
  users         - profile + an embedded `purchase_summary` computed from transactions
                  (computed pattern), built server-side with an aggregation + $merge

Sessions are NOT stored here: they go to HBase (time-series, very high volume).

Usage:
    python mongodb/load_mongo.py                       # all transactions
    python mongodb/load_mongo.py --max-transactions 20000
Env:
    MONGO_URI (default mongodb://localhost:27018)
"""
import argparse
import json
import os
import time
from datetime import datetime

from pymongo import ASCENDING, DESCENDING, MongoClient

parser = argparse.ArgumentParser()
parser.add_argument("--data", default="data", help="Folder with the generated JSON files")
parser.add_argument("--db", default="ecommerce")
parser.add_argument("--max-transactions", type=int, default=None,
                    help="Load only the first N transactions (default: all)")
parser.add_argument("--batch", type=int, default=5000, help="insert_many batch size")
args = parser.parse_args()

MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27018")


def load(name):
    with open(os.path.join(args.data, name)) as f:
        return json.load(f)


def to_dt(s):
    """ISO string -> datetime, so MongoDB stores a real Date (enables date operators/ranges)."""
    return datetime.fromisoformat(s) if s else None


def insert_batches(coll, docs):
    for i in range(0, len(docs), args.batch):
        coll.insert_many(docs[i:i + args.batch], ordered=False)


t0 = time.time()
client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
print("Connected to MongoDB", client.server_info()["version"], "at", MONGO_URI)
db = client[args.db]

# Start clean so the script can be re-run safely
for c in ("categories", "products", "transactions", "users"):
    db[c].drop()

# ---------------------------------------------------------------- categories
categories = load("categories.json")
for c in categories:
    c["_id"] = c.pop("category_id")          # natural key as _id (free unique index)
db.categories.insert_many(categories)

# lookup tables used to denormalise category info into products / line items
cat_by_id = {c["_id"]: c for c in categories}
sub_by_id = {s["subcategory_id"]: s for c in categories for s in c["subcategories"]}
print(f"categories:   {db.categories.count_documents({}):>8,}")

# ---------------------------------------------------------------- products
products = load("products.json")
product_cat = {}
for p in products:
    cat = cat_by_id[p["category_id"]]
    sub = sub_by_id[p["subcategory_id"]]
    p["_id"] = p.pop("product_id")
    # Embed the hierarchy: one read gives a product with its full classification.
    # Category data is tiny and almost never changes, so duplicating it is cheap.
    p["category"] = {"id": cat["_id"], "name": cat["name"]}
    p["subcategory"] = {"id": sub["subcategory_id"], "name": sub["name"],
                        "profit_margin": sub["profit_margin"]}
    del p["category_id"], p["subcategory_id"]
    p["creation_date"] = to_dt(p["creation_date"])
    for ph in p["price_history"]:
        ph["date"] = to_dt(ph["date"])
    product_cat[p["_id"]] = (cat["_id"], cat["name"], sub["subcategory_id"], sub["profit_margin"])
insert_batches(db.products, products)
print(f"products:     {db.products.count_documents({}):>8,}")

# ---------------------------------------------------------------- transactions
transactions = load("transactions.json")
if args.max_transactions:
    transactions = transactions[:args.max_transactions]
for t in transactions:
    t["_id"] = t.pop("transaction_id")
    t["timestamp"] = to_dt(t["timestamp"])
    for item in t["items"]:
        cat_id, cat_name, sub_id, margin = product_cat[item["product_id"]]
        # Extended reference: copy the few fields analytics needs, so revenue/profit
        # by category runs without a $lookup join against products.
        item["category_id"] = cat_id
        item["category_name"] = cat_name
        item["subcategory_id"] = sub_id
        item["profit_margin"] = margin
insert_batches(db.transactions, transactions)
print(f"transactions: {db.transactions.count_documents({}):>8,}")

# ---------------------------------------------------------------- users
users = load("users.json")
for u in users:
    u["_id"] = u.pop("user_id")
    u["registration_date"] = to_dt(u["registration_date"])
    u["last_active"] = to_dt(u["last_active"])
insert_batches(db.users, users)

# Computed pattern: pre-aggregate each user's purchase history INTO the user document,
# so a profile read returns spend/frequency without scanning transactions.
# Runs entirely inside MongoDB ($merge writes the result back into `users`).
db.transactions.aggregate([
    {"$group": {
        "_id": "$user_id",
        "total_spent": {"$sum": "$total"},
        "order_count": {"$sum": 1},
        "first_purchase": {"$min": "$timestamp"},
        "last_purchase": {"$max": "$timestamp"},
    }},
    {"$project": {
        "_id": 1,
        "purchase_summary": {
            "total_spent": {"$round": ["$total_spent", 2]},
            "order_count": "$order_count",
            "avg_order_value": {"$round": [{"$divide": ["$total_spent", "$order_count"]}, 2]},
            "first_purchase": "$first_purchase",
            "last_purchase": "$last_purchase",
        },
    }},
    {"$merge": {"into": "users", "on": "_id", "whenMatched": "merge", "whenNotMatched": "discard"}},
])
# Users who never bought anything get an explicit empty summary
db.users.update_many(
    {"purchase_summary": {"$exists": False}},
    {"$set": {"purchase_summary": {"total_spent": 0, "order_count": 0, "avg_order_value": 0,
                                   "first_purchase": None, "last_purchase": None}}},
)
print(f"users:        {db.users.count_documents({}):>8,}")

# ---------------------------------------------------------------- indexes
# Chosen to match the query patterns used in aggregations.py
db.transactions.create_index([("user_id", ASCENDING), ("timestamp", DESCENDING)])  # a user's orders, newest first
db.transactions.create_index([("timestamp", ASCENDING)])                            # date-range revenue queries
db.transactions.create_index([("items.product_id", ASCENDING)])                     # multikey: orders containing a product
db.transactions.create_index([("session_id", ASCENDING)], sparse=True)              # link to HBase sessions
db.products.create_index([("category.id", ASCENDING)])
db.users.create_index([("purchase_summary.total_spent", DESCENDING)])               # top customers
db.users.create_index([("geo_data.country", ASCENDING)])

print(f"\nDone in {time.time() - t0:,.1f}s. Example user document:")
print(db.users.find_one({"purchase_summary.order_count": {"$gt": 0}}))
