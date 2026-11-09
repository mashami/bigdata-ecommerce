# E-commerce Big Data Analytics: MongoDB + HBase + Apache Spark

AUCA Big Data Analytics final project. A synthetic e-commerce dataset is stored in the database that suits
each kind of data, processed with Spark, combined in an integrated Customer Lifetime Value analysis, and
summarised in charts.

```
dataset_generator.py ──► data/*.json  (10k users · 5k products · 25 categories · 75k orders · 300k sessions)
        │
        ├──► MongoDB  (documents)     products, users (+ purchase summary, + CLV), transactions (+ line items)
        ├──► HBase    (wide-column)   user_sessions (user#reverse-time), product_metrics (product#day)
        └──► Spark    (processing)    cleaning → Parquet → recommendations, Spark SQL
                         │
                         └──► Integration: CLV = MongoDB + HBase joined in Spark ──► written back to MongoDB
                                                                                  ──► charts
```

| Data | Stored in | Why |
|---|---|---|
| Products, users, transactions | **MongoDB** | Nested, read as whole objects (an order with its items, a product with its category); flexible aggregation pipelines for revenue analytics |
| Browsing sessions, page views, daily product metrics | **HBase** | Very high volume, append-only, time-ordered, always read by key range ("this user's latest sessions", "this product in September") |
| Cleaning, joins across sources, recommendations, CLV | **Spark** | Distributed batch processing over all data, SQL with window functions, joins between systems |

## Repository layout

| Path | What it does |
|---|---|
| `dataset_generator.py` | Generates the dataset (course script, made runnable and configurable; see changes in its header) |
| `mongodb/load_mongo.py` | Creates the MongoDB document model, loads data, builds the computed `purchase_summary` and indexes |
| `mongodb/aggregations.py` | 3 aggregation pipelines: revenue/profit by category and month, top products ($lookup), user segments ($facet) |
| `hbase/load_hbase.py` | Creates `user_sessions` and `product_metrics`, loads sessions and per-day product metrics |
| `hbase/query_hbase.py` | 6 key-range queries: recent sessions, time window, server-side filter, clickstream, product metrics, weekly conversion |
| `hbase/hbase_shell_commands.txt` | Equivalent HBase shell schema commands and sample queries |
| `spark/common.py` | Spark session settings and explicit schemas for the raw JSON |
| `spark/spark_clean.py` | Cleaning and normalisation → Parquet, data-quality report |
| `spark/spark_recommendations.py` | "Bought X also bought Y", "viewed together", top-5 per product, category affinity, random baseline |
| `spark/spark_sql.py` | 5 Spark SQL queries over views named after their home system (`mongo_*`, `hbase_*`) |
| `integration/clv_integration.py` | Customer Lifetime Value: MongoDB + HBase → Spark → tiers → written back to MongoDB |
| `visualization/make_charts.py` | The 5 report charts → `visualization/charts/` |
| `results/` | CSV outputs of every analysis (input for the charts) |

## Setup (macOS, Apple Silicon)

```bash
brew install python@3.11 openjdk@17          # Spark needs Java 17
python3.11 -m venv venv && source venv/bin/activate
pip install -r requirements.txt

# MongoDB 7 on port 27018 (27018 avoids clashing with a locally installed MongoDB on 27017)
docker run -d --name mongo -p 27018:27017 mongo:7
# HBase 2.1 standalone with the Thrift server on 9090
docker run -d --name hbase -h hbase -p 2181:2181 -p 9090:9090 -p 16010:16010 \
  --platform linux/amd64 harisekhon/hbase:2.1
```

Make sure `SPARK_HOME` is **not** set to a different Spark installation (PySpark 3.5 must use its own).
If HBase stops answering after the computer sleeps, run `docker restart hbase` and wait ~2 minutes.

## Run order

```bash
python dataset_generator.py                 # ~1 min  → data/
python mongodb/load_mongo.py                # ~10 s
python mongodb/aggregations.py
python hbase/load_hbase.py                  # 5-20 min (Docker emulation); --max-sessions N for a quick test
python hbase/query_hbase.py
python spark/spark_clean.py                 # ~2-4 min → data/clean/
python spark/spark_recommendations.py
python spark/spark_sql.py
python integration/clv_integration.py       # add --sessions-from parquet if HBase is unavailable
python visualization/make_charts.py
```

The generator uses fixed random seeds, so every run reproduces the same dataset and results.
Connection settings can be overridden with `MONGO_URI`, `HBASE_HOST`, `HBASE_PORT`, `DATA_DIR`.

## Key results

- **Revenue** is flat at about $884K per full day ($79.6M over 90 days); categories earn similar revenue
  ($2.7M–3.8M) but profit varies twofold ($0.58M–1.25M) because subcategory margins range from 18% to 36%.
- **Funnel**: 300,000 sessions → 98.8% view a product → 67.9% open the cart → 43.6% reach checkout →
  12.6% place an order. Only 29% of checkouts become orders.
- **Stock-outs**: 296 products sold out; after that the site stopped showing them, so they sold a median of
  36 units versus 70 for products in stock. Best seller #4 (`prod_01565`) has only 30 units left.
- **CLV**: the top 25% of customers hold 42% of predicted 12-month value. Value is driven by recency and order
  frequency (correlation recency~CLV = −0.49), much less by browsing time (+0.15).
  1,103 lower-tier customers with heavy browsing and 3+ abandoned carts are a retargeting opportunity.
- **Recommendations**: the real data has as many repeated product pairs as a random shuffle (ratio 1.00), and
  category lift is 1.02–1.08 — the generator buys products at random, so the recommender works but the data
  contains no genuine affinity signal.

## Data-quality findings (handled in `spark_clean.py`)

- 23,071 sessions are marked `converted` but have no transaction (the generator stops creating orders once its
  target is reached) → relabelled `converted_no_order`.
- 9,856 users have `last_active` earlier than their real last activity → recomputed from sessions and orders.
- 37,230 transactions have no `session_id` → kept as channel `offline`.
- The original generator never assigned `subcategory_id` to products → added.

## Scale note

The original generator targets 2M sessions and 500K transactions (~6–8 GB of JSON). This project uses
300K sessions and 75K transactions (same 4:1 ratio, ~600 MB) so the whole pipeline runs on one laptop.
All code is written for distributed execution and runs unchanged at the full size on a cluster.
