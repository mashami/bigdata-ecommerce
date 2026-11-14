
import argparse
import calendar
import glob
import json
import os
import time
from collections import defaultdict
from datetime import datetime

import happybase

parser = argparse.ArgumentParser()
parser.add_argument("--data", default="data")
parser.add_argument("--max-sessions", type=int, default=None, help="Load only the first N sessions")
parser.add_argument("--batch", type=int, default=2000, help="Rows per batch sent to HBase")
args = parser.parse_args()

HOST = os.environ.get("HBASE_HOST", "localhost")
PORT = int(os.environ.get("HBASE_PORT", "9090"))
SESSIONS_TABLE = "user_sessions"
METRICS_TABLE = "product_metrics"


def b(x):
    """HBase stores raw bytes: convert every key/value to UTF-8 bytes."""
    return str(x).encode()


def epoch_ms(iso):
    return calendar.timegm(datetime.fromisoformat(iso).timetuple()) * 1000


def reverse_ts(iso):
    return f"{10**13 - epoch_ms(iso):013d}"


def day(iso):
    return iso[:10].replace("-", "")       # "2026-09-15T10:..." -> "20260915"


t0 = time.time()
conn = happybase.Connection(HOST, port=PORT, timeout=120000)
conn.open()
print(f"Connected to HBase Thrift at {HOST}:{PORT}; existing tables: {conn.tables()}")

# ---------------------------------------------------------------- create tables
family_opts = dict(max_versions=1, bloom_filter_type="ROW")   # keep 1 version; row bloom filter speeds up gets
for name, families in [(SESSIONS_TABLE, ["s", "pv", "cart"]), (METRICS_TABLE, ["m"])]:
    if name.encode() in conn.tables():
        conn.disable_table(name)
        conn.delete_table(name)
    conn.create_table(name, {f: family_opts for f in families})
    print(f"created table {name} with families {families}")

sessions_tbl = conn.table(SESSIONS_TABLE)
metrics_tbl = conn.table(METRICS_TABLE)

# product metrics are aggregated in memory while we stream the sessions, then written at the end
metrics = defaultdict(lambda: defaultdict(float))   # (product_id, yyyymmdd) -> {metric: value}

# ---------------------------------------------------------------- user_sessions
loaded = 0
files = sorted(glob.glob(os.path.join(args.data, "sessions_*.json")),
               key=lambda p: int(p.rsplit("_", 1)[1].split(".")[0]))
with sessions_tbl.batch(batch_size=args.batch) as batch:
    for path in files:
        with open(path) as f:
            sessions = json.load(f)
        for s in sessions:
            if args.max_sessions and loaded >= args.max_sessions:
                break
            row = b(f"{s['user_id']}#{reverse_ts(s['start_time'])}#{s['session_id']}")
            cart = s["cart_contents"]
            data = {
                b"s:session_id": b(s["session_id"]),
                b"s:start_time": b(s["start_time"]),
                b"s:end_time": b(s["end_time"]),
                b"s:duration": b(s["duration_seconds"]),
                b"s:status": b(s["conversion_status"]),
                b"s:referrer": b(s["referrer"]),
                b"s:device": b(s["device_profile"]["type"]),
                b"s:os": b(s["device_profile"]["os"]),
                b"s:browser": b(s["device_profile"]["browser"]),
                b"s:country": b(s["geo_data"]["country"]),
                b"s:city": b(s["geo_data"]["city"]),
                b"s:ip": b(s["geo_data"]["ip_address"]),
                b"s:page_count": b(len(s["page_views"])),
                b"s:product_views": b(len(s["viewed_products"])),
                b"s:cart_items": b(len(cart)),
                b"s:cart_value": b(round(sum(v["quantity"] * v["price"] for v in cart.values()), 2)),
            }
            for i, pv in enumerate(s["page_views"]):
                data[b(f"pv:{i:02d}")] = b("|".join([
                    pv["timestamp"], pv["page_type"], pv["product_id"] or "",
                    pv["category_id"] or "", str(pv["view_duration"])]))
                if pv["page_type"] == "product_detail" and pv["product_id"]:
                    metrics[(pv["product_id"], day(pv["timestamp"]))]["views"] += 1
            for pid, v in cart.items():
                data[b(f"cart:{pid}")] = b(f"{v['quantity']}|{v['price']}")
                m = metrics[(pid, day(s["start_time"]))]
                m["cart_adds"] += 1
                m["cart_units"] += v["quantity"]
            batch.put(row, data)
            loaded += 1
            if loaded % 20000 == 0:
                rate = loaded / (time.time() - t0)
                print(f"  sessions loaded: {loaded:,}  ({rate:,.0f}/s)")
        del sessions
        if args.max_sessions and loaded >= args.max_sessions:
            break
print(f"user_sessions: {loaded:,} rows")

# ---------------------------------------------------------------- product_metrics
with open(os.path.join(args.data, "transactions.json")) as f:
    transactions = json.load(f)
for t in transactions:
    d = day(t["timestamp"])
    for item in t["items"]:
        m = metrics[(item["product_id"], d)]
        m["orders"] += 1
        m["units_sold"] += item["quantity"]
        m["revenue"] += item["subtotal"]
        if t["session_id"]:
            m["web_orders"] += 1        # order placed through a browsing session
del transactions

with metrics_tbl.batch(batch_size=args.batch) as batch:
    for (pid, d), vals in metrics.items():
        batch.put(b(f"{pid}#{d}"), {
            b"m:views": b(int(vals["views"])),
            b"m:cart_adds": b(int(vals["cart_adds"])),
            b"m:cart_units": b(int(vals["cart_units"])),
            b"m:web_orders": b(int(vals["web_orders"])),
            b"m:orders": b(int(vals["orders"])),
            b"m:units_sold": b(int(vals["units_sold"])),
            b"m:revenue": b(round(vals["revenue"], 2)),
        })
print(f"product_metrics: {len(metrics):,} rows (product x day)")

# ---------------------------------------------------------------- sanity check
first = next(sessions_tbl.scan(limit=1))
print("\nFirst row key in user_sessions:", first[0].decode())
print(f"Done in {time.time() - t0:,.0f}s")
conn.close()
