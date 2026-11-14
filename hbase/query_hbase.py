
import argparse
import calendar
import csv
import os
import time
from collections import defaultdict
from datetime import datetime, timedelta

import happybase

parser = argparse.ArgumentParser()
parser.add_argument("--user", default="user_000042")
parser.add_argument("--product", default="prod_02076")
args = parser.parse_args()

conn = happybase.Connection(os.environ.get("HBASE_HOST", "localhost"),
                            port=int(os.environ.get("HBASE_PORT", "9090")))
sessions = conn.table("user_sessions")
metrics = conn.table("product_metrics")
os.makedirs("results", exist_ok=True)


def reverse_ts(dt):
    return f"{10**13 - calendar.timegm(dt.timetuple()) * 1000:013d}"


def dec(row):
    return {k.decode(): v.decode() for k, v in row.items()}


def timed(title):
    """Print a header and return a function that prints the elapsed time."""
    print(f"\n=== {title} ===")
    start = time.perf_counter()
    return lambda n: print(f"   -> {n} row(s) in {(time.perf_counter() - start) * 1000:.1f} ms")


def print_session(key, d):
    print(f"   {d['s:start_time'][:19]}  {d['s:status']:<9}  {d['s:device']:<7} "
          f"pages={d['s:page_count']:>2}  cart_value={d['s:cart_value']:>8}  ref={d['s:referrer']}")


# ---- Q1: most recent sessions of a user ----------------------------------------------
done = timed(f"Q1  10 most recent sessions of {args.user}")
rows = list(sessions.scan(row_prefix=f"{args.user}#".encode(), columns=[b"s"], limit=10))
for k, r in rows:
    print_session(k, dec(r))
done(len(rows))
total = sum(1 for _ in sessions.scan(row_prefix=f"{args.user}#".encode(), columns=[b"s:status"]))
print(f"   ({args.user} has {total} sessions in total)")

# ---- Q2: sessions in a time window -----------------------------------------------------
# newest end of the window -> smallest reverse timestamp -> start row
latest = datetime.fromisoformat(dec(rows[0][1])["s:start_time"]) if rows else datetime.now()
t_to = latest.replace(microsecond=0) + timedelta(seconds=1)
t_from = t_to - timedelta(days=30)
done = timed(f"Q2  {args.user} sessions between {t_from:%Y-%m-%d} and {t_to:%Y-%m-%d}")
rows = list(sessions.scan(row_start=f"{args.user}#{reverse_ts(t_to)}".encode(),
                          row_stop=f"{args.user}#{reverse_ts(t_from)}".encode(),
                          columns=[b"s"]))
for k, r in rows:
    print_session(k, dec(r))
done(len(rows))

# ---- Q3: only converted sessions (filter runs inside HBase, not in Python) -------------
done = timed(f"Q3  converted sessions of {args.user} (server-side filter)")
flt = "SingleColumnValueFilter('s', 'status', =, 'binary:converted', true, true)"
rows = list(sessions.scan(row_prefix=f"{args.user}#".encode(), columns=[b"s"], filter=flt))
for k, r in rows:
    print_session(k, dec(r))
done(len(rows))

# ---- Q4: full clickstream of one session -----------------------------------------------
key = rows[0][0] if rows else next(sessions.scan(row_prefix=f"{args.user}#".encode(), limit=1))[0]
done = timed("Q4  full detail of one session (single get)")
d = dec(sessions.row(key))
print(f"   row key: {key.decode()}")
print(f"   {d['s:session_id']}  {d['s:start_time']}  status={d['s:status']}  "
      f"device={d['s:device']}/{d['s:os']}/{d['s:browser']}  from {d['s:city']}, {d['s:country']}")
for col in sorted(c for c in d if c.startswith("pv:")):
    ts, page, prod, cat, dur = d[col].split("|")
    print(f"     {col}  {ts[11:19]}  {page:<17} {prod or cat or '':<11} {dur:>5}s")
for col in sorted(c for c in d if c.startswith("cart:")):
    qty, price = d[col].split("|")
    print(f"     {col}  qty={qty} @ {price}")
done(1)

# ---- Q5: product metrics for a date range ----------------------------------------------
first = next(metrics.scan(row_prefix=f"{args.product}#".encode(), limit=1), None)
last = next(metrics.scan(row_prefix=f"{args.product}#".encode(), limit=1, reverse=True), None) \
    if first else None
if not first:
    print(f"\nNo metrics found for {args.product}")
    raise SystemExit
start_day = datetime.strptime(first[0].decode().split("#")[1], "%Y%m%d")
month_start = (start_day.replace(day=1) + timedelta(days=32)).replace(day=1)   # first full month
month_end = (month_start + timedelta(days=32)).replace(day=1)
done = timed(f"Q5  daily metrics of {args.product}, {month_start:%B %Y}")
rows = list(metrics.scan(row_start=f"{args.product}#{month_start:%Y%m%d}".encode(),
                         row_stop=f"{args.product}#{month_end:%Y%m%d}".encode()))
print(f"   {'date':<10} {'views':>5} {'cart':>5} {'orders':>6} {'units':>5} {'revenue':>10}")
for k, r in rows:
    m = dec(r)
    print(f"   {k.decode().split('#')[1]:<10} {m['m:views']:>5} {m['m:cart_adds']:>5} "
          f"{m['m:orders']:>6} {m['m:units_sold']:>5} {m['m:revenue']:>10}")
done(len(rows))

# ---- Q6: view -> cart -> purchase conversion per week ----------------------------------
done = timed(f"Q6  weekly view-to-purchase conversion of {args.product} (whole history)")
weeks = defaultdict(lambda: defaultdict(float))
n = 0
for k, r in metrics.scan(row_prefix=f"{args.product}#".encode()):
    n += 1
    m = dec(r)
    d = datetime.strptime(k.decode().split("#")[1], "%Y%m%d")
    wk = (d - timedelta(days=d.weekday())).strftime("%Y-%m-%d")   # Monday of that week
    for f in ("views", "cart_adds", "web_orders", "orders", "revenue"):
        weeks[wk][f] += float(m[f"m:{f}"])
out = []
print(f"   {'week of':<10} {'views':>5} {'cart':>5} {'web_orders':>10} {'view->cart':>10} {'view->buy':>9}")
for wk in sorted(weeks):
    w = weeks[wk]
    v2c = w["cart_adds"] / w["views"] if w["views"] else 0
    v2b = w["web_orders"] / w["views"] if w["views"] else 0
    print(f"   {wk:<10} {w['views']:>5.0f} {w['cart_adds']:>5.0f} {w['web_orders']:>10.0f} "
          f"{v2c:>9.1%} {v2b:>9.1%}")
    out.append({"week": wk, "views": int(w["views"]), "cart_adds": int(w["cart_adds"]),
                "web_orders": int(w["web_orders"]), "all_orders": int(w["orders"]),
                "revenue": round(w["revenue"], 2),
                "view_to_cart": round(v2c, 4), "view_to_purchase": round(v2b, 4)})
done(n)
path = f"results/hbase_{args.product}_weekly_conversion.csv"
with open(path, "w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=list(out[0].keys()))
    w.writeheader()
    w.writerows(out)
print(f"   -> saved {path}")
conn.close()
