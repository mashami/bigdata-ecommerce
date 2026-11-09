"""
dataset_generator.py  -  synthetic e-commerce dataset for the AUCA Big Data final project.

Based on the course-provided generator (originally a Colab notebook). Changes from the original:
  1. Converted from notebook JSON into a runnable Python script.
  2. Scale is configurable from the command line (default: 300k sessions instead of 2M),
     because the full dataset needs ~6-8 GB of disk and a lot of RAM on a laptop.
  3. Every product now gets a `subcategory_id` (the project PDF's sample data has one,
     but the original script never assigned it).
  4. Sessions are written to disk in chunks while they are generated, instead of being
     held in memory all at once, so RAM usage stays low.
  5. Category lookups use a dictionary instead of a linear search (faster).
  6. All output goes to a `data/` folder.

The generation logic itself (page-flow probabilities, cart/conversion rules, inventory,
discounts) is unchanged, so the data has the same shape the assignment describes.

Usage:
    python dataset_generator.py                 # default: 300,000 sessions
    python dataset_generator.py --sessions 20000 --transactions 5000   # quick test
"""
import argparse
import datetime
import json
import os
import random
import threading
import time
import uuid

import numpy as np
from faker import Faker

# --- Configuration (command-line overridable) ---
parser = argparse.ArgumentParser(description="Generate the synthetic e-commerce dataset.")
parser.add_argument("--users", type=int, default=10_000)
parser.add_argument("--products", type=int, default=5_000)
parser.add_argument("--categories", type=int, default=25)
parser.add_argument("--sessions", type=int, default=300_000)
parser.add_argument("--transactions", type=int, default=75_000)
parser.add_argument("--days", type=int, default=90, help="Timespan of activity in days")
parser.add_argument("--chunk-size", type=int, default=100_000, help="Sessions per output file")
parser.add_argument("--out", default="data", help="Output folder")
args = parser.parse_args()

NUM_USERS = args.users
NUM_PRODUCTS = args.products
NUM_CATEGORIES = args.categories
NUM_SESSIONS = args.sessions
NUM_TRANSACTIONS = args.transactions
TIMESPAN_DAYS = args.days
CHUNK_SIZE = args.chunk_size
OUT_DIR = args.out
MAX_ITERATIONS = (NUM_SESSIONS + NUM_TRANSACTIONS) * 2  # Fail-safe

os.makedirs(OUT_DIR, exist_ok=True)

# --- Initialization (fixed seeds = reproducible dataset) ---
fake = Faker()
np.random.seed(42)
random.seed(42)
Faker.seed(42)

start_clock = time.time()
print(f"Initializing dataset generation -> {OUT_DIR}/")
print(f"Targets: {NUM_SESSIONS:,} sessions, {NUM_TRANSACTIONS:,} transactions, "
      f"{NUM_USERS:,} users, {NUM_PRODUCTS:,} products")


# --- ID Generators ---
def generate_session_id():
    return f"sess_{uuid.uuid4().hex[:10]}"


def generate_transaction_id():
    return f"txn_{uuid.uuid4().hex[:12]}"


# --- Inventory Management ---
class InventoryManager:
    def __init__(self, products):
        self.products = {p["product_id"]: p for p in products}
        self.lock = threading.RLock()  # For thread safety

    def update_stock(self, product_id, quantity):
        with self.lock:
            if product_id not in self.products:
                return False
            if self.products[product_id]["current_stock"] >= quantity:
                self.products[product_id]["current_stock"] -= quantity
                return True
            return False

    def get_product(self, product_id):
        with self.lock:
            return self.products.get(product_id)


# --- Helper Functions ---
def determine_page_type(position, previous_pages):
    """Determine page type based on position in the user journey and the previous page."""
    if position == 0:
        # First page is usually homepage, search or category
        return random.choice(["home", "search", "category_listing"])

    if not previous_pages:
        return "home"

    prev_page = previous_pages[-1]["page_type"]

    # Realistic page-flow transitions (a simple Markov chain)
    if prev_page == "home":
        return random.choices(["category_listing", "search", "product_detail"],
                              weights=[0.5, 0.3, 0.2])[0]
    elif prev_page == "category_listing":
        return random.choices(["product_detail", "category_listing", "search", "home"],
                              weights=[0.7, 0.1, 0.1, 0.1])[0]
    elif prev_page == "search":
        return random.choices(["product_detail", "search", "category_listing", "home"],
                              weights=[0.6, 0.2, 0.1, 0.1])[0]
    elif prev_page == "product_detail":
        return random.choices(["product_detail", "cart", "category_listing", "search", "home"],
                              weights=[0.3, 0.3, 0.2, 0.1, 0.1])[0]
    elif prev_page == "cart":
        return random.choices(["checkout", "product_detail", "category_listing", "home"],
                              weights=[0.6, 0.2, 0.1, 0.1])[0]
    elif prev_page == "checkout":
        return random.choices(["confirmation", "cart", "home"],
                              weights=[0.8, 0.1, 0.1])[0]
    elif prev_page == "confirmation":
        return random.choices(["home", "product_detail", "category_listing"],
                              weights=[0.6, 0.2, 0.2])[0]
    else:
        return "home"


def get_page_content(page_type, products_list, categories_list, category_index):
    """Return (product, category) appropriate for the page type."""
    if page_type == "product_detail":
        # Try to find an active product with stock
        for _ in range(10):  # Limit attempts to avoid an infinite loop
            product = random.choice(products_list)
            if product["is_active"] and product["current_stock"] > 0:
                return product, category_index.get(product["category_id"])
        # Fall back to any product
        product = random.choice(products_list)
        return product, category_index.get(product["category_id"])

    elif page_type == "category_listing":
        return None, random.choice(categories_list)

    return None, None


def write_json(path, obj):
    with open(path, "w") as f:
        json.dump(obj, f)


# --- Category Generation ---
categories = []
for cat_id in range(NUM_CATEGORIES):
    category = {
        "category_id": f"cat_{cat_id:03d}",
        "name": fake.company(),
        "subcategories": [],
    }
    for sub_id in range(random.randint(3, 5)):
        category["subcategories"].append({
            "subcategory_id": f"sub_{cat_id:03d}_{sub_id:02d}",
            "name": fake.bs(),
            "profit_margin": round(random.uniform(0.1, 0.4), 2),
        })
    categories.append(category)

category_index = {c["category_id"]: c for c in categories}
print(f"Generated {len(categories)} categories")

# --- Product Generation ---
products = []
product_creation_start = datetime.datetime.now() - datetime.timedelta(days=TIMESPAN_DAYS * 2)

for prod_id in range(NUM_PRODUCTS):
    category = random.choice(categories)
    subcategory = random.choice(category["subcategories"])  # FIX: assign a subcategory

    # Price history with 1-3 price points
    base_price = round(random.uniform(5, 500), 2)
    price_history = []

    initial_date = fake.date_time_between(
        start_date=product_creation_start,
        end_date=product_creation_start + datetime.timedelta(days=TIMESPAN_DAYS // 3),
    )
    price_history.append({"price": base_price, "date": initial_date.isoformat()})

    # 0-2 more price changes (+/- 20%)
    for _ in range(random.randint(0, 2)):
        price_change_date = fake.date_time_between(start_date=initial_date, end_date="now")
        new_price = round(base_price * random.uniform(0.8, 1.2), 2)
        price_history.append({"price": new_price, "date": price_change_date.isoformat()})
        initial_date = price_change_date

    price_history.sort(key=lambda x: x["date"])
    current_price = price_history[-1]["price"]

    products.append({
        "product_id": f"prod_{prod_id:05d}",
        "name": fake.catch_phrase().title(),
        "category_id": category["category_id"],
        "subcategory_id": subcategory["subcategory_id"],
        "base_price": current_price,
        "current_stock": random.randint(10, 1000),
        "is_active": random.choices([True, False], weights=[0.95, 0.05])[0],
        "price_history": price_history,
        "creation_date": price_history[0]["date"],
    })

print(f"Generated {len(products)} products")

# --- User Generation ---
users = []
for user_id in range(NUM_USERS):
    reg_date = fake.date_time_between(start_date=f"-{TIMESPAN_DAYS * 3}d",
                                      end_date=f"-{TIMESPAN_DAYS}d")
    users.append({
        "user_id": f"user_{user_id:06d}",
        "geo_data": {
            "city": fake.city(),
            "state": fake.state_abbr(),
            "country": fake.country_code(),
        },
        "registration_date": reg_date.isoformat(),
        "last_active": fake.date_time_between(start_date=reg_date, end_date="now").isoformat(),
    })

print(f"Generated {len(users)} users")


def build_transaction(user, items, session_id, timestamp, payment_methods, status):
    """Shared logic: subtotal, optional discount (20% chance), total."""
    subtotal = round(sum(item["subtotal"] for item in items), 2)
    discount = 0
    if random.random() < 0.2:
        discount = round(subtotal * random.choice([0.05, 0.1, 0.15, 0.2]), 2)
    return {
        "transaction_id": generate_transaction_id(),
        "session_id": session_id,
        "user_id": user["user_id"],
        "timestamp": timestamp,
        "items": items,
        "subtotal": subtotal,
        "discount": discount,
        "total": round(subtotal - discount, 2),
        "payment_method": random.choice(payment_methods),
        "status": status,
    }


# --- Session & Transaction Generation ---
inventory = InventoryManager(products)
session_buffer = []   # flushed to disk every CHUNK_SIZE sessions
session_files = 0
transactions = []
transaction_counter = 0
session_counter = 0
iteration = 0


def flush_sessions():
    global session_buffer, session_files
    if session_buffer:
        write_json(os.path.join(OUT_DIR, f"sessions_{session_files}.json"), session_buffer)
        session_files += 1
        session_buffer = []


print("Generating sessions and transactions...")
while ((session_counter < NUM_SESSIONS or transaction_counter < NUM_TRANSACTIONS)
       and iteration < MAX_ITERATIONS):
    iteration += 1

    # ---- Session generation ----
    if session_counter < NUM_SESSIONS:
        user = random.choice(users)
        session_id = generate_session_id()
        session_start = fake.date_time_between(start_date=f"-{TIMESPAN_DAYS}d", end_date="now")
        session_duration = random.randint(30, 3600)  # 30 sec to 1 hour

        page_views = []
        viewed_products = set()
        cart_contents = {}

        # Page-view timeline: random cut points inside the session
        time_slots = sorted([0] + [random.randint(1, session_duration - 1)
                                   for _ in range(random.randint(3, 15))] + [session_duration])

        for i in range(len(time_slots) - 1):
            view_duration = time_slots[i + 1] - time_slots[i]
            page_type = determine_page_type(i, page_views)
            product, category = get_page_content(page_type, products, categories, category_index)

            # Product views and add-to-cart (30% chance on a product page)
            if page_type == "product_detail" and product:
                product_id = product["product_id"]
                viewed_products.add(product_id)

                if random.random() < 0.3:
                    if product_id not in cart_contents:
                        cart_contents[product_id] = {"quantity": 0, "price": product["base_price"]}
                    max_possible = min(3, inventory.get_product(product_id)["current_stock"]
                                       - cart_contents[product_id]["quantity"])
                    if max_possible > 0:
                        cart_contents[product_id]["quantity"] += random.randint(1, max_possible)

            page_views.append({
                "timestamp": (session_start + datetime.timedelta(seconds=time_slots[i])).isoformat(),
                "page_type": page_type,
                "product_id": product["product_id"] if product else None,
                "category_id": category["category_id"] if category else None,
                "view_duration": view_duration,
            })

        # Conversion: needs a cart AND a checkout/confirmation page; then 70% chance
        converted = False
        if cart_contents and any(p["page_type"] in ("checkout", "confirmation") for p in page_views):
            converted = random.random() < 0.7

        session_geo = user["geo_data"].copy()
        session_geo["ip_address"] = fake.ipv4()

        session_buffer.append({
            "session_id": session_id,
            "user_id": user["user_id"],
            "start_time": session_start.isoformat(),
            "end_time": (session_start + datetime.timedelta(seconds=session_duration)).isoformat(),
            "duration_seconds": session_duration,
            "geo_data": session_geo,
            "device_profile": {
                "type": random.choice(["mobile", "desktop", "tablet"]),
                "os": random.choice(["iOS", "Android", "Windows", "macOS"]),
                "browser": random.choice(["Chrome", "Safari", "Firefox", "Edge"]),
            },
            "viewed_products": list(viewed_products),
            "page_views": page_views,
            "cart_contents": {k: v for k, v in cart_contents.items() if v["quantity"] > 0},
            "conversion_status": "converted" if converted else "abandoned" if cart_contents else "browsed",
            "referrer": random.choice(["direct", "email", "social", "search_engine", "affiliate"]),
        })
        session_counter += 1
        if len(session_buffer) >= CHUNK_SIZE:
            flush_sessions()

        # Session-linked transaction (all items must be in stock, else no transaction)
        if converted and transaction_counter < NUM_TRANSACTIONS:
            items, valid = [], True
            for prod_id, details in cart_contents.items():
                qty = details["quantity"]
                if qty > 0:
                    if inventory.update_stock(prod_id, qty):
                        items.append({
                            "product_id": prod_id,
                            "quantity": qty,
                            "unit_price": details["price"],
                            "subtotal": round(qty * details["price"], 2),
                        })
                    else:
                        valid = False
                        break
            if valid and items:
                ts = (session_start + datetime.timedelta(seconds=session_duration)).isoformat()
                transactions.append(build_transaction(
                    user, items, session_id, ts,
                    ["credit_card", "paypal", "apple_pay", "crypto"], "completed"))
                transaction_counter += 1

    # ---- Extra transactions not linked to a session (e.g. app / phone orders) ----
    if transaction_counter < NUM_TRANSACTIONS and random.random() < 0.2:
        user = random.choice(users)
        items = []
        for product in random.sample(products, k=min(3, len(products))):
            if product["is_active"]:
                qty = random.randint(1, 3)
                if inventory.update_stock(product["product_id"], qty):
                    items.append({
                        "product_id": product["product_id"],
                        "quantity": qty,
                        "unit_price": product["base_price"],
                        "subtotal": round(qty * product["base_price"], 2),
                    })
        if items:
            ts = fake.date_time_between(start_date=f"-{TIMESPAN_DAYS}d", end_date="now").isoformat()
            transactions.append(build_transaction(
                user, items, None, ts,
                ["credit_card", "paypal", "bank_transfer", "gift_card"],
                random.choice(["completed", "processing", "shipped", "delivered"])))
            transaction_counter += 1

    if iteration % 10_000 == 0:
        print(f"Progress: {session_counter:,}/{NUM_SESSIONS:,} sessions, "
              f"{transaction_counter:,}/{NUM_TRANSACTIONS:,} transactions "
              f"(iteration {iteration:,}, {time.time() - start_clock:,.0f}s)")

flush_sessions()

# --- Data Export ---
print("Saving datasets...")
write_json(os.path.join(OUT_DIR, "users.json"), users)
write_json(os.path.join(OUT_DIR, "products.json"), list(inventory.products.values()))
write_json(os.path.join(OUT_DIR, "categories.json"), categories)
write_json(os.path.join(OUT_DIR, "transactions.json"), transactions)

linked = sum(1 for t in transactions if t["session_id"])
print(f"""
Dataset generation complete in {time.time() - start_clock:,.0f}s
- Sessions:     {session_counter:,} (target: {NUM_SESSIONS:,}) in {session_files} file(s)
- Transactions: {len(transactions):,} (target: {NUM_TRANSACTIONS:,}); {linked:,} linked to a session
- Remaining stock units: {sum(p['current_stock'] for p in inventory.products.values()):,}
- Output folder: {os.path.abspath(OUT_DIR)}
""")
