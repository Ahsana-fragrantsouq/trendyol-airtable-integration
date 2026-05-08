import os
import base64
import requests
requests.adapters.DEFAULT_RETRIES = 3
import threading
from datetime import datetime
from flask import Flask, jsonify, request

# ======================================================
# CONFIG
# ======================================================
AIRTABLE_TOKEN = os.getenv("AIRTABLE_TOKEN")
BASE_ID = os.getenv("BASE_ID")
CUSTOMERS_TABLE_ID = os.getenv("CUSTOMERS_TABLE")
ORDERS_TABLE_ID = os.getenv("ORDERS_TABLE")
ORDER_LINE_ITEMS_TABLE_ID = os.getenv("ORDER_LINE_ITEMS_TABLE")
FRENCH_INVENTORIES_TABLE_ID = os.getenv("FRENCH_INVENTORIES_TABLE")

TRENDYOL_SELLER_ID = os.getenv("SELLER_ID")
TRENDYOL_API_KEY = os.getenv("API_KEY")
TRENDYOL_API_SECRET = os.getenv("API_SECRET")

AIRTABLE_URL = "https://api.airtable.com/v0"
TRENDYOL_BASE_URL = "https://apigw.trendyol.com"
REQUEST_TIMEOUT = 30

# ======================================================
# FLASK APP
# ======================================================
app = Flask(__name__)

# ======================================================
# ENV CHECK
# ======================================================
print("🔐 ENV CHECK:")
print("AIRTABLE_TOKEN:", bool(AIRTABLE_TOKEN))
print("BASE_ID:", bool(BASE_ID))
print("CUSTOMERS_TABLE:", bool(CUSTOMERS_TABLE_ID))
print("ORDERS_TABLE:", bool(ORDERS_TABLE_ID))
print("ORDER_LINE_ITEMS_TABLE:", bool(ORDER_LINE_ITEMS_TABLE_ID))
print("FRENCH_INVENTORIES_TABLE:", bool(FRENCH_INVENTORIES_TABLE_ID))
print("SELLER_ID:", bool(TRENDYOL_SELLER_ID))
print("API_KEY:", bool(TRENDYOL_API_KEY))
print("API_SECRET:", bool(TRENDYOL_API_SECRET))
print("--------------------------------------------------")

# ======================================================
# HEADERS
# ======================================================
AIRTABLE_HEADERS = {
    "Authorization": f"Bearer {AIRTABLE_TOKEN}",
    "Content-Type": "application/json"
}

basic_token = base64.b64encode(
    f"{TRENDYOL_API_KEY}:{TRENDYOL_API_SECRET}".encode()
).decode()

TRENDYOL_HEADERS = {
    "Authorization": f"Basic {basic_token}",
    "User-Agent": f"{TRENDYOL_SELLER_ID} - Self Integration",
    "Content-Type": "application/json",
    "storeFrontCode": "AE"
}

# ======================================================
# GLOBAL LOCK
# ======================================================
sync_lock = threading.Lock()

# ======================================================
# AIRTABLE HELPERS
# ======================================================
def airtable_search(table_id, formula):
    print(f"🔍 Airtable search | table={table_id} | formula={formula}")
    r = requests.get(
        f"{AIRTABLE_URL}/{BASE_ID}/{table_id}",
        headers=AIRTABLE_HEADERS,
        params={"filterByFormula": formula},
        timeout=REQUEST_TIMEOUT
    )
    r.raise_for_status()
    records = r.json().get("records", [])
    print(f"🔍 Found {len(records)} records")
    return records

def airtable_create(table_id, fields):
    print(f"📝 Creating Airtable record in table={table_id}")
    print("🧾 Payload:", fields)
    r = requests.post(
        f"{AIRTABLE_URL}/{BASE_ID}/{table_id}",
        headers=AIRTABLE_HEADERS,
        json={"fields": fields},
        timeout=REQUEST_TIMEOUT
    )
    if r.status_code >= 400:
        print("❌ Airtable error:", r.text)
        r.raise_for_status()
    record = r.json()
    print("✅ Airtable record created:", record["id"])
    return record["id"]

def airtable_update(table_id, record_id, fields):
    print(f"✏️ Updating Airtable record {record_id} in table={table_id}")
    print("🧾 Update payload:", fields)
    r = requests.patch(
        f"{AIRTABLE_URL}/{BASE_ID}/{table_id}/{record_id}",
        headers=AIRTABLE_HEADERS,
        json={"fields": fields},
        timeout=REQUEST_TIMEOUT
    )
    if r.status_code >= 400:
        print("❌ Airtable update error:", r.text)
        r.raise_for_status()
    print("✅ Airtable record updated")

# ======================================================
# STATUS MAPPERS
# ======================================================
def map_shipping_status(order):
    s = order.get("status", "").lower()
    if s == "delivered":
        return "Delivered"
    if s in ["shipped", "invoiced", "in_transit"]:
        return "In Transit"
    if s == "cancelled":
        return "Cancelled"
    return "New"

def map_payment_status(order):
    s = order.get("status", "").lower()
    if s in ["paid", "invoiced"]:
        return "Paid"
    if s == "cancelled":
        return "Failed"
    if s == "refunded":
        return "Refund"
    return "Pending"

# ======================================================
# CUSTOMER
# ======================================================
def get_or_create_customer(c):
    print(f"👤 Processing customer {c['id']} | {c['name']}")
    records = airtable_search(
        CUSTOMERS_TABLE_ID,
        f"{{Trendyol Id}}='{c['id']}'"
    )
    if records:
        print("👤 Existing customer found")
        return records[0]["id"]

    print("👤 Creating new customer")
    record_id = airtable_create(
        CUSTOMERS_TABLE_ID,
        {
            "Customer Name": c["name"],
            "Trendyol Id": c["id"]
        }
    )
    print("👤 Customer created:", record_id)
    return record_id

# ======================================================
# NEW: FRENCH INVENTORIES — FIND PRODUCT BY SKU
# ======================================================
def get_french_inventory_record_id(merchant_sku):
    """
    Looks up a record in the French Inventories table by the SKU field.
    The merchant_sku comes from the Trendyol line item's merchantSku field.
    Returns the Airtable record ID if found, or None if not found.
    """
    if not merchant_sku:
        print("⚠️ No merchantSku provided — skipping product lookup")
        return None

    print(f"🔎 Looking up French Inventories | SKU={merchant_sku}")
    records = airtable_search(
        FRENCH_INVENTORIES_TABLE_ID,
        f"{{SKU}}='{merchant_sku}'"
    )
    if records:
        record_id = records[0]["id"]
        print(f"✅ Found French Inventory record: {record_id}")
        return record_id

    print(f"⚠️ No French Inventory record found for SKU={merchant_sku}")
    return None

# ======================================================
# NEW: ORDERS TABLE — GET OR CREATE ORDER
# ======================================================
def get_or_create_order(order_id, order_number, customer_id, order_date, pay, ship):
    """
    Checks if an order already exists in the Orders table by Order ID.
    - If it exists: updates Payment Status and Shipping Status, returns record ID.
    - If not: creates a new record and returns the new record ID.
    """
    print(f"📋 Processing Orders table | Order ID={order_id}")
    records = airtable_search(
        ORDERS_TABLE_ID,
        f"{{Order ID}}='{order_id}'"
    )

    if records:
        existing_id = records[0]["id"]
        print(f"📋 Existing order found: {existing_id} — updating statuses")
        airtable_update(
            ORDERS_TABLE_ID,
            existing_id,
            {
                "Payment Status": pay,
                "Shipping Status": ship
            }
        )
        return existing_id

    print(f"📋 Creating new order in Orders table")
    new_id = airtable_create(
        ORDERS_TABLE_ID,
        {
            "Order ID": order_id,
            "Customer": [customer_id],
            "Order Date": order_date,
            "Sales Channel": "Trendyol",
            "Payment Status": pay,
            "Shipping Status": ship
        }
    )
    print(f"📋 Order created: {new_id}")
    return new_id

# ======================================================
# ORDER LINE ITEMS — DUPLICATE CHECK
# ======================================================
def get_existing_order_line(order_id, product_name):
    """
    Returns the Airtable record ID if the line item already exists, or None.
    Matches by Order ID text field and Trendyol Product Name.
    """
    print(f"🔁 Checking existing line | Order={order_id} | Product={product_name}")
    records = airtable_search(
        ORDER_LINE_ITEMS_TABLE_ID,
        f"AND({{Order ID}}='{order_id}', {{Trendyol Product Name}}='{product_name}')"
    )
    if records:
        record_id = records[0]["id"]
        print(f"🔁 Found existing record: {record_id}")
        return record_id
    print("🔁 No existing record found")
    return None

# ======================================================
# ORDER LINE ITEMS — CREATE
# ======================================================
def create_order_line(
    order_id, order_number, order_record_id,
    customer_id, date, pay, ship,
    product, qty, price,
    french_inventory_record_id
):
    """
    Creates a new Order Line Item record.
    - order_record_id: Airtable record ID from the Orders table (linked field)
    - french_inventory_record_id: Airtable record ID from French Inventories (linked field), can be None
    """
    print(f"🛒 Creating line item | {order_number} | {product}")

    fields = {
        # Text/plain fields
        "Order ID": order_id,
        "Order Number": order_number,
        "Order Date": date,
        "Rate": price,
        "Qty": qty,
        "Trendyol Product Name": product,
        "Sales Channel": "Trendyol",
        "Payment Status": pay,
        "Shipping Status": ship,

        # Linked record fields
        "Customer": [customer_id],
        "Order": [order_record_id],

        # TODO: Set Tax Type once business rule is confirmed
        # "Tax Type": "5%",
    }

    # Only link Product if we found a matching SKU in French Inventories
    if french_inventory_record_id:
        fields["Product"] = [french_inventory_record_id]

    airtable_create(ORDER_LINE_ITEMS_TABLE_ID, fields)

# ======================================================
# ORDER LINE ITEMS — UPDATE STATUSES
# ======================================================
def update_order_line_statuses(record_id, pay, ship):
    """
    Updates only Payment Status and Shipping Status on an existing line item.
    """
    print(f"🔄 Updating statuses for record {record_id} | Pay={pay} | Ship={ship}")
    airtable_update(
        ORDER_LINE_ITEMS_TABLE_ID,
        record_id,
        {
            "Payment Status": pay,
            "Shipping Status": ship
        }
    )

# ======================================================
# MAIN SYNC LOGIC
# ======================================================
def sync_trendyol_orders_job():
    if not sync_lock.acquire(blocking=False):
        print("⏳ Sync already running — skipped")
        return

    print("⏰ Trendyol sync started")

    try:
        r = requests.get(
            f"{TRENDYOL_BASE_URL}/integration/order/sellers/{TRENDYOL_SELLER_ID}/orders",
            headers=TRENDYOL_HEADERS,
            params={"page": 0, "size": 50},
            timeout=REQUEST_TIMEOUT
        )
        r.raise_for_status()

        orders = r.json().get("content", [])
        print(f"📦 Orders fetched: {len(orders)}")

        for o in orders:
            print(f"\n{'='*50}")
            print(f"📦 Processing order {o['orderNumber']}")

            order_id     = str(o["id"])
            order_number = str(o["orderNumber"])

            order_date = datetime.utcfromtimestamp(
                o["orderDate"] / 1000
            ).strftime("%Y-%m-%d")

            pay  = map_payment_status(o)
            ship = map_shipping_status(o)

            # ── STEP 1: Get or create Customer ──────────────────
            customer_id = get_or_create_customer({
                "id":   str(o["customerId"]),
                "name": f"{o.get('customerFirstName', '')} {o.get('customerLastName', '')}".strip()
            })

            # ── STEP 2: Get or create/update Order in Orders table ──
            order_record_id = get_or_create_order(
                order_id, order_number,
                customer_id, order_date,
                pay, ship
            )

            # ── STEP 3: Process each line item ──────────────────
            for line in o.get("lines", []):
                product     = line.get("productName", "")
                qty         = line.get("quantity", 1)
                price       = line.get("price", 0)
                merchant_sku = line.get("merchantSku", "")   # used for French Inventories lookup

                # Find matching product in French Inventories by SKU
                french_inventory_record_id = get_french_inventory_record_id(merchant_sku)

                # Check if this line item already exists
                existing_record_id = get_existing_order_line(order_id, product)

                if existing_record_id:
                    # Record exists → update statuses only
                    update_order_line_statuses(existing_record_id, pay, ship)
                    print(f"🔄 Updated statuses for {order_number} → {product}")
                else:
                    # New record → create it
                    create_order_line(
                        order_id, order_number, order_record_id,
                        customer_id, order_date, pay, ship,
                        product, qty, price,
                        french_inventory_record_id
                    )
                    print(f"✅ Created line item for {order_number} → {product}")

    except Exception as e:
        print("❌ Sync error:", e)

    finally:
        sync_lock.release()
        print("🎉 Trendyol sync finished")

# ======================================================
# ENDPOINTS
# ======================================================
@app.route("/ping", methods=["GET"])
def ping():
    print("🔥 /ping endpoint HIT")

    received_secret = request.headers.get("X-Update-Secret")
    expected_secret = os.getenv("UPDATE_SECRET")

    if received_secret != expected_secret:
        print("⛔ Unauthorized")
        return jsonify({"error": "Unauthorized"}), 401

    print("🚀 Starting background sync")
    thread = threading.Thread(target=sync_trendyol_orders_job)
    thread.daemon = True
    thread.start()

    return jsonify({"status": "Sync started in background"}), 200

@app.route("/wake", methods=["GET"])
def wake():
    print("🌅 Server woken up")
    return "awake", 200

@app.route("/", methods=["GET", "HEAD"])
def health():
    return "OK", 200

# ======================================================
# RUN
# ======================================================
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
