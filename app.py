import os
import threading
import requests
import base64
from flask import Flask, jsonify
from datetime import datetime, timezone, timedelta

app = Flask(__name__)

# ===============================
# ENV VARIABLES
# ===============================
AIRTABLE_TOKEN = os.getenv("AIRTABLE_TOKEN")
BASE_ID = os.getenv("BASE_ID")

ORDERS_TABLE = os.getenv("ORDERS_TABLE")
CUSTOMERS_TABLE = os.getenv("CUSTOMERS_TABLE")
FRENCH_INVENTORIES_TABLE = os.getenv("FRENCH_INVENTORIES_TABLE")

SELLER_ID = os.getenv("SELLER_ID")
API_KEY = os.getenv("API_KEY")
API_SECRET = os.getenv("API_SECRET")

# Stored as epoch milliseconds (string)
LAST_SYNC_DATE = os.getenv("LAST_SYNC_DATE")

# ===============================
# HEADERS
# ===============================
AIRTABLE_HEADERS = {
    "Authorization": f"Bearer {AIRTABLE_TOKEN}",
    "Content-Type": "application/json"
}

auth_string = f"{API_KEY}:{API_SECRET}"
encoded_auth = base64.b64encode(auth_string.encode()).decode()

TRENDYOL_HEADERS = {
    "Authorization": f"Basic {encoded_auth}",
    "Accept": "application/json",
    "User-Agent": "TrendyolAirtableSync/1.0",
    "storeFrontCode": "AE"
}

# ===============================
# LOG
# ===============================
def log(msg):
    print(f"[{datetime.utcnow().isoformat()}] {msg}", flush=True)

# ===============================
# AIRTABLE HELPERS
# ===============================
def airtable_get(table, formula):
    url = f"https://api.airtable.com/v0/{BASE_ID}/{table}"
    params = {"filterByFormula": formula}
    log(f"📡 Airtable GET → {table} | {formula}")
    return requests.get(url, headers=AIRTABLE_HEADERS, params=params).json()

def airtable_create(table, fields):
    url = f"https://api.airtable.com/v0/{BASE_ID}/{table}"
    log(f"📝 Airtable CREATE → {table}")
    res = requests.post(url, headers=AIRTABLE_HEADERS, json={"fields": fields})

    if res.status_code >= 300:
        log(f"❌ Airtable CREATE failed → {res.text}")
        return None

    return res.json()

# ===============================
# CUSTOMER
# ===============================
def get_or_create_customer(customer):
    trendyol_id = str(customer["id"])
    log(f"👤 Checking customer {trendyol_id}")

    res = airtable_get(CUSTOMERS_TABLE, f"{{Trendyol Id}}='{trendyol_id}'")
    records = res.get("records", [])

    if records:
        log("✅ Customer exists")
        return records[0]["id"]

    log("➕ Creating new customer")
    created = airtable_create(CUSTOMERS_TABLE, {
        "Name": f'{customer.get("firstName","")} {customer.get("lastName","")}',
        "Trendyol Id": trendyol_id,
        "Contact Number": customer.get("phone"),
        "Address": customer.get("address"),
        "Acquired sales channel": "Trendyol"
    })

    return created["id"] if created else None

# ===============================
# INVENTORY
# ===============================
def get_inventory_record(sku):
    log(f"📦 Searching SKU {sku}")
    res = airtable_get(FRENCH_INVENTORIES_TABLE, f"{{SKU}}='{sku}'")
    records = res.get("records", [])

    if records:
        return records[0]["id"]

    log("⚠️ SKU not found in inventory")
    return None

# ===============================
# SYNC LOGIC (CORRECT)
# ===============================
def sync_orders_background():
    global LAST_SYNC_DATE

    log("🚀 Trendyol → Airtable sync started")

    # If no last sync → last 24 hours
    if not LAST_SYNC_DATE:
        start_dt = datetime.now(timezone.utc) - timedelta(days=1)
        LAST_SYNC_DATE = str(int(start_dt.timestamp() * 1000))
        log(f"🕒 LAST_SYNC_DATE not set, defaulting to {LAST_SYNC_DATE}")

    url = f"https://apigw.trendyol.com/integration/order/sellers/{SELLER_ID}/orders"
    params = {
        "page": 0,
        "size": 50,
        "startDate": LAST_SYNC_DATE
    }

    log(f"🔐 Calling Trendyol API | startDate={LAST_SYNC_DATE}")
    res = requests.get(url, headers=TRENDYOL_HEADERS, params=params)

    if res.status_code != 200:
        log(f"❌ Trendyol API error {res.status_code} → {res.text}")
        return

    orders = res.json().get("content", [])
    log(f"📦 Orders fetched: {len(orders)}")

    newest_order_time = None

    for order in orders:
        order_id = str(order["id"])
        log(f"🔍 Processing order {order_id}")

        # Skip if already exists
        exists = airtable_get(ORDERS_TABLE, f"{{Order ID}}='{order_id}'")
        if exists.get("records"):
            log("⏭️ Order already exists, skipping")
            continue

        customer_id = get_or_create_customer({
            "id": order["customerId"],
            "firstName": order["shipmentAddress"]["firstName"],
            "lastName": order["shipmentAddress"]["lastName"],
            "phone": order["shipmentAddress"].get("phone"),
            "address": order["shipmentAddress"].get("address1")
        })

        sku_links = []
        for line in order["lines"]:
            sku = line.get("merchantSku")
            if not sku:
                continue

            inv_id = get_inventory_record(sku)
            if inv_id:
                sku_links.append(inv_id)

        airtable_create(ORDERS_TABLE, {
            "Order ID": order_id,
            "Order Number": str(order.get("orderNumber")),
            "Customer": [customer_id] if customer_id else [],
            "Item SKU": sku_links,
            "Order Date": datetime.fromtimestamp(
                order["orderDate"] / 1000
            ).strftime("%Y-%m-%d"),
            "Sales Channel": "Trendyol",
            "Payment Status": "Pending",
            "Shipping Status": "New"
        })

        log(f"✅ Order {order_id} created with {len(sku_links)} items")
        newest_order_time = max(newest_order_time or 0, order["orderDate"])

    # IMPORTANT NOTE
    if newest_order_time:
        log(
            f"🕒 Update LAST_SYNC_DATE in Render ENV to → {newest_order_time}"
        )

    log("🏁 Sync finished")

# ===============================
# ENDPOINT
# ===============================
@app.route("/", methods=["GET"])
def trigger_sync():
    log("📥 Sync request received")
    threading.Thread(target=sync_orders_background).start()
    return jsonify({"status": "sync started"}), 200

# ===============================
# RUN
# ===============================
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)
