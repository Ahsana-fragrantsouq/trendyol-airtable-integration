import os
import threading
import requests
import base64
from flask import Flask, jsonify
from datetime import datetime

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

# ===============================
# GLOBAL LOCKS
# ===============================
sync_lock = threading.Lock()
customer_lock = threading.Lock()

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
    print(f"[{datetime.utcnow()}] {msg}", flush=True)

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
    res = requests.post(url, headers=AIRTABLE_HEADERS, json={"fields": fields})

    if res.status_code not in (200, 201):
        log(f"❌ Airtable CREATE failed → {res.text}")
        return None

    return res.json()

# ===============================
# CHECK ORDER
# ===============================
def order_exists(order_id):
    res = airtable_get(ORDERS_TABLE, f"{{Order ID}}='{order_id}'")
    return len(res.get("records", [])) > 0

# ===============================
# CUSTOMER (THREAD SAFE)
# ===============================
def get_or_create_customer(customer):
    trendyol_id = str(customer["id"])

    with customer_lock:
        res = airtable_get(CUSTOMERS_TABLE, f"{{Trendyol Id}}='{trendyol_id}'")
        records = res.get("records", [])

        if records:
            log(f"✅ Customer exists → {records[0]['id']}")
            return records[0]["id"]

        log(f"➕ Creating customer {trendyol_id}")
        created = airtable_create(CUSTOMERS_TABLE, {
            "Name": f"{customer.get('firstName','')} {customer.get('lastName','')}",
            "Trendyol Id": trendyol_id,
            "Contact Number": customer.get("phone"),
            "Address": customer.get("address"),
            "Acquired sales channel": "Trendyol"
        })

        return created["id"] if created else None

# ===============================
# INVENTORY
# ===============================
def get_inventory_ids(lines):
    ids = []

    for line in lines:
        sku = line.get("merchantSku")
        if not sku or sku == "merchantSku":
            continue

        res = airtable_get(FRENCH_INVENTORIES_TABLE, f"{{SKU}}='{sku}'")
        records = res.get("records", [])
        if records:
            ids.append(records[0]["id"])

    return ids

# ===============================
# SYNC LOGIC
# ===============================
def sync_orders():
    if not sync_lock.acquire(blocking=False):
        log("⏭️ Sync already running, skipping")
        return

    try:
        log("🚀 Sync started")

        url = f"https://apigw.trendyol.com/integration/order/sellers/{SELLER_ID}/orders"
        res = requests.get(url, headers=TRENDYOL_HEADERS, params={"page": 0, "size": 50})

        if res.status_code != 200:
            log(f"❌ Trendyol error {res.status_code}")
            return

        orders = res.json().get("content", [])
        log(f"📦 Orders fetched: {len(orders)}")

        for order in orders:
            order_id = str(order["id"])

            if order_exists(order_id):
                log(f"⏭️ Order {order_id} exists")
                continue

            customer_id = get_or_create_customer({
                "id": order["customerId"],
                "firstName": order["shipmentAddress"]["firstName"],
                "lastName": order["shipmentAddress"]["lastName"],
                "phone": order["shipmentAddress"].get("phone"),
                "address": order["shipmentAddress"].get("address1")
            })

            inventory_ids = get_inventory_ids(order["lines"])

            record = airtable_create(ORDERS_TABLE, {
                "Order ID": order_id,
                "Order Number": str(order.get("orderNumber")),
                "Customer": [customer_id] if customer_id else [],
                "Item SKU": inventory_ids,
                "Order Date": order["orderDate"],
                "Sales Channel": "Trendyol",
                "Payment Status": "Pending",
                "Shipping Status": "New"
            })

            if record:
                log(f"✅ Order {order_id} created")

        log("🏁 Sync finished")

    finally:
        sync_lock.release()

# ===============================
# ENDPOINT
# ===============================
@app.route("/", methods=["GET"])
def trigger():
    log("📥 Sync request received")
    threading.Thread(target=sync_orders).start()
    return jsonify({"status": "sync started"}), 200

# ===============================
# RUN
# ===============================
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)
