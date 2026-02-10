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
# HEADERS
# ===============================
AIRTABLE_HEADERS = {
    "Authorization": f"Bearer {AIRTABLE_TOKEN}",
    "Content-Type": "application/json"
}

# 🔐 CORRECT Trendyol Basic Auth (Base64)
auth_string = f"{API_KEY}:{API_SECRET}"
encoded_auth = base64.b64encode(auth_string.encode()).decode()

TRENDYOL_HEADERS = {
    "Authorization": f"Basic {encoded_auth}",
    "Accept": "application/json",
    "User-Agent": "TrendyolAirtableSync/1.0",
    "storeFrontCode": "AE"
}

# ===============================
# UTIL LOG
# ===============================
def log(msg):
    print(f"[{datetime.utcnow()}] {msg}", flush=True)

# ===============================
# AIRTABLE HELPERS
# ===============================
def airtable_get(table, formula):
    url = f"https://api.airtable.com/v0/{BASE_ID}/{table}"
    params = {"filterByFormula": formula}
    log(f"📡 Airtable GET → {table} | Formula: {formula}")
    return requests.get(url, headers=AIRTABLE_HEADERS, params=params).json()

def airtable_create(table, fields):
    url = f"https://api.airtable.com/v0/{BASE_ID}/{table}"
    log(f"📝 Airtable CREATE → {table}")
    res = requests.post(url, headers=AIRTABLE_HEADERS, json={"fields": fields})
    return res.json()

# ===============================
# CHECK ORDER EXISTS
# ===============================
def order_exists(order_id):
    res = airtable_get(ORDERS_TABLE, f"{{Order ID}}='{order_id}'")
    return len(res.get("records", [])) > 0

# ===============================
# CUSTOMER
# ===============================
def get_or_create_customer(customer):
    trendyol_id = str(customer["id"])
    log(f"👤 Searching customer {trendyol_id}")

    res = airtable_get(CUSTOMERS_TABLE, f"{{Trendyol Id}}='{trendyol_id}'")
    records = res.get("records", [])

    if records:
        record_id = records[0]["id"]
        log(f"✅ Customer exists → {record_id}")
        return record_id

    log("➕ Creating new customer")
    created = airtable_create(CUSTOMERS_TABLE, {
        "Name": f'{customer.get("firstName","")} {customer.get("lastName","")}',
        "Trendyol Id": trendyol_id,
        "Contact Number": customer.get("phone"),
        "Address": customer.get("address"),
        "Acquired sales channel": "Trendyol"
    })

    record_id = created["id"]
    log(f"✅ Customer created → {record_id}")
    return record_id

# ===============================
# INVENTORY
# ===============================
def get_inventory_record(sku):
    log(f"📦 Searching inventory SKU: {sku}")
    res = airtable_get(FRENCH_INVENTORIES_TABLE, f"{{SKU}}='{sku}'")
    records = res.get("records", [])
    if records:
        log(f"✅ SKU found → {records[0]['id']}")
        return records[0]["id"]
    log("⚠️ SKU not found in inventory")
    return None

# ===============================
# BACKGROUND SYNC
# ===============================
def sync_orders_background():
    try:
        log("🚀 Trendyol → Airtable sync started")

        url = f"https://apigw.trendyol.com/integration/order/sellers/{SELLER_ID}/orders"
        params = {"page": 0, "size": 50}

        log("🔐 Calling Trendyol API...")
        res = requests.get(url, headers=TRENDYOL_HEADERS, params=params)

        if res.status_code != 200:
            log(f"❌ Trendyol API error {res.status_code} | {res.text}")
            return

        orders = res.json().get("content", [])
        log(f"✅ Trendyol response 200 | Orders fetched: {len(orders)}")

        for order in orders:
            order_id = str(order["id"])
            log(f"🔍 Processing order {order_id}")

            if order_exists(order_id):
                log("⏭️ Order already exists, skipping")
                continue

            customer_id = get_or_create_customer({
                "id": order["customerId"],
                "firstName": order["shipmentAddress"]["firstName"],
                "lastName": order["shipmentAddress"]["lastName"],
                "phone": order["shipmentAddress"].get("phone"),
                "address": order["shipmentAddress"].get("address1")
            })

            for line in order["lines"]:
                sku = line.get("merchantSku")
                if not sku or sku == "merchantSku":
                    log("⚠️ Invalid SKU, skipping line")
                    continue

                inventory_id = get_inventory_record(sku)

                airtable_create(ORDERS_TABLE, {
                    "Order ID": order_id,
                    "Order Number": str(order.get("orderNumber")),
                    "Customer": [customer_id],
                    "Item SKU": [inventory_id] if inventory_id else [],
                    "Order Date": order["orderDate"],
                    "Sales Channel": "Trendyol",
                    "Payment Status": "Pending",
                    "Shipping Status": "New"
                })

                log("✅ Order created successfully")

        log("🏁 Sync finished successfully")

    except Exception as e:
        log(f"🔥 Fatal error: {e}")

# ===============================
# HTTP ENDPOINT
# ===============================
@app.route("/", methods=["GET"])
def trigger_sync():
    log("📥 Sync request received")

    thread = threading.Thread(target=sync_orders_background)
    thread.start()

    log("🧵 Background sync started")
    return jsonify({"status": "sync started"}), 200

# ===============================
# LOCAL RUN
# ===============================
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)
