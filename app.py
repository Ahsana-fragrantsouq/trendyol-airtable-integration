import os
import base64
import requests
from flask import Flask, jsonify
from datetime import datetime

app = Flask(__name__)

# ================== CONFIG ==================
AIRTABLE_TOKEN = os.getenv("AIRTABLE_TOKEN")
BASE_ID = os.getenv("BASE_ID")

ORDERS_TABLE = os.getenv("ORDERS_TABLE")
CUSTOMERS_TABLE = os.getenv("CUSTOMERS_TABLE")
INVENTORY_TABLE = os.getenv("FRENCH_INVENTORIES_TABLE")

API_KEY = os.getenv("API_KEY")
API_SECRET = os.getenv("API_SECRET")
SELLER_ID = os.getenv("SELLER_ID")

AIRTABLE_HEADERS = {
    "Authorization": f"Bearer {AIRTABLE_TOKEN}",
    "Content-Type": "application/json"
}

TRENDYOL_AUTH = base64.b64encode(
    f"{API_KEY}:{API_SECRET}".encode()
).decode()

TRENDYOL_HEADERS = {
    "Authorization": f"Basic {TRENDYOL_AUTH}",
    "User-Agent": "TrendyolAirtableSync/1.0",
    "storeFrontCode": "AE",
    "Accept": "application/json"
}

# ================== LOG HELPER ==================
def log(msg):
    print(f"[{datetime.utcnow()}] {msg}", flush=True)

# ================== AIRTABLE HELPERS ==================
def airtable_get(table, formula=None):
    url = f"https://api.airtable.com/v0/{BASE_ID}/{table}"
    params = {"filterByFormula": formula} if formula else {}
    log(f"📡 Airtable GET → {table} | Formula: {formula}")
    res = requests.get(url, headers=AIRTABLE_HEADERS, params=params)
    return res.json()

def airtable_create(table, fields):
    url = f"https://api.airtable.com/v0/{BASE_ID}/{table}"
    log(f"📝 Airtable CREATE → {table}")
    res = requests.post(url, headers=AIRTABLE_HEADERS, json={"fields": fields})
    return res.json()

# ================== BUSINESS LOGIC ==================
def order_exists(order_id):
    formula = f"{{Order ID}}='{order_id}'"
    res = airtable_get(ORDERS_TABLE, formula)
    exists = len(res.get("records", [])) > 0
    log(f"🔍 Order {order_id} exists: {exists}")
    return exists

def get_or_create_customer(customer):
    log(f"👤 Searching customer {customer['id']}")
    formula = f"{{Trendyol Id}}='{customer['id']}'"
    res = airtable_get(CUSTOMERS_TABLE, formula)

    if res["records"]:
        customer_id = res["records"][0]["id"]
        log(f"✅ Customer exists → {customer_id}")
        return customer_id

    log("➕ Creating new customer")
    created = airtable_create(CUSTOMERS_TABLE, {
        "Name": f"{customer.get('firstName','')} {customer.get('lastName','')}",
        "Trendyol Id": str(customer["id"]),
        "Contact Number": customer.get("phone"),
        "Address": customer.get("address")
    })
    return created["id"]

def get_inventory_record(sku):
    log(f"📦 Searching inventory SKU: {sku}")
    formula = f"{{SKU}}='{sku}'"
    res = airtable_get(INVENTORY_TABLE, formula)

    if res["records"]:
        inventory_id = res["records"][0]["id"]
        log(f"✅ SKU linked → {inventory_id}")
        return inventory_id

    log("⚠️ SKU not found in inventory")
    return None

# ================== MAIN ROUTE ==================
@app.route("/", methods=["GET"])
def sync_orders():
    log("🚀 Trendyol → Airtable sync started")

    url = f"https://apigw.trendyol.com/integration/order/sellers/{SELLER_ID}/orders"
    params = {"page": 0, "size": 50}

    log("🔐 Calling Trendyol API...")
    res = requests.get(url, headers=TRENDYOL_HEADERS, params=params)

    if res.status_code != 200:
        log(f"❌ Trendyol API error {res.status_code}")
        return jsonify({"error": res.text}), 400

    data = res.json()
    orders = data.get("content", [])
    log(f"✅ Trendyol response 200 | Orders fetched: {len(orders)}")

    created_count = 0

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
            sku = line["merchantSku"]
            inventory_id = get_inventory_record(sku)

            log("📝 Creating order record in Airtable")
            airtable_create(ORDERS_TABLE, {
                "Order ID": order_id,
                "Order Number": str(order.get("orderNumber")),
                "Customer": [customer_id],
                "Item SKU": [inventory_id] if inventory_id else [],
                "Order Date": order["orderDate"],
                "Sales Channel": "Trendyol"
            })

            created_count += 1
            log("✅ Order created successfully")

    log(f"🏁 Sync finished | Orders created: {created_count}")

    return jsonify({
        "status": "success",
        "orders_created": created_count
    })
