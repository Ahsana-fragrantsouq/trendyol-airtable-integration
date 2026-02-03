import os
import base64
import requests
from flask import Flask, request, jsonify

app = Flask(__name__)

# ---------------- CONFIG ----------------
AIRTABLE_TOKEN = os.getenv("AIRTABLE_TOKEN")
BASE_ID = os.getenv("AIRTABLE_BASE_ID")
CUSTOMERS_TABLE_ID = os.getenv("CUSTOMERS_TABLE_ID")
ORDERS_TABLE_ID = os.getenv("ORDERS_TABLE_ID")

TRENDYOL_SELLER_ID = os.getenv("TRENDYOL_SELLER_ID")
TRENDYOL_API_KEY = os.getenv("TRENDYOL_API_KEY")
TRENDYOL_API_SECRET = os.getenv("TRENDYOL_API_SECRET")

print("🔧 CONFIG LOADED")
print("BASE_ID:", BASE_ID)
print("CUSTOMERS_TABLE_ID:", CUSTOMERS_TABLE_ID)
print("ORDERS_TABLE_ID:", ORDERS_TABLE_ID)
print("TRENDYOL_SELLER_ID:", TRENDYOL_SELLER_ID)

# ---------------- HEADERS ----------------
AIRTABLE_HEADERS = {
    "Authorization": f"Bearer {AIRTABLE_TOKEN}",
    "Content-Type": "application/json"
}

# ✅ BASIC AUTH (ApiKey:Secret → Base64)
basic_token = base64.b64encode(
    f"{TRENDYOL_API_KEY}:{TRENDYOL_API_SECRET}".encode()
).decode()

TRENDYOL_HEADERS = {
    "Authorization": f"Basic {basic_token}",
    "storeFrontCode": "AE",
    "User-Agent": "TrendyolAirtableSync/1.0",
    "Content-Type": "application/json"
}

AIRTABLE_URL = "https://api.airtable.com/v0"
TRENDYOL_BASE_URL = "https://apigw.trendyol.com"

# ---------------- HEALTH CHECK ----------------
@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"}), 200

# ---------------- AIRTABLE HELPERS ----------------
def airtable_search(table_id, formula):
    url = f"{AIRTABLE_URL}/{BASE_ID}/{table_id}"
    params = {"filterByFormula": formula}

    print(f"🔍 Airtable search | {formula}")
    r = requests.get(url, headers=AIRTABLE_HEADERS, params=params)
    r.raise_for_status()
    return r.json().get("records", [])


def airtable_create(table_id, fields):
    url = f"{AIRTABLE_URL}/{BASE_ID}/{table_id}"
    payload = {"fields": fields}

    print(f"➕ Airtable create | {fields}")
    r = requests.post(url, headers=AIRTABLE_HEADERS, json=payload)
    r.raise_for_status()
    return r.json()

# ---------------- CUSTOMER ----------------
def get_or_create_customer(customer):
    formula = f"{{Trendyol Id}}='{customer['customerId']}'"
    records = airtable_search(CUSTOMERS_TABLE_ID, formula)

    if records:
        print("✅ Customer exists")
        return records[0]["id"]

    print("🆕 Creating new customer")

    record = airtable_create(
        CUSTOMERS_TABLE_ID,
        {
            "Trendyol Id": customer["customerId"],
            "Name": customer["name"],
            "Address": customer["address"],
            "Acquired sales channel": "Trendyol"
        }
    )
    return record["id"]

# ---------------- ORDER ----------------
def order_exists(order_id):
    formula = f"{{Order ID}}='{order_id}'"
    records = airtable_search(ORDERS_TABLE_ID, formula)
    return len(records) > 0


def create_order(order, customer_record_id):
    print(f"🆕 Creating order {order['orderId']}")
    airtable_create(
        ORDERS_TABLE_ID,
        {
            "Order ID": order["orderId"],
            "Customer": [customer_record_id],
            "Order Date": order["orderDate"],
            "Item SKU": order["sku"],
            "Product Name": order["productName"],
            "Payment Status": "Pending",
            "Shipping Status": "New",
            "Sales Channel": "Trendyol"
        }
    )

# ---------------- TRENDYOL SYNC (ORDERS API) ----------------
@app.route("/trendyol/sync", methods=["GET"])
def sync_trendyol_orders():
    try:
        print("📡 Fetching Trendyol orders")

        url = f"{TRENDYOL_BASE_URL}/integration/order/sellers/{TRENDYOL_SELLER_ID}/orders"
        params = {"page": 0, "size": 10}

        r = requests.get(url, headers=TRENDYOL_HEADERS, params=params)
        print("➡️ Status:", r.status_code)
        print("📨 Raw:", r.text)
        r.raise_for_status()

        orders = r.json().get("content", [])
        processed = 0

        for o in orders:
            order_id = str(o["orderNumber"])

            if order_exists(order_id):
                print("⏭️ Skipping existing order:", order_id)
                continue

            customer = {
                "customerId": str(o["customerId"]),
                "name": f"{o.get('customerFirstName','')} {o.get('customerLastName','')}",
                "address": o.get("shipmentAddress", {}).get("fullAddress", "")
            }

            customer_record_id = get_or_create_customer(customer)

            order = {
                "orderId": order_id,
                "orderDate": o.get("orderDate"),
                "sku": o["lines"][0]["merchantSku"],
                "productName": o["lines"][0]["productName"]
            }

            create_order(order, customer_record_id)
            processed += 1
            print("✅ Synced order:", order_id)

        return jsonify({"synced": processed}), 200

    except Exception as e:
        print("❌ Trendyol sync error:", e)
        return jsonify({"error": "sync failed"}), 500

# ---------------- RUN ----------------
if __name__ == "__main__":
    print("🔥 Flask server running")
    app.run(host="0.0.0.0", port=5000)
