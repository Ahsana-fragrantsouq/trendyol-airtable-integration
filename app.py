import os
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

AIRTABLE_HEADERS = {
    "Authorization": f"Bearer {AIRTABLE_TOKEN}",
    "Content-Type": "application/json"
}

TRENDYOL_HEADERS = {
    "User-Agent": "TrendyolAirtableSync/1.0",
    "ApiKey": TRENDYOL_API_KEY,
    "Secret": TRENDYOL_API_SECRET,
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
    try:
        url = f"{AIRTABLE_URL}/{BASE_ID}/{table_id}"
        params = {"filterByFormula": formula}

        print(f"🔍 Airtable search | {formula}")
        r = requests.get(url, headers=AIRTABLE_HEADERS, params=params)
        r.raise_for_status()

        records = r.json().get("records", [])
        print(f"📄 Records found: {len(records)}")
        return records

    except Exception as e:
        print("❌ Airtable search error:", e)
        return []


def airtable_create(table_id, fields):
    try:
        url = f"{AIRTABLE_URL}/{BASE_ID}/{table_id}"
        payload = {"fields": fields}

        print(f"➕ Airtable create | {fields}")
        r = requests.post(url, headers=AIRTABLE_HEADERS, json=payload)
        r.raise_for_status()

        return r.json()

    except Exception as e:
        print("❌ Airtable create error:", e)
        return None

# ---------------- CUSTOMER ----------------
def get_or_create_customer(customer):
    customer_id = customer["customerId"]
    print(f"👤 Processing customer | Trendyol ID: {customer_id}")

    formula = f"{{Trendyol Id}}='{customer_id}'"
    records = airtable_search(CUSTOMERS_TABLE_ID, formula)

    if records:
        print("✅ Customer already exists")
        return records[0]["id"]

    print("🆕 Creating new customer")

    record = airtable_create(
        CUSTOMERS_TABLE_ID,
        {
            "Trendyol Id": customer_id,
            "Name": customer["name"],
            "Address": customer["address"],
            "Acquired sales channel": "Trendyol"
        }
    )

    if record:
        print("✅ Customer created:", record["id"])
        return record["id"]

    return None

# ---------------- ORDER ----------------
def order_exists(order_id):
    print(f"🧾 Checking order existence | {order_id}")
    formula = f"{{Order ID}}='{order_id}'"
    records = airtable_search(ORDERS_TABLE_ID, formula)
    return len(records) > 0


def create_order(order, customer_record_id):
    print(f"🆕 Creating order | {order['orderId']}")

    airtable_create(
        ORDERS_TABLE_ID,
        {
            "Order ID": order["orderId"],
            "Customer": [customer_record_id],
            "Order Date": order["orderDate"],
            "Item SKU": order["sku"],
            "Product Name": order["productName"],
            "Payment Status": order.get("paymentStatus", "Pending"),
            "Shipping Status": order.get("shippingStatus", "New"),
            "Sales Channel": "Trendyol"
        }
    )

    print("✅ Order created")

# ---------------- MANUAL TEST ENDPOINT ----------------
@app.route("/trendyol/order", methods=["POST"])
def receive_trendyol_order():
    try:
        data = request.json
        print("📨 Incoming manual payload:", data)

        if order_exists(data["orderId"]):
            print("⏭️ Duplicate order, skipping")
            return jsonify({"status": "skipped"}), 200

        customer_id = get_or_create_customer(data["customer"])
        create_order(data, customer_id)

        return jsonify({"status": "success"}), 201

    except Exception as e:
        print("❌ Processing error:", e)
        return jsonify({"error": "internal error"}), 500

# ---------------- TRENDYOL SYNC (DATE FILTERED) ----------------
@app.route("/trendyol/sync", methods=["GET"])
def sync_trendyol_orders():
    if not TRENDYOL_API_KEY:
        return jsonify({"error": "Trendyol API not configured"}), 400

    try:
        url = f"{TRENDYOL_BASE_URL}/integration/order/sellers/{TRENDYOL_SELLER_ID}/shipment-packages"

        # 📅 Orders created on 01/02/2026
        start_date = 1769904000000  # 01-02-2026 00:00:00
        end_date = 1769990399000    # 01-02-2026 23:59:59

        params = {
            "page": 0,
            "size": 50,
            "startDate": start_date,
            "endDate": end_date
        }

        print("📡 Fetching Trendyol orders")
        print("📅 Date filter: 01/02/2026")
        print("⏱️ startDate:", start_date)
        print("⏱️ endDate:", end_date)

        r = requests.get(url, headers=TRENDYOL_HEADERS, params=params)
        print("➡️ Trendyol response status:", r.status_code)
        r.raise_for_status()

        packages = r.json().get("content", [])
        print(f"📦 Total packages received: {len(packages)}")

        processed = 0

        for pkg in packages:
            order_id = str(pkg["orderNumber"])
            print("🧾 Processing order:", order_id)

            if order_exists(order_id):
                print("⏭️ Order already exists, skipping")
                continue

            customer = {
                "customerId": str(pkg["customerId"]),
                "name": f"{pkg.get('customerFirstName','')} {pkg.get('customerLastName','')}",
                "address": pkg.get("shipmentAddress", {}).get("fullAddress", "")
            }

            customer_record_id = get_or_create_customer(customer)

            order = {
                "orderId": order_id,
                "orderDate": pkg.get("orderDate"),
                "sku": pkg["lines"][0]["merchantSku"],
                "productName": pkg["lines"][0]["productName"]
            }

            create_order(order, customer_record_id)
            processed += 1
            print("✅ Order synced:", order_id)

        print("🎉 Sync completed | Total new orders:", processed)
        return jsonify({"synced": processed}), 200

    except Exception as e:
        print("❌ Trendyol sync error:", e)
        return jsonify({"error": "sync failed"}), 500

# ---------------- RUN ----------------
if __name__ == "__main__":
    print("🔥 Flask server running")
    app.run(host="0.0.0.0", port=5000)
