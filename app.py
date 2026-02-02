import os
import requests
from flask import Flask, request, jsonify


app = Flask(__name__)

# ---------------- CONFIG ----------------
AIRTABLE_TOKEN = os.getenv("AIRTABLE_TOKEN")
BASE_ID = os.getenv("AIRTABLE_BASE_ID")
CUSTOMERS_TABLE_ID = os.getenv("CUSTOMERS_TABLE_ID")
ORDERS_TABLE_ID = os.getenv("ORDERS_TABLE_ID")

print("🔧 CONFIG LOADED")
print("BASE_ID:", BASE_ID)
print("CUSTOMERS_TABLE_ID:", CUSTOMERS_TABLE_ID)
print("ORDERS_TABLE_ID:", ORDERS_TABLE_ID)

HEADERS = {
    "Authorization": f"Bearer {AIRTABLE_TOKEN}",
    "Content-Type": "application/json"
}

AIRTABLE_URL = "https://api.airtable.com/v0"

# ---------------- HELPERS ----------------

def airtable_search(table_id, formula):
    url = f"{AIRTABLE_URL}/{BASE_ID}/{table_id}"
    params = {"filterByFormula": formula}

    print(f"🔍 Searching Airtable | Table: {table_id} | Formula: {formula}")

    r = requests.get(url, headers=HEADERS, params=params)
    print("➡️ Airtable search status:", r.status_code)

    r.raise_for_status()
    records = r.json().get("records", [])
    print(f"📄 Records found: {len(records)}")

    return records


def airtable_create(table_id, fields):
    url = f"{AIRTABLE_URL}/{BASE_ID}/{table_id}"
    payload = {"fields": fields}

    print(f"➕ Creating record in table {table_id}")
    print("📦 Payload:", fields)

    r = requests.post(url, headers=HEADERS, json=payload)
    print("➡️ Airtable create status:", r.status_code)

    r.raise_for_status()
    return r.json()

# ---------------- CUSTOMER ----------------

def get_or_create_customer(customer):
    trendyol_customer_id = customer["customerId"]
    print(f"👤 Processing customer | Trendyol ID: {trendyol_customer_id}")

    formula = f"{{Trendyol Id}}='{trendyol_customer_id}'"
    records = airtable_search(CUSTOMERS_TABLE_ID, formula)

    if records:
        print("✅ Customer already exists")
        return records[0]["id"]

    print("🆕 Customer not found, creating new one")

    new_customer = airtable_create(
        CUSTOMERS_TABLE_ID,
        {
            "Trendyol Id": trendyol_customer_id,
            "Name": customer["name"],
            "Address": customer["address"],
            "Acquired sales channel": "Trendyol"
        }
    )

    print("✅ Customer created | Airtable ID:", new_customer["id"])
    return new_customer["id"]

# ---------------- ORDER ----------------

def order_exists(order_id):
    print(f"🧾 Checking if order exists | Order ID: {order_id}")

    formula = f"{{Order ID}}='{order_id}'"
    records = airtable_search(ORDERS_TABLE_ID, formula)

    exists = len(records) > 0
    print("📦 Order exists:", exists)

    return exists


def create_order(order, customer_record_id):
    print(f"🆕 Creating order | Order ID: {order['orderId']}")

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

    print("✅ Order created successfully")

# ---------------- API ENDPOINT ----------------

@app.route("/trendyol/order", methods=["POST"])
def receive_trendyol_order():
    print("\n🚀 New Trendyol order request received")

    data = request.json
    print("📨 Incoming payload:", data)

    order_id = data["orderId"]

    # 1. check duplicate order
    if order_exists(order_id):
        print("⏭️ Skipping duplicate order")
        return jsonify({"status": "skipped", "reason": "order already exists"}), 200

    # 2. customer
    customer_record_id = get_or_create_customer(data["customer"])

    # 3. create order
    create_order(data, customer_record_id)

    print("🎉 Order processing completed\n")
    return jsonify({"status": "success", "orderId": order_id}), 201

# ---------------- RUN ----------------

if __name__ == "__main__":
    print("🔥 Flask server starting...")
    app.run(debug=True)
