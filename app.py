import os
import time
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
TRENDYOL_BASE_URL = "https://apigw.trendyol.com/ae"

# ---------------- HEALTH CHECK ----------------
@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"}), 200

# ---------------- AIRTABLE HELPERS ----------------
def airtable_search(table_id, formula):
    try:
        url = f"{AIRTABLE_URL}/{BASE_ID}/{table_id}"
        r = requests.get(url, headers=AIRTABLE_HEADERS, params={"filterByFormula": formula})
        r.raise_for_status()
        return r.json().get("records", [])
    except Exception as e:
        print("❌ Airtable search error:", e)
        return []


def airtable_create(table_id, fields):
    try:
        url = f"{AIRTABLE_URL}/{BASE_ID}/{table_id}"
        r = requests.post(url, headers=AIRTABLE_HEADERS, json={"fields": fields})
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print("❌ Airtable create error:", e)
        return None

# ---------------- CUSTOMER ----------------
def get_or_create_customer(customer):
    records = airtable_search(
        CUSTOMERS_TABLE_ID,
        f"{{Trendyol Id}}='{customer['customerId']}'"
    )

    if records:
        return records[0]["id"]

    record = airtable_create(
        CUSTOMERS_TABLE_ID,
        {
            "Trendyol Id": customer["customerId"],
            "Name": customer["name"],
            "Address": customer["address"],
            "Acquired sales channel": "Trendyol"
        }
    )

    return record["id"] if record else None

# ---------------- ORDER ----------------
def order_exists(order_id):
    return len(airtable_search(ORDERS_TABLE_ID, f"{{Order ID}}='{order_id}'")) > 0


def create_order(order, customer_record_id):
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

# ---------------- MANUAL TEST ENDPOINT ----------------
@app.route("/trendyol/order", methods=["POST"])
def receive_trendyol_order():
    try:
        data = request.json

        if order_exists(data["orderId"]):
            return jsonify({"status": "skipped"}), 200

        customer_id = get_or_create_customer(data["customer"])
        create_order(data, customer_id)

        return jsonify({"status": "success"}), 201

    except Exception as e:
        print("❌ Processing error:", e)
        return jsonify({"error": "internal error"}), 500

# ---------------- TRENDYOL SYNC (RECENT SHIPMENTS – STABLE) ----------------
@app.route("/trendyol/sync", methods=["GET"])
def sync_trendyol_orders():
    if not TRENDYOL_API_KEY:
        return jsonify({"error": "Trendyol API not configured"}), 400

    try:
        print("📡 Fetching recent Trendyol shipment-packages")

        url = f"{TRENDYOL_BASE_URL}/integration/order/sellers/{TRENDYOL_SELLER_ID}/shipment-packages"

        processed = 0
        MAX_PAGES = 3
        PAGE_SIZE = 10  # 🔑 AE-safe size

        for page in range(MAX_PAGES):
            params = {"page": page, "size": PAGE_SIZE}
            print(f"📄 Fetching page {page}")

            for attempt in range(2):  # 🔁 retry once
                try:
                    r = requests.get(
                        url,
                        headers=TRENDYOL_HEADERS,
                        params=params,
                        timeout=20
                    )
                    print("➡️ Status:", r.status_code)
                    r.raise_for_status()
                    break
                except Exception as e:
                    print(f"⚠️ Attempt {attempt+1} failed:", e)
                    if attempt == 0:
                        time.sleep(3)
                    else:
                        raise

            packages = r.json().get("content", [])
            print(f"📦 Packages on page {page}: {len(packages)}")

            if not packages:
                break

            for pkg in packages:
                order_id = str(pkg["orderNumber"])

                if order_exists(order_id):
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
                print("✅ Synced order:", order_id)

        print("🎉 Recent shipment sync completed | New orders:", processed)
        return jsonify({"synced": processed}), 200

    except Exception as e:
        print("❌ Trendyol sync error:", e)
        return jsonify({"error": "sync failed"}), 500

# ---------------- RUN ----------------
if __name__ == "__main__":
    print("🔥 Flask server running")
    app.run(host="0.0.0.0", port=5000)
