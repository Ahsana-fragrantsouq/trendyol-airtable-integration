import os
import requests
from flask import Flask, jsonify

app = Flask(__name__)

# ---------------- CONFIG ----------------
AIRTABLE_TOKEN = os.getenv("AIRTABLE_TOKEN")
BASE_ID = os.getenv("AIRTABLE_BASE_ID")
CUSTOMERS_TABLE_ID = os.getenv("CUSTOMERS_TABLE_ID")
ORDERS_TABLE_ID = os.getenv("ORDERS_TABLE_ID")

TRENDYOL_SELLER_ID = os.getenv("TRENDYOL_SELLER_ID")
TRENDYOL_API_KEY = os.getenv("TRENDYOL_API_KEY")
TRENDYOL_API_SECRET = os.getenv("TRENDYOL_API_SECRET")

AIRTABLE_HEADERS = {
    "Authorization": f"Bearer {AIRTABLE_TOKEN}",
    "Content-Type": "application/json"
}

TRENDYOL_HEADERS = {
    "User-Agent": "TrendyolAirtableSync/1.0",
    "ApiKey": TRENDYOL_API_KEY,
    "Secret": TRENDYOL_API_SECRET
}

AIRTABLE_URL = "https://api.airtable.com/v0"
TRENDYOL_BASE_URL = "https://apigw.trendyol.com/ae"

# ---------------- AIRTABLE HELPERS ----------------
def airtable_search(table_id, formula):
    r = requests.get(
        f"{AIRTABLE_URL}/{BASE_ID}/{table_id}",
        headers=AIRTABLE_HEADERS,
        params={"filterByFormula": formula}
    )
    r.raise_for_status()
    return r.json().get("records", [])


def airtable_create(table_id, fields):
    r = requests.post(
        f"{AIRTABLE_URL}/{BASE_ID}/{table_id}",
        headers=AIRTABLE_HEADERS,
        json={"fields": fields}
    )
    r.raise_for_status()
    return r.json()

# ---------------- HELPERS ----------------
def order_exists(order_id):
    return len(airtable_search(ORDERS_TABLE_ID, f"{{Order ID}}='{order_id}'")) > 0


def get_or_create_customer(customer):
    records = airtable_search(
        CUSTOMERS_TABLE_ID,
        f"{{Trendyol Id}}='{customer['id']}'"
    )

    if records:
        return records[0]["id"]

    rec = airtable_create(
        CUSTOMERS_TABLE_ID,
        {
            "Trendyol Id": customer["id"],
            "Name": customer["name"],
            "Address": customer["address"],
            "Acquired sales channel": "Trendyol"
        }
    )
    return rec["id"]

# ---------------- SYNC ORDERS (CORRECT WAY) ----------------
@app.route("/trendyol/sync", methods=["GET"])
def sync_orders():
    try:
        url = f"{TRENDYOL_BASE_URL}/integration/order/sellers/{TRENDYOL_SELLER_ID}/orders"

        # 📅 01/02/2026
        params = {
            "startDate": 1769904000000,
            "endDate": 1769990399000,
            "page": 0,
            "size": 50
        }

        print("📡 Fetching orders from Trendyol (ORDERS API)")
        r = requests.get(url, headers=TRENDYOL_HEADERS, params=params)
        print("➡️ Status:", r.status_code)
        print("📨 Raw:", r.text)
        r.raise_for_status()

        orders = r.json().get("content", [])
        print(f"📦 Orders received: {len(orders)}")

        synced = 0

        for o in orders:
            order_id = str(o["orderNumber"])
            if order_exists(order_id):
                continue

            customer = {
                "id": str(o["customerId"]),
                "name": f"{o.get('customerFirstName','')} {o.get('customerLastName','')}",
                "address": o.get("shipmentAddress", {}).get("fullAddress", "")
            }

            customer_id = get_or_create_customer(customer)

            airtable_create(
                ORDERS_TABLE_ID,
                {
                    "Order ID": order_id,
                    "Customer": [customer_id],
                    "Order Date": o.get("orderDate"),
                    "Sales Channel": "Trendyol"
                }
            )

            synced += 1

        return jsonify({"synced": synced}), 200

    except Exception as e:
        print("❌ Sync error:", e)
        return jsonify({"error": "sync failed"}), 500

# ---------------- RUN ----------------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
