import os
import base64
import requests
import threading
from datetime import datetime
from flask import Flask, jsonify, request

# ======================================================
# APP
# ======================================================
app = Flask(__name__)

# ======================================================
# CONFIG
# ======================================================
AIRTABLE_TOKEN = os.getenv("AIRTABLE_TOKEN")
BASE_ID = os.getenv("BASE_ID")
CUSTOMERS_TABLE_ID = os.getenv("CUSTOMERS_TABLE")
ORDERS_TABLE_ID = os.getenv("ORDERS_TABLE")

TRENDYOL_SELLER_ID = os.getenv("SELLER_ID")
TRENDYOL_API_KEY = os.getenv("API_KEY")
TRENDYOL_API_SECRET = os.getenv("API_SECRET")

CRON_SECRET = os.getenv("CRON_SECRET")  # MUST be set in Render

AIRTABLE_URL = "https://api.airtable.com/v0"
TRENDYOL_BASE_URL = "https://apigw.trendyol.com"
REQUEST_TIMEOUT = 20

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
    "User-Agent": "TrendyolAirtableSync/1.0",
    "Content-Type": "application/json"
}

# ======================================================
# AIRTABLE HELPERS
# ======================================================
def airtable_search(table_id, formula):
    r = requests.get(
        f"{AIRTABLE_URL}/{BASE_ID}/{table_id}",
        headers=AIRTABLE_HEADERS,
        params={"filterByFormula": formula},
        timeout=REQUEST_TIMEOUT
    )
    r.raise_for_status()
    return r.json().get("records", [])

def airtable_create(table_id, fields):
    r = requests.post(
        f"{AIRTABLE_URL}/{BASE_ID}/{table_id}",
        headers=AIRTABLE_HEADERS,
        json={"fields": fields},
        timeout=REQUEST_TIMEOUT
    )
    if r.status_code >= 400:
        print("❌ Airtable error:", r.text)
        r.raise_for_status()

# ======================================================
# STATUS MAPPERS
# ======================================================
def map_shipping_status(order):
    return "Shipped" if order.get("status", "").lower() in [
        "shipped", "delivered", "invoiced"
    ] else "New"

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
    records = airtable_search(
        CUSTOMERS_TABLE_ID,
        f"{{Trendyol Id}}='{c['id']}'"
    )
    if records:
        return records[0]["id"]

    r = requests.post(
        f"{AIRTABLE_URL}/{BASE_ID}/{CUSTOMERS_TABLE_ID}",
        headers=AIRTABLE_HEADERS,
        json={"fields": {"Name": c["name"], "Trendyol Id": c["id"]}},
        timeout=REQUEST_TIMEOUT
    )
    r.raise_for_status()
    return r.json()["id"]

# ======================================================
# DUPLICATE CHECK
# ======================================================
def order_line_exists(order_id, product_name):
    records = airtable_search(
        ORDERS_TABLE_ID,
        f"AND({{Order ID}}='{order_id}', {{Trendyol Product Name}}='{product_name}')"
    )
    return bool(records)

# ======================================================
# CREATE ORDER LINE
# ======================================================
def create_order_line(order_id, order_number, customer_id, date, pay, ship, product, qty, price):
    airtable_create(
        ORDERS_TABLE_ID,
        {
            "Order ID": order_id,
            "Order Number": order_number,
            "Customer": [customer_id],
            "Order Date": date,
            "Payment Status": pay,
            "Shipping Status": ship,
            "Sales Channel": "Trendyol",
            "Trendyol Product Name": product,
            "Quantity": str(qty),
            "Item Value": str(price)
        }
    )

# ======================================================
# MAIN SYNC LOGIC
# ======================================================
def sync_trendyol_orders_job():
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

        for o in orders:
            order_id = str(o["id"])
            order_number = str(o["orderNumber"])

            customer_id = get_or_create_customer({
                "id": str(o["customerId"]),
                "name": f"{o.get('customerFirstName','')} {o.get('customerLastName','')}"
            })

            order_date = datetime.utcfromtimestamp(
                o["orderDate"] / 1000
            ).strftime("%Y-%m-%d")

            pay = map_payment_status(o)
            ship = map_shipping_status(o)

            for line in o.get("lines", []):
                product = line.get("productName", "")
                qty = line.get("quantity", 1)
                price = line.get("price", "")

                if order_line_exists(order_id, product):
                    continue

                create_order_line(
                    order_id,
                    order_number,
                    customer_id,
                    order_date,
                    pay,
                    ship,
                    product,
                    qty,
                    price
                )

                print(f"✅ Synced {order_number} → {product}")

    except Exception as e:
        print("❌ Sync error:", e)

    print("🎉 Trendyol sync finished")

# ======================================================
# API ROUTES (CRON SAFE)
# ======================================================
@app.route("/trendyol/sync", methods=["POST", "HEAD"])
def cron_sync():
    # Allow cron-job.org HEAD checks
    if request.method == "HEAD":
        return "", 200

    # Secret validation
    if request.headers.get("X-Cron-Secret") != CRON_SECRET:
        return "Unauthorized", 401

    threading.Thread(target=sync_trendyol_orders_job).start()
    return jsonify({"status": "sync started"}), 202

@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"}), 200

# ======================================================
# LOCAL RUN
# ======================================================
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
