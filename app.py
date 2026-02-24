import os
import base64
import requests
import threading
from datetime import datetime
from flask import Flask, jsonify, request

# ======================================================
# CONFIG
# ======================================================
AIRTABLE_TOKEN = os.getenv("AIRTABLE_TOKEN")
BASE_ID = os.getenv("BASE_ID")
CUSTOMERS_TABLE_ID = os.getenv("CUSTOMERS_TABLE")
ORDER_LINE_ITEMS_TABLE_ID = os.getenv("ORDER_LINE_ITEMS_TABLE")
FRENCH_INVENTORIES_TABLE_ID = os.getenv("FRENCH_INVENTORIES_TABLE")

TRENDYOL_SELLER_ID = os.getenv("SELLER_ID")
TRENDYOL_API_KEY = os.getenv("API_KEY")
TRENDYOL_API_SECRET = os.getenv("API_SECRET")

AIRTABLE_URL = "https://api.airtable.com/v0"
TRENDYOL_BASE_URL = "https://apigw.trendyol.com"
REQUEST_TIMEOUT = 30

# ======================================================
# FLASK APP
# ======================================================
app = Flask(__name__)

# ======================================================
# ENV CHECK
# ======================================================
print("🔐 ENV CHECK:")
print("AIRTABLE_TOKEN:", bool(AIRTABLE_TOKEN))
print("BASE_ID:", bool(BASE_ID))
print("CUSTOMERS_TABLE:", bool(CUSTOMERS_TABLE_ID))
print("ORDER_LINE_ITEMS_TABLE:", bool(ORDER_LINE_ITEMS_TABLE_ID))
print("SELLER_ID:", bool(TRENDYOL_SELLER_ID))
print("API_KEY:", bool(TRENDYOL_API_KEY))
print("API_SECRET:", bool(TRENDYOL_API_SECRET))
print("--------------------------------------------------")
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
    "Content-Type": "application/json",
    "storeFrontCode": "AE"
}

# ======================================================
# GLOBAL LOCK
# ======================================================
sync_lock = threading.Lock()

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
        ORDER_LINE_ITEMS_TABLE_ID,
        f"AND({{Order ID}}='{order_id}', {{Trendyol Product Name}}='{product_name}')"
    )
    return bool(records)

# ======================================================
# CREATE ORDER LINE ITEM
# ======================================================
def create_order_line(order_id, order_number, customer_id, date, pay, ship, product, qty, price):
    airtable_create(
        ORDER_LINE_ITEMS_TABLE_ID,
        {
            "Order ID": order_id,
            "Order Number": order_number,
            "Customer": [customer_id],
            "Order Date": date,
            "Payment Status": pay,
            "Shipping Status": ship,
            "Sales Channel (from Orders)": "Trendyol",
            "Trendyol Product Name": product,
            "Qty": qty,
            "Rate": price
        }
    )

# ======================================================
# MAIN UPDATE LOGIC
# ======================================================
def sync_trendyol_orders_job():
    if not sync_lock.acquire(blocking=False):
        print("⏳ Sync already running — skipped")
        return

    print("⏰ Trendyol update started")

    try:
        r = requests.get(
            f"{TRENDYOL_BASE_URL}/integration/order/sellers/{TRENDYOL_SELLER_ID}/orders",
            headers=TRENDYOL_HEADERS,
            params={"page": 0, "size": 50},
            timeout=REQUEST_TIMEOUT
        )
        r.raise_for_status()

        orders = r.json().get("content", [])
        print(f"📦 Orders fetched: {len(orders)}")

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
        print("❌ Update error:", e)

    finally:
        sync_lock.release()
        print("🎉 Trendyol update finished")

# ======================================================
# BROWSER / AUTOMATION TRIGGER
# ======================================================
@app.route("/update", methods=["GET"])
def update_from_browser():
    secret = request.headers.get("X-Update-Secret")
    if secret != os.getenv("UPDATE_SECRET"):
        return jsonify({"error": "Unauthorized"}), 401

    if sync_lock.locked():
        return jsonify({"status": "Sync already running"}), 200

    threading.Thread(
        target=sync_trendyol_orders_job,
        daemon=True
    ).start()

    return jsonify({"status": "Trendyol sync started in background"}), 202

# ======================================================
# PING ENDPOINT
# ======================================================
@app.route("/ping", methods=["GET"])
def ping():
    secret = request.headers.get("X-Update-Secret")
    if secret != os.getenv("UPDATE_SECRET"):
        return jsonify({"error": "Unauthorized"}), 401

    if sync_lock.locked():
        return jsonify({"status": "Sync already running"}), 200

    threading.Thread(
        target=sync_trendyol_orders_job,
        daemon=True
    ).start()

    return jsonify({"status": "Ping OK – sync started"}), 200

@app.route("/", methods=["GET"])
def health():
    return "OK", 200

# ======================================================
# RUN
# ======================================================
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)