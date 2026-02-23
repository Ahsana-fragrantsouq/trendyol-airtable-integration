import os
import base64
import requests
import threading
from datetime import datetime
from dotenv import load_dotenv
from flask import Flask, jsonify

# ======================================================
# LOAD ENV
# ======================================================
load_dotenv()

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

AIRTABLE_URL = "https://api.airtable.com/v0"
TRENDYOL_BASE_URL = "https://apigw.trendyol.com"
REQUEST_TIMEOUT = 30

# ======================================================
# FLASK APP
# ======================================================
app = Flask(__name__)

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
# LOCK
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
    r.raise_for_status()
    return r.json()

def airtable_update(table_id, record_id, fields):
    r = requests.patch(
        f"{AIRTABLE_URL}/{BASE_ID}/{table_id}/{record_id}",
        headers=AIRTABLE_HEADERS,
        json={"fields": fields},
        timeout=REQUEST_TIMEOUT
    )
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
# CUSTOMER SYNC
# ======================================================
def get_or_create_customer(customer):
    """
    customer = { id, name }
    """
    records = airtable_search(
        CUSTOMERS_TABLE_ID,
        f"{{Trendyol Id}}='{customer['id']}'"
    )

    # Exists → update name
    if records:
        record_id = records[0]["id"]
        airtable_update(
            CUSTOMERS_TABLE_ID,
            record_id,
            {"Name": customer["name"]}
        )
        return record_id

    # New customer
    res = airtable_create(
        CUSTOMERS_TABLE_ID,
        {
            "Name": customer["name"],
            "Trendyol Id": customer["id"],
            "Acquired sales channel": "Trendyol"
        }
    )
    return res["id"]

# ======================================================
# ORDER HELPERS
# ======================================================
def order_line_exists(order_id, product_name):
    records = airtable_search(
        ORDERS_TABLE_ID,
        f"AND({{Order ID}}='{order_id}', {{Trendyol Product Name}}='{product_name}')"
    )
    return bool(records)

def create_order_line(order, line, customer_record_id):
    order_id = str(order["id"])

    if order_line_exists(order_id, line["productName"]):
        return

    order_date = datetime.utcfromtimestamp(
        order["orderDate"] / 1000
    ).strftime("%Y-%m-%d")

    airtable_create(
        ORDERS_TABLE_ID,
        {
            "Order ID": order_id,
            "Order Number": str(order["orderNumber"]),
            "Customer": [customer_record_id],
            "Order Date": order_date,
            "Trendyol Product Name": line["productName"],
            "Quantity": str(line.get("quantity", 1)),
            "Item Value": str(line.get("price", "")),
            "Payment Status": map_payment_status(order),
            "Shipping Status": map_shipping_status(order),
            "Sales Channel": "Trendyol"
        }
    )

    print(f"✅ Order synced → {order_id} | {line['productName']}")

# ======================================================
# 🔥 MAIN UPDATE FUNCTION (WHAT YOU ASKED)
# ======================================================
def update_trendyol_to_airtable():
    if not sync_lock.acquire(blocking=False):
        return

    print("🔄 Updating Trendyol → Airtable")

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

        for order in orders:
            customer_record_id = get_or_create_customer({
                "id": str(order["customerId"]),
                "name": f"{order.get('customerFirstName','')} {order.get('customerLastName','')}"
            })

            for line in order.get("lines", []):
                create_order_line(order, line, customer_record_id)

    except Exception as e:
        print("❌ Update error:", e)

    finally:
        sync_lock.release()
        print("🎉 Update finished")

# ======================================================
# BROWSER TRIGGER
# ======================================================
@app.route("/update", methods=["GET"])
def manual_update():
    update_trendyol_to_airtable()
    return jsonify({"status": "Trendyol orders synced to Airtable"}), 200

# ======================================================
# RUN (RENDER / LOCAL)
# ======================================================
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)