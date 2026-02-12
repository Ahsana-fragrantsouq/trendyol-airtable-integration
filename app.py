import os
import base64
import requests
import pytz
from datetime import datetime
from flask import Flask, jsonify
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

app = Flask(__name__)

# ---------------- CONFIG ----------------
AIRTABLE_TOKEN = os.getenv("AIRTABLE_TOKEN")
BASE_ID = os.getenv("BASE_ID")
CUSTOMERS_TABLE_ID = os.getenv("CUSTOMERS_TABLE")
ORDERS_TABLE_ID = os.getenv("ORDERS_TABLE")

TRENDYOL_SELLER_ID = os.getenv("SELLER_ID")
TRENDYOL_API_KEY = os.getenv("API_KEY")
TRENDYOL_API_SECRET = os.getenv("API_SECRET")

AIRTABLE_URL = "https://api.airtable.com/v0"
TRENDYOL_BASE_URL = "https://apigw.trendyol.com"

# ---------------- HEADERS ----------------
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

    if r.status_code >= 400:
        print("❌ Airtable error:", r.text)
        r.raise_for_status()

# ---------------- STATUS MAPPERS ----------------
def map_shipping_status(order):
    status = str(order.get("status", "")).lower()
    return "Shipped" if status in ["shipped", "delivered", "invoiced"] else "New"


def map_payment_status(order):
    status = str(order.get("status", "")).lower()
    if status in ["paid", "invoiced"]:
        return "Paid"
    if status == "cancelled":
        return "Failed"
    if status == "refunded":
        return "Refund"
    return "Pending"

# ---------------- CUSTOMER ----------------
def get_or_create_customer(customer):
    records = airtable_search(
        CUSTOMERS_TABLE_ID,
        f"{{Trendyol Id}}='{customer['id']}'"
    )

    if records:
        return records[0]["id"]

    r = requests.post(
        f"{AIRTABLE_URL}/{BASE_ID}/{CUSTOMERS_TABLE_ID}",
        headers=AIRTABLE_HEADERS,
        json={
            "fields": {
                "Name": customer["name"],
                "Trendyol Id": customer["id"]
            }
        }
    )
    r.raise_for_status()
    return r.json()["id"]

# ---------------- DUPLICATE CHECK ----------------
def order_line_exists(order_id, product_name):
    records = airtable_search(
        ORDERS_TABLE_ID,
        f"AND({{Order ID}}='{order_id}', {{Trendyol Product Name}}='{product_name}')"
    )
    return len(records) > 0

# ---------------- CREATE ORDER LINE ----------------
def create_order_line(
    order_id,
    order_number,
    customer_record_id,
    order_date,
    payment_status,
    shipping_status,
    product_name,
    quantity,
    item_value
):
    airtable_create(
        ORDERS_TABLE_ID,
        {
            "Order ID": order_id,
            "Order Number": order_number,
            "Customer": [customer_record_id],
            "Order Date": order_date,
            "Payment Status": payment_status,
            "Shipping Status": shipping_status,
            "Sales Channel": "Trendyol",
            "Trendyol Product Name": product_name,
            "Quantity": str(quantity),
            "Item Value": str(item_value)
        }
    )

# ---------------- TRENDYOL SYNC ----------------
def sync_trendyol_orders_job():
    try:
        print("⏰ Trendyol sync started")

        r = requests.get(
            f"{TRENDYOL_BASE_URL}/integration/order/sellers/{TRENDYOL_SELLER_ID}/orders",
            headers=TRENDYOL_HEADERS,
            params={"page": 0, "size": 50}
        )
        r.raise_for_status()

        orders = r.json().get("content", [])

        for o in orders:
            order_id = str(o["id"])
            order_number = str(o["orderNumber"])

            customer_record_id = get_or_create_customer({
                "id": str(o["customerId"]),
                "name": f"{o.get('customerFirstName','')} {o.get('customerLastName','')}"
            })

            order_date = datetime.utcfromtimestamp(
                o["orderDate"] / 1000
            ).strftime("%Y-%m-%d")

            payment_status = map_payment_status(o)
            shipping_status = map_shipping_status(o)

            # 🔥 ONE ROW PER PRODUCT
            for line in o.get("lines", []):
                product_name = line.get("productName", "")
                quantity = line.get("quantity", 1)
                item_value = line.get("price", "")

                if order_line_exists(order_id, product_name):
                    continue

                create_order_line(
                    order_id,
                    order_number,
                    customer_record_id,
                    order_date,
                    payment_status,
                    shipping_status,
                    product_name,
                    quantity,
                    item_value
                )

                print(f"✅ Synced {order_number} → {product_name}")

        print("🎉 Trendyol sync finished")

    except Exception as e:
        print("❌ Sync error:", e)

# ---------------- API ----------------
@app.route("/trendyol/sync")
def manual_sync():
    sync_trendyol_orders_job()
    return jsonify({"status": "sync completed"}), 200

@app.route("/health")
def health():
    return jsonify({"status": "ok"}), 200

# ---------------- SCHEDULER ----------------
ist = pytz.timezone("Asia/Kolkata")
scheduler = BackgroundScheduler(timezone=ist)

scheduler.add_job(sync_trendyol_orders_job, CronTrigger(hour=9, minute=0))
scheduler.add_job(sync_trendyol_orders_job, CronTrigger(hour=18, minute=45))
scheduler.add_job(sync_trendyol_orders_job, CronTrigger(hour=19, minute=0))
scheduler.add_job(sync_trendyol_orders_job, CronTrigger(hour=19, minute=15))

scheduler.start()
print("⏰ Scheduler started (IST)")

# ---------------- RUN ----------------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
