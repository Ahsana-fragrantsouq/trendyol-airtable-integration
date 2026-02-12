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
    url = f"{AIRTABLE_URL}/{BASE_ID}/{table_id}"
    r = requests.get(url, headers=AIRTABLE_HEADERS, params={
        "filterByFormula": formula
    })
    r.raise_for_status()
    return r.json().get("records", [])


def airtable_create(table_id, fields):
    url = f"{AIRTABLE_URL}/{BASE_ID}/{table_id}"
    r = requests.post(url, headers=AIRTABLE_HEADERS, json={
        "fields": fields
    })
    r.raise_for_status()
    return r.json()

# ---------------- STATUS MAPPERS ----------------
def map_shipping_status(order):
    status = str(order.get("status", "")).lower()
    if status in ["shipped", "delivered", "invoiced"]:
        return "Shipped"
    return "New"


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
    formula = f"{{Trendyol Id}}='{customer['id']}'"
    records = airtable_search(CUSTOMERS_TABLE_ID, formula)

    if records:
        return records[0]["id"]

    record = airtable_create(
        CUSTOMERS_TABLE_ID,
        {
            "Name": customer["name"],
            "Trendyol Id": customer["id"]
        }
    )
    return record["id"]

# ---------------- ORDER ----------------
def order_exists(order_id):
    records = airtable_search(
        ORDERS_TABLE_ID,
        f"{{Order ID}}='{order_id}'"
    )
    return len(records) > 0


def create_order(
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
    payload = {
        "Order ID": order_id,
        "Order Number": order_number,
        "Customer": [customer_record_id],   # ✅ linked record
        "Order Date": order_date,
        "Payment Status": payment_status,
        "Shipping Status": shipping_status,
        "Sales Channel": "Trendyol",
        "Trendyol Product Name": product_name,
        "Quantity": quantity,
        "Item Value": item_value
    }

    r = requests.post(
        f"{AIRTABLE_URL}/{BASE_ID}/{ORDERS_TABLE_ID}",
        headers=AIRTABLE_HEADERS,
        json={"fields": payload}
    )

    if r.status_code >= 400:
        print("❌ Airtable error:", r.text)
        r.raise_for_status()



# ---------------- TRENDYOL SYNC ----------------
def sync_trendyol_orders_job():
    try:
        print("⏰ Trendyol sync started")

        response = requests.get(
            f"{TRENDYOL_BASE_URL}/integration/order/sellers/{TRENDYOL_SELLER_ID}/orders",
            headers=TRENDYOL_HEADERS,
            params={"page": 0, "size": 20}
        )
        response.raise_for_status()

        orders = response.json().get("content", [])

        for o in orders:
            order_id = str(o["id"])                 # internal Trendyol ID
            order_number = str(o["orderNumber"])   # shown to customer

            if order_exists(order_id):
                continue

            # -------- CUSTOMER --------
            customer_record_id = get_or_create_customer({
                "id": str(o["customerId"]),
                "name": f"{o.get('customerFirstName','')} {o.get('customerLastName','')}"
            })

            # -------- DATE --------
            order_date = datetime.utcfromtimestamp(
                o["orderDate"] / 1000
            ).strftime("%Y-%m-%d")

            # -------- PRODUCTS (MULTI LINE) --------
            product_names = []
            skus = []
            quantities = []
            prices = []

            for line in o.get("lines", []):
                product_names.append(line.get("productName", ""))
                skus.append(line.get("merchantSku", ""))
                quantities.append(str(line.get("quantity", 1)))
                prices.append(str(line.get("price", "")))

            create_order(
                order_id=order_id,
                order_number=order_number,
                customer_record_id=customer_record_id,
                order_date=order_date,
                payment_status=map_payment_status(o),
                shipping_status=map_shipping_status(o),
                product_name="\n".join(product_names),
                item_sku="\n".join(skus),
                quantity="\n".join(quantities),
                item_value="\n".join(prices)
            )

            print("✅ Order synced:", order_number)

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

# ---------------- SCHEDULER (IST) ----------------
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
