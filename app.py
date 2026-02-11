import os
import base64
import requests
import pytz
from datetime import datetime
from flask import Flask, jsonify
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

app = Flask(__name__)

# ---------------- CONFIG (MATCHES RENDER ENV) ----------------
AIRTABLE_TOKEN = os.getenv("AIRTABLE_TOKEN")
BASE_ID = os.getenv("BASE_ID")
CUSTOMERS_TABLE_ID = os.getenv("CUSTOMERS_TABLE")
ORDERS_TABLE_ID = os.getenv("ORDERS_TABLE")

TRENDYOL_SELLER_ID = os.getenv("SELLER_ID")
TRENDYOL_API_KEY = os.getenv("API_KEY")
TRENDYOL_API_SECRET = os.getenv("API_SECRET")

print("🔧 CONFIG LOADED")
print("BASE_ID:", BASE_ID)
print("CUSTOMERS_TABLE_ID:", CUSTOMERS_TABLE_ID)
print("ORDERS_TABLE_ID:", ORDERS_TABLE_ID)
print("SELLER_ID:", TRENDYOL_SELLER_ID)

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
    "storeFrontCode": "AE",
    "User-Agent": "TrendyolAirtableSync/1.0",
    "Content-Type": "application/json"
}

AIRTABLE_URL = "https://api.airtable.com/v0"
TRENDYOL_BASE_URL = "https://apigw.trendyol.com"

# ---------------- HEALTH CHECK ----------------
@app.route("/health")
def health():
    return jsonify({"status": "ok"}), 200


# ---------------- AIRTABLE HELPERS ----------------
def airtable_search(table_id, formula):
    url = f"{AIRTABLE_URL}/{BASE_ID}/{table_id}"
    params = {"filterByFormula": formula}
    r = requests.get(url, headers=AIRTABLE_HEADERS, params=params)
    r.raise_for_status()
    return r.json().get("records", [])


def airtable_create(table_id, fields):
    url = f"{AIRTABLE_URL}/{BASE_ID}/{table_id}"
    payload = {"fields": fields}
    r = requests.post(url, headers=AIRTABLE_HEADERS, json=payload)

    if not r.ok:
        print("❌ Airtable error:", r.text)

    r.raise_for_status()
    return r.json()


# ---------------- CUSTOMER ----------------
def get_or_create_customer(customer):
    formula = f"{{Trendyol Id}}='{customer['customerId']}'"
    records = airtable_search(CUSTOMERS_TABLE_ID, formula)

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
    return record["id"]


# ---------------- ORDER ----------------
def order_exists(order_id):
    formula = f"{{Order ID}}='{order_id}'"
    records = airtable_search(ORDERS_TABLE_ID, formula)
    return len(records) > 0


def create_order(order, customer_record_id):
    airtable_create(
        ORDERS_TABLE_ID,
        {
            "Order ID": order["orderId"],
            "Customer": [customer_record_id],  # must be LINKED RECORD
            "Order Date": order["orderDate"],  # ISO format
            "Item SKU": order["sku"],
            "Product Name": order["productName"],
            "Payment Status": "Pending",
            "Shipping Status": "New",
            "Sales Channel": "Trendyol"
        }
    )


# ---------------- TRENDYOL SYNC JOB ----------------
def sync_trendyol_orders_job():
    try:
        print("⏰ Trendyol sync started")

        url = f"{TRENDYOL_BASE_URL}/integration/order/sellers/{TRENDYOL_SELLER_ID}/orders"
        params = {"page": 0, "size": 10}

        r = requests.get(url, headers=TRENDYOL_HEADERS, params=params)
        r.raise_for_status()

        orders = r.json().get("content", [])
        synced = 0

        for o in orders:
            order_id = str(o["orderNumber"])

            if order_exists(order_id):
                continue

            # --- customer ---
            customer = {
                "customerId": str(o["customerId"]),
                "name": f"{o.get('customerFirstName','')} {o.get('customerLastName','')}",
                "address": o.get("shipmentAddress", {}).get("fullAddress", "")
            }

            customer_record_id = get_or_create_customer(customer)

            # --- safe line handling ---
            line = o.get("lines", [{}])[0]

            # --- convert timestamp (ms → ISO date) ---
            order_date = None
            if o.get("orderDate"):
                order_date = datetime.utcfromtimestamp(
                    o["orderDate"] / 1000
                ).strftime("%Y-%m-%d")

            order = {
                "orderId": order_id,
                "orderDate": order_date,
                "sku": line.get("merchantSku", ""),
                "productName": line.get("productName", "")
            }

            create_order(order, customer_record_id)
            synced += 1
            print("✅ Synced:", order_id)

        print(f"🎉 Sync done | {synced} new orders")

    except Exception as e:
        print("❌ Sync error:", e)


# ---------------- MANUAL API ----------------
@app.route("/trendyol/sync")
def manual_sync():
    sync_trendyol_orders_job()
    return jsonify({"status": "manual sync triggered"}), 200


# ---------------- IP CHECK ----------------
@app.route("/ip")
def ip():
    return requests.get("https://api.ipify.org").text


# ---------------- SCHEDULER (IST) ----------------
ist = pytz.timezone("Asia/Kolkata")
scheduler = BackgroundScheduler(timezone=ist)

scheduler.add_job(sync_trendyol_orders_job, CronTrigger(hour=4, minute=0), id="4am")
scheduler.add_job(sync_trendyol_orders_job, CronTrigger(hour=17, minute=55), id="545pm")
scheduler.add_job(sync_trendyol_orders_job, CronTrigger(hour=18, minute=15), id="615pm")
scheduler.add_job(sync_trendyol_orders_job, CronTrigger(hour=19, minute=0), id="7pm")

scheduler.start()
print("⏰ Scheduler started (IST)")


# ---------------- RUN ----------------
if __name__ == "__main__":
    print("🔥 Flask server running")
    app.run(host="0.0.0.0", port=5000)
