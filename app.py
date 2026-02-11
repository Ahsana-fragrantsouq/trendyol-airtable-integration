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
INVENTORY_TABLE_ID = os.getenv("FRENCH_INVENTORIES_TABLE")

TRENDYOL_SELLER_ID = os.getenv("SELLER_ID")
TRENDYOL_API_KEY = os.getenv("API_KEY")
TRENDYOL_API_SECRET = os.getenv("API_SECRET")

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
    r = requests.post(url, headers=AIRTABLE_HEADERS, json={"fields": fields})

    if not r.ok:
        print("❌ Airtable error:", r.text)

    r.raise_for_status()
    return r.json()

# ---------------- CUSTOMER ----------------
def get_or_create_customer(trendyol_customer):
    formula = f"{{Trendyol Id}}='{trendyol_customer['id']}'"
    records = airtable_search(CUSTOMERS_TABLE_ID, formula)

    if records:
        return records[0]["id"]

    record = airtable_create(
        CUSTOMERS_TABLE_ID,
        {
            "Name": trendyol_customer["name"],
            "Trendyol Id": trendyol_customer["id"]
        }
    )
    return record["id"]

# ---------------- INVENTORY (SKU) ----------------
def get_inventory_record_id(sku):
    formula = f"{{SKU}}='{sku}'"
    records = airtable_search(INVENTORY_TABLE_ID, formula)

    if not records:
        raise Exception(f"SKU not found in French Inventories: {sku}")

    return records[0]["id"]

# ---------------- ORDER ----------------
def order_exists(order_id):
    records = airtable_search(
        ORDERS_TABLE_ID,
        f"{{Order ID}}='{order_id}'"
    )
    return len(records) > 0


def create_order(data):
    airtable_create(
        ORDERS_TABLE_ID,
        {
            "Order ID": data["order_id"],
            "Customer": [data["customer_record_id"]],
            "Item SKU": [data["inventory_record_id"]],
            "Order Date": data["order_date"],
            "Payment Status": "Pending",
            "Shipping Status": "New",
            "Sales Channel": "Trendyol"
        }
    )

# ---------------- TRENDYOL SYNC ----------------
def sync_trendyol_orders_job():
    try:
        print("⏰ Trendyol sync started")

        r = requests.get(
            f"{TRENDYOL_BASE_URL}/integration/order/sellers/{TRENDYOL_SELLER_ID}/orders",
            headers=TRENDYOL_HEADERS,
            params={"page": 0, "size": 10}
        )
        r.raise_for_status()

        orders = r.json().get("content", [])

        for o in orders:
            order_id = str(o["orderNumber"])

            if order_exists(order_id):
                continue

            # ---- customer ----
            customer_record_id = get_or_create_customer({
                "id": str(o["customerId"]),
                "name": f"{o.get('customerFirstName','')} {o.get('customerLastName','')}"
            })

            # ---- inventory / SKU ----
            line = o.get("lines", [{}])[0]
            sku = line.get("merchantSku")
            inventory_record_id = get_inventory_record_id(sku)

            # ---- order date ----
            order_date = datetime.utcfromtimestamp(
                o["orderDate"] / 1000
            ).strftime("%Y-%m-%d")

            create_order({
                "order_id": order_id,
                "customer_record_id": customer_record_id,
                "inventory_record_id": inventory_record_id,
                "order_date": order_date
            })

            print("✅ Synced order:", order_id)

        print("🎉 Trendyol sync completed")

    except Exception as e:
        print("❌ Sync error:", e)

# ---------------- API ----------------
@app.route("/trendyol/sync")
def manual_sync():
    sync_trendyol_orders_job()
    return jsonify({"status": "sync triggered"}), 200

# ---------------- SCHEDULER ----------------
ist = pytz.timezone("Asia/Kolkata")
scheduler = BackgroundScheduler(timezone=ist)

scheduler.add_job(sync_trendyol_orders_job, CronTrigger(hour=18, minute=30))
scheduler.add_job(sync_trendyol_orders_job, CronTrigger(hour=18, minute=10))
scheduler.add_job(sync_trendyol_orders_job, CronTrigger(hour=18, minute=20))
scheduler.add_job(sync_trendyol_orders_job, CronTrigger(hour=18, minute=40))

scheduler.start()

# ---------------- RUN ----------------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
