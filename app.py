import os
import time
import requests
import base64
from flask import Flask
from datetime import datetime, timezone, timedelta

app = Flask(__name__)

# ===============================
# ENV
# ===============================
AIRTABLE_TOKEN = os.getenv("AIRTABLE_TOKEN")
BASE_ID = os.getenv("BASE_ID")

ORDERS_TABLE = os.getenv("ORDERS_TABLE")
CUSTOMERS_TABLE = os.getenv("CUSTOMERS_TABLE")
FRENCH_INVENTORIES_TABLE = os.getenv("FRENCH_INVENTORIES_TABLE")

SELLER_ID = os.getenv("SELLER_ID")
API_KEY = os.getenv("API_KEY")
API_SECRET = os.getenv("API_SECRET")

LAST_SYNC_DATE = os.getenv("LAST_SYNC_DATE")  # epoch ms string

# ===============================
# HEADERS
# ===============================
AIRTABLE_HEADERS = {
    "Authorization": f"Bearer {AIRTABLE_TOKEN}",
    "Content-Type": "application/json"
}

auth_string = f"{API_KEY}:{API_SECRET}"
encoded_auth = base64.b64encode(auth_string.encode()).decode()

TRENDYOL_HEADERS = {
    "Authorization": f"Basic {encoded_auth}",
    "Accept": "application/json",
    "User-Agent": "TrendyolAirtableSync/1.0",
    "storeFrontCode": "AE"
}

# ===============================
# LOG
# ===============================
def log(msg):
    print(f"[{datetime.utcnow().isoformat()}] {msg}", flush=True)

# ===============================
# AIRTABLE
# ===============================
def airtable_get(table, formula):
    url = f"https://api.airtable.com/v0/{BASE_ID}/{table}"
    params = {"filterByFormula": formula}
    return requests.get(url, headers=AIRTABLE_HEADERS, params=params).json()

def airtable_create(table, fields):
    url = f"https://api.airtable.com/v0/{BASE_ID}/{table}"
    res = requests.post(url, headers=AIRTABLE_HEADERS, json={"fields": fields})
    if res.status_code >= 300:
        log(f"❌ Airtable error → {res.text}")
        return None
    return res.json()

# ===============================
# HELPERS
# ===============================
def get_or_create_customer(c):
    res = airtable_get(CUSTOMERS_TABLE, f"{{Trendyol Id}}='{c['id']}'")
    if res.get("records"):
        return res["records"][0]["id"]

    created = airtable_create(CUSTOMERS_TABLE, {
        "Name": f"{c['firstName']} {c['lastName']}",
        "Trendyol Id": str(c["id"]),
        "Contact Number": c.get("phone"),
        "Address": c.get("address"),
        "Acquired sales channel": "Trendyol"
    })
    return created["id"] if created else None

def get_inventory_record(sku):
    res = airtable_get(FRENCH_INVENTORIES_TABLE, f"{{SKU}}='{sku}'")
    return res["records"][0]["id"] if res.get("records") else None

# ===============================
# MAIN SYNC LOOP (NO CRON)
# ===============================
def watch_trendyol_orders():
    global LAST_SYNC_DATE

    if not LAST_SYNC_DATE:
        LAST_SYNC_DATE = str(
            int((datetime.now(timezone.utc) - timedelta(days=1)).timestamp() * 1000)
        )
        log(f"🕒 Initial LAST_SYNC_DATE → {LAST_SYNC_DATE}")

    while True:
        try:
            log("🔍 Checking for new Trendyol orders")

            res = requests.get(
                f"https://apigw.trendyol.com/integration/order/sellers/{SELLER_ID}/orders",
                headers=TRENDYOL_HEADERS,
                params={"page": 0, "size": 50, "startDate": LAST_SYNC_DATE}
            )

            if res.status_code != 200:
                log(f"❌ Trendyol error {res.status_code}")
                time.sleep(300)
                continue

            orders = res.json().get("content", [])
            log(f"📦 Orders fetched: {len(orders)}")

            newest_time = None

            for order in orders:
                order_id = str(order["id"])

                exists = airtable_get(ORDERS_TABLE, f"{{Order ID}}='{order_id}'")
                if exists.get("records"):
                    continue

                customer_id = get_or_create_customer({
                    "id": order["customerId"],
                    "firstName": order["shipmentAddress"]["firstName"],
                    "lastName": order["shipmentAddress"]["lastName"],
                    "phone": order["shipmentAddress"].get("phone"),
                    "address": order["shipmentAddress"].get("address1")
                })

                sku_links = []
                for line in order["lines"]:
                    inv = get_inventory_record(line.get("merchantSku"))
                    if inv:
                        sku_links.append(inv)

                airtable_create(ORDERS_TABLE, {
                    "Order ID": order_id,
                    "Order Number": str(order["orderNumber"]),
                    "Customer": [customer_id] if customer_id else [],
                    "Item SKU": sku_links,
                    "Order Date": datetime.fromtimestamp(
                        order["orderDate"] / 1000
                    ).strftime("%Y-%m-%d"),
                    "Sales Channel": "Trendyol",
                    "Payment Status": "Pending",
                    "Shipping Status": "New"
                })

                log(f"✅ New order {order_id} saved")
                newest_time = max(newest_time or 0, order["orderDate"])

            if newest_time:
                LAST_SYNC_DATE = str(newest_time)
                log(f"🕒 LAST_SYNC_DATE updated → {LAST_SYNC_DATE}")

        except Exception as e:
            log(f"🔥 Error → {e}")

        log("😴 Sleeping 5 minutes")
        time.sleep(300)  # 5 minutes

# ===============================
# START WATCHER ON BOOT
# ===============================
@app.before_first_request
def start_watcher():
    log("🧵 Starting background watcher")
    import threading
    threading.Thread(target=watch_trendyol_orders, daemon=True).start()

@app.route("/")
def health():
    return "OK", 200
