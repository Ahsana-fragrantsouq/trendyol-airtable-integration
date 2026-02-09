import os
import base64
import requests
import time
from flask import Flask, jsonify

app = Flask(__name__)

@app.route("/", methods=["GET"])
def connection_test():
    print("🔹 Connection test started")
    results = {}

    # =========================
    # TRENDYOL CONNECTION
    # =========================
    try:
        print("➡️ Testing Trendyol connection...")

        seller_id = os.environ["SELLER_ID"]
        api_key = os.environ["API_KEY"]
        api_secret = os.environ["API_SECRET"]

        print("✅ Trendyol env variables loaded")

        auth = base64.b64encode(
            f"{api_key}:{api_secret}".encode()
        ).decode()

        # REQUIRED date params (milliseconds)
        now = int(time.time() * 1000)
        one_day_ago = now - (24 * 60 * 60 * 1000)

        r = requests.get(
            f"https://api.trendyol.com/sapigw/suppliers/{seller_id}/orders",
            headers={
                "Authorization": f"Basic {auth}",
                "User-Agent": "RenderConnectionTest",
                "Accept": "application/json"
            },
            params={
                "startDate": one_day_ago,
                "endDate": now,
                "page": 0,
                "size": 1
            },
            timeout=10
        )

        print(f"📦 Trendyol response status: {r.status_code}")
        print(f"📦 Trendyol response body (debug): {r.text[:300]}")
        results["trendyol"] = r.status_code

    except Exception as e:
        print("❌ Trendyol error:", str(e))
        results["trendyol_error"] = str(e)

    # =========================
    # AIRTABLE CONNECTIONS
    # =========================
    try:
        print("➡️ Testing Airtable connection...")

        airtable_headers = {
            "Authorization": f"Bearer {os.environ['AIRTABLE_TOKEN']}"
        }

        base_id = os.environ["BASE_ID"]

        tables = {
            "Orders": "ORDERS_TABLE",
            "Customers": "CUSTOMERS_TABLE",
            "French Inventories": "FRENCH_INVENTORIES_TABLE"
        }

        for table_name, env_key in tables.items():
            try:
                table_value = os.environ[env_key]
                print(f"➡️ Testing Airtable table: {table_name}")

                r = requests.get(
                    f"https://api.airtable.com/v0/{base_id}/{table_value}",
                    headers=airtable_headers,
                    timeout=10
                )

                print(f"📊 {table_name} status: {r.status_code}")
                results[table_name] = r.status_code

            except Exception as e:
                print(f"❌ {table_name} error:", str(e))
                results[f"{table_name}_error"] = str(e)

    except Exception as e:
        print("❌ Airtable general error:", str(e))
        results["airtable_error"] = str(e)

    print("🔹 Connection test finished")
    return jsonify(results)

if __name__ == "__main__":
    print("🚀 Server starting on port 10000")
    app.run(host="0.0.0.0", port=10000)
