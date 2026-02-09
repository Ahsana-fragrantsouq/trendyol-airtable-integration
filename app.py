import os
import base64
import requests
from flask import Flask, jsonify

app = Flask(__name__)

@app.route("/", methods=["GET"])
def connection_test():
    results = {}

    # =========================
    # TRENDYOL CONNECTION
    # =========================
    try:
        auth = base64.b64encode(
            f"{os.environ['API_KEY']}:{os.environ['API_SECRET']}".encode()
        ).decode()

        r = requests.get(
            f"https://api.trendyol.com/sapigw/suppliers/{os.environ['SELLER_ID']}/orders",
            headers={
                "Authorization": f"Basic {auth}",
                "User-Agent": "RenderConnectionTest"
            },
            timeout=10
        )

        results["trendyol"] = r.status_code

    except Exception as e:
        results["trendyol_error"] = str(e)

    # =========================
    # AIRTABLE CONNECTIONS
    # =========================
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
            r = requests.get(
                f"https://api.airtable.com/v0/{base_id}/{os.environ[env_key]}",
                headers=airtable_headers,
                timeout=10
            )
            results[table_name] = r.status_code

        except Exception as e:
            results[f"{table_name}_error"] = str(e)

    return jsonify(results)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)
