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
    # TRENDYOL CONNECTION (FIXED)
    # =========================
    try:
        print("➡️ Testing Trendyol connection (correct gateway)...")

        seller_id = os.environ["SELLER_ID"]
        api_key = os.environ["API_KEY"]
        api_secret = os.environ["API_SECRET"]

        auth = base64.b64encode(
            f"{api_key}:{api_secret}".encode()
        ).decode()

        r = requests.get(
            f"https://apigw.trendyol.com/integration/order/sellers/{seller_id}/orders",
            headers={
                "Authorization": f"Basic {auth}",
                "User-Agent": "TrendyolAirtableSync/1.0",
                "Content-Type": "application/json",
                "storeFrontCode": "AE"
            },
            params={
                "page": 0,
                "size": 1
            },
            timeout=10
        )

        print("📦 Trendyol status:", r.status_code)
        print("📦 Trendyol body:", r.text[:300])

        results["trendyol"] = r.status_code

    except Exception as e:
        print("❌ Trendyol error:", str(e))
        results["trendyol_error"] = str(e)

    return jsonify(results)

if __name__ == "__main__":
    print("🚀 Server starting on port 10000")
    app.run(host="0.0.0.0", port=10000)
