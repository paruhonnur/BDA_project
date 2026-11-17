import requests
import os

API_KEY = os.getenv("DATA_GOV_API_KEY")

RESOURCE_ID = "9ef84268-d588-465a-a308-a864a43d0070"

URL = "https://api.data.gov.in/resource/" + RESOURCE_ID

if not API_KEY:
    print("API key not found. Set DATA_GOV_API_KEY first.")
else:
    params = {
        "api-key": API_KEY,
        "format": "json",
        "limit": 10
    }

    print("Connecting to Government Mandi API...")

    try:
        response = requests.get(URL, params=params, timeout=120)

        if response.status_code == 200:
            data = response.json()
            records = data.get("records", [])

            print("API Connection Successful!")
            print("Records Retrieved:", len(records))

            for record in records:
                print(record)
        else:
            print("Error:", response.status_code)
            print(response.text)

    except requests.exceptions.RequestException as e:
        print("Connection failed:", e)
