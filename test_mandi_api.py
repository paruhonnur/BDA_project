import requests
import os
import sys

# Read API key from environment variable
API_KEY = os.getenv("DATA_GOV_API_KEY")

# Government Mandi API resource ID
RESOURCE_ID = "9ef84268-d588-465a-a308-a864a43d0070"

url = f"https://api.data.gov.in/resource/{RESOURCE_ID}"

if not API_KEY:
    print("API key is missing")
    print("Please configure DATA_GOV_API_KEY")
    sys.exit(1)

params = {
    "api-key": API_KEY,
    "format": "json",
    "limit": 10,
    "filters[state.keyword]": "Karnataka"
}

try:
    print("Connecting to Government Mandi API...")

    response = requests.get(
        url,
        params=params,
        timeout=60
    )

    print("HTTP Status:", response.status_code)

    response.raise_for_status()

    data = response.json()

    records = data.get("records", [])

    print("Total records:", data.get("total"))
    print("Records received:", len(records))

    if not records:
        print("No mandi records received.")
    else:
        for record in records:
            print("\n-----------------------------")
            print("District:", record.get("district"))
            print("Market:", record.get("market"))
            print("Commodity:", record.get("commodity"))
            print("Date:", record.get("arrival_date"))
            print("Minimum price:", record.get("min_price"))
            print("Maximum price:", record.get("max_price"))
            print("Modal price:", record.get("modal_price"))

except requests.exceptions.Timeout:
    print("API request timed out. Try again.")

except requests.exceptions.RequestException as e:
    print("API Request Failed:", e)

except ValueError:
    print("Invalid JSON response received from API.")