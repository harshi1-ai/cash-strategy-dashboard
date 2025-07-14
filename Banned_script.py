import requests
import json
from pymongo import MongoClient
from datetime import datetime

# API constants
BASE_URL = "https://jri4df7kaa.execute-api.ap-south-1.amazonaws.com/prod/interactive/nontransactional/{tenantid}/v1"
TENANT_ID = "385"
ACCESS_TOKEN='eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJtZW1iZXJJZCI6OTA5MDkwLCJ1c2VyaWQiOjkwOTA5MCwidGVuYW50aWQiOjkwOTA5MCwibWVtYmVySW5mbyI6eyJ0ZW5hbnRJZCI6IjM4NSIsImdyb3VwSWQiOiJITyIsInVzZXJJZCI6IklTMjEwNTIiLCJ0ZW1wbGF0ZUlkIjoiV0FWRSIsInVkSWQiOiJmYjViNDExMTgzYmM0ZDEyIiwib2NUb2tlbiI6IjB4MDFCODFCMEI4Rjc3OTRGNjlEQjE1NThDOUY2NTMzIiwidXNlckNvZGUiOiJOWlNBRCIsImdyb3VwQ29kZSI6IkFBQUFBIiwiYXBpa2V5RGF0YSI6eyJzUHVibGlzaGVyQ29kZSI6ImN1IiwiQ3VzdG9tZXJJZCI6IjM4NSIsInNQYXJ0bmVyQXBwSWQiOiIwMUYwMEYiLCJzQXBwbGljYXRpb25Ub2tlbiI6IkluZGlyYVNlY3VyaXRpZXNCMkMxMDcwNDY0ZGVlZiIsIlB1Ymxpc2hlck5hbWUiOiJJbmRpcmEgU2VjdXJpdGllcyBQdnQgTHRkIC0gQjJDIiwiQnJva2VyTmFtZSI6IkluZGlyYSBTZWN1cml0aWVzIFB2dCBMdGQiLCJQcm9kdWN0U291cmNlIjoiIiwiQjJDIjoiWSIsInVzZXJJZCI6IklTMjEwNTIiLCJleHAiOjkyMDY1NTc1MDAsImlhdCI6MTc0MTU5NzUxNH0sInNvdXJjZSI6Ik1PQklMRUFQSSJ9LCJleHAiOjE3NTE3NDAxOTksImlhdCI6MTc1MTY5MjcxM30.QN8ZrfMea3D_oxCIDl4YhE-sYs9koeB0VCTwJ5zjEKw'   
API_KEY="eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzUHVibGlzaGVyQ29kZSI6ImN1IiwiQ3VzdG9tZXJJZCI6IjM4NSIsInNQYXJ0bmVyQXBwSWQiOiIwMUYwMEYiLCJzQXBwbGljYXRpb25Ub2tlbiI6IkluZGlyYVNlY3VyaXRpZXNCMkMxMDcwNDY0ZGVlZiIsIlB1Ymxpc2hlck5hbWUiOiJJbmRpcmEgU2VjdXJpdGllcyBQdnQgTHRkIC0gQjJDIiwiQnJva2VyTmFtZSI6IkluZGlyYSBTZWN1cml0aWVzIFB2dCBMdGQiLCJQcm9kdWN0U291cmNlIjoiIiwiQjJDIjoiWSIsInVzZXJJZCI6IklTMTgwODkiLCJleHAiOjkyMTQ0MTIxNjAsImlhdCI6MTc0OTQ1MjE3M30.QsnWmQBYiHunBeh9pogNBKYj6AT6mU7i2j2hDB4Z0j4"
mongo_client = MongoClient("mongodb://localhost:27017/")

# Get database and collections
db = mongo_client["OdinMasterData"]
holiday_collection = db["HolidayMaster"]
banned_scrips_collection = db["BannedScrips"]

headers = {
    "Authorization": f"Bearer {ACCESS_TOKEN}",
    "x-api-key": API_KEY
}

def save_banned_scrips_to_mongo(exchange, scrips_data):
    """
    Save each banned scrip as a separate document into MongoDB
    """
    for scrip in scrips_data:
        scrip["exchange"] = exchange if exchange else scrip.get("exchange", "UNKNOWN")
        result = banned_scrips_collection.update_one(
            {"exchange": scrip["exchange"], "token": scrip["token"], "symbol": scrip["symbol"]},
            {"$set": scrip},
            upsert=True
        )
        print(f"✅ MongoDB write for banned scrip {scrip['symbol']}: matched={result.matched_count}, modified={result.modified_count}")

def get_banned_scrips(exchange=None, token=None):
    url = f"{BASE_URL}/BannedScrips".format(tenantid=TENANT_ID)

    params = {}
    if exchange:
        params["exchange"] = exchange
    if token:
        params["token"] = token

    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        data = response.json()

        if data.get("status") == "success":
            print("Banned Scrips API call successful!")
            print(f"Message: {data.get('message')}")
            scrips = data.get("data", [])
            if scrips:
                print("\nBanned Scrips:")
                for scrip in scrips:
                    print(f"Exchange: {scrip['exchange']}, Token: {scrip['token']}, Symbol: {scrip['symbol']}")
                # Save each scrip as a separate document
                save_banned_scrips_to_mongo(exchange if exchange else "ALL", scrips)
            else:
                print("\nNo banned scrips found.")
        else:
            print(f"API call failed: {data.get('message')}")
    except requests.exceptions.RequestException as err:
        print(f"Request error: {err}")
    except json.JSONDecodeError:
        print("Error: Failed to parse the response as JSON.")

def save_holiday_master_to_mongo(exchange, holiday_data):
    """
    Save holiday master data for one exchange into MongoDB
    """
    document = {
        "outer_key": "data",
        "sub_key": exchange,
        "values": holiday_data,
        "last_updated": datetime.now().isoformat()
    }
    
    result = holiday_collection.update_one(
        {"sub_key": exchange},
        {"$set": document},
        upsert=True
    )
    print(f"✅ MongoDB write for holidays {exchange}: matched={result.matched_count}, modified={result.modified_count}")

def get_holiday_master(exchange=None, year=None):
    url = f"{BASE_URL}/holidaymaster".format(tenantid=TENANT_ID)

    params = {}
    if exchange:
        params["exchange"] = exchange
    if year:
        params["year"] = year

    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()

        data = response.json()
        if data.get("status") == "success":
            print(f"\n✅ Holidays for {exchange} ({year}):")
            holiday_data = data.get("data", {})
            if holiday_data:
                for exch_key, dates in holiday_data.items():
                    print(f"Exchange: {exch_key}")
                    for d in dates:
                        print(f" - {d}")
                    # Save to MongoDB
                    save_holiday_master_to_mongo(exch_key, dates)
            else:
                print("No holidays found.")
        else:
            print(f"API call failed for {exchange}: {data.get('message')}")
    except requests.exceptions.RequestException as err:
        print(f"Request error for {exchange}: {err}")
    except json.JSONDecodeError:
        print(f"Error parsing JSON for {exchange}.")

if __name__ == "__main__":
    # Call Banned Scrips API example
    get_banned_scrips(exchange="NSE_FO")

    # Call Holiday Master API for all exchanges
    exchanges = ["NSE_EQ", "NSE_FO"]
    
    # Get current year automatically
    year = datetime.now().year

    for exch in exchanges:
        get_holiday_master(exchange=exch, year=year)


