from pycloudrestapi import IBTConnect
import pyotp
from pymongo import MongoClient



# Define your credentials
params = {
    "baseurl": "https://jri4df7kaa.execute-api.ap-south-1.amazonaws.com/prod/interactive",
    "api_key": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzUHVibGlzaGVyQ29kZSI6ImN1IiwiQ3VzdG9tZXJJZCI6IjM4NSIsInNQYXJ0bmVyQXBwSWQiOiIwMUYwMEYiLCJzQXBwbGljYXRpb25Ub2tlbiI6IkluZGlyYVNlY3VyaXRpZXNCMkMxMDcwNDY0ZGVlZiIsIlB1Ymxpc2hlck5hbWUiOiJJbmRpcmEgU2VjdXJpdGllcyBQdnQgTHRkIC0gQjJDIiwiQnJva2VyTmFtZSI6IkluZGlyYSBTZWN1cml0aWVzIFB2dCBMdGQiLCJQcm9kdWN0U291cmNlIjoiIiwiQjJDIjoiWSIsInVzZXJJZCI6IklTMjEwNTIiLCJleHAiOjkyMDY1NTc1MDAsImlhdCI6MTc0MTU5NzUxNH0.bpDvWPT460MXChAkZMICaMh5E8CXDXWJ_neY3bPGTg4",
    "x-api-key": None,
    "userId": "IS21052",
    "password": "Meenu@5555",
    "totp": pyotp.TOTP("KMMGACRMBJXAOW37").now(),
    "debug": True
}

try:
    mongo_client = MongoClient("mongodb://localhost:27017/") 
    db = mongo_client["MarketDB"]  # Database name
    collection = db["BannedScrips"]  # Collection name
    holiday_collection = db["HolidayMaster"] 
    lt_price_band_collection = db["LTP band master"]
    print("✅ Connected to MongoDB")

except Exception as e:
        print(f"MongoDB Connection Error: {e}")
        

# Initialize IBTConnect
ibt = IBTConnect(params)

# Log in
login_response = ibt.login(params)

if login_response.get("status") == "success":
    print("Login Successful!")

    # Fetch banned scrips data
    tenant_id = login_response.get("data", {}).get("tenant_id", "YOUR_TENANT_ID")  # Auto-fetch if available
    banned_scrips_response = ibt.Banned_Scrips({"tenantid": tenant_id})
    if banned_scrips_response and "data" in banned_scrips_response:
        data_to_insert = banned_scrips_response["data"]

        # Ensure it's a list before inserting
        if isinstance(data_to_insert, dict):  
            data_to_insert = [data_to_insert]

        if isinstance(data_to_insert, list) and len(data_to_insert) > 0:
            collection.insert_many(data_to_insert)
            print("Banned Scrips Data Stored in MongoDB!")
        else:
            print("No valid Banned Scrips Data Found.")
    else:
        print("No Banned Scrips Data Found.")
    
    
    # ✅ Fetch & Store Data as Separate Documents
    holiday_master_response = ibt.holiday_master({"tenantid": tenant_id})
    print("🔍 Raw Holiday Master Response:", holiday_master_response)  

    if holiday_master_response:
        holiday_data = holiday_master_response  

        if not holiday_data or not isinstance(holiday_data, dict):  
            print("No valid data to store.")
        else:
            holiday_records = []  

            for outer_key, inner_dict in holiday_data.items():  
                if isinstance(inner_dict, dict):  
                    for sub_key, values in inner_dict.items():  
                        if isinstance(values, list):
                            holiday_entry = {
                                "outer_key": outer_key,  
                                "sub_key": sub_key,  
                                "values": values  
                            }
                            holiday_records.append(holiday_entry)  

            
            if holiday_records:
                holiday_collection.insert_many(holiday_records) 
                print("✅Data Stored as Separate Entries in MongoDB!")
            else:
                print("No valid records to insert.")

    else:
        print("No Data Found.")

    # GET LTP Price Band Master
    
    lt_price_band_response = ibt.get_LTP_price_band_master({"tenantid": tenant_id})
    print("🔍 Raw LT Price Band Master Response:", lt_price_band_response) 

    if lt_price_band_response and "data" in lt_price_band_response:
        lt_price_band_data = lt_price_band_response["data"]  

        if not lt_price_band_data:
            print("No valid LT Price Band Master data to store.")
        else:
            lt_price_band_records = [] 

            for record in lt_price_band_data:  
                if isinstance(record, dict): 
                    lt_price_band_records.append(record)

            if lt_price_band_records:
                lt_price_band_collection.insert_many(lt_price_band_records)  
                print("✅ LT Price Band Master Data Stored Successfully in MongoDB!")
            else:
                print("No valid LT Price Band Master records to insert.")

    else:
        print("No LT Price Band Master Data Found.")

else:
    print("Login Failed:", login_response)
