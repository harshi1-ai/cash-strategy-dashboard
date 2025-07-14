import requests
import pymongo

# MongoDB Setup
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["odin_database"]
collection = db["scrip_master"]  

def fetch_and_store_scrip_master(exchange="NSE_FO"):
    
    url = f"https://odinscripmaster.s3.ap-south-1.amazonaws.com/scripfiles/{exchange}.json"
    
    response = requests.get(url)
    
    if response.status_code == 200:
        data = response.json()
        
       
        if isinstance(data, list):
            collection.insert_many(data)
            print(f"Successfully stored {len(data)} scrips in MongoDB.")
        else:
            print("Unexpected data format received.")
    
    else:
        print(f"Failed to fetch data: {response.status_code}")


fetch_and_store_scrip_master()

def get_details(exchange, **kwargs):
    
    query = {}

    if exchange == "NSE_EQ":
        if "code" in kwargs:
            query["code"] = kwargs["code"]
        elif "inst" in kwargs and "symbol" in kwargs:
            query["inst"] = kwargs["inst"]
            query["sym"] = kwargs["symbol"]
        else:
            print("For NSE_EQ, provide either 'code' or ('inst' + 'symbol').")
            return None

    elif exchange == "NSE_FO":
        if "code" in kwargs:
            query["code"] = kwargs["code"]
        elif "symbol" in kwargs and "expiry" in kwargs and "inst" in kwargs:
            query["sym"] = kwargs["symbol"]
            query["expiry"] = kwargs["expiry"]
            query["inst"] = kwargs["inst"]
        else:
            print("For NSE_FO, provide either 'code' or ('symbol' + 'expiry' + 'inst').")
            return None
    
    else:
        print("Invalid exchange. Choose 'NSE_EQ' or 'NSE_FO'.")
        return None

    result = collection.find_one(query, {"_id": 0})  
    
    if result:
        print("Data Found:", result)
        return result
    else:
        print("No data found for the given input.")
        return None


get_details("NSE_EQ", code=73497)
get_details("NSE_EQ", inst="EQUITIES", symbol="011NSETEST")
get_details("NSE_FO", code=12345)
get_details("NSE_FO", symbol="RELIANCE", expiry="2024-03-28", inst="FUTIDX")

