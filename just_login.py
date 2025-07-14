# import asyncio
# import pyotp
# import pandas as pd
# import numpy as np
# from pycloudrestapi import IBTConnect 
# import logging
# from pya3 import *
# import motor.motor_asyncio
# import aiohttp
# from datetime import datetime
# import pytz
# import json

# logger = logging.getLogger(__name__)

# class Login:
#     def __init__(self, user_id):
#         self.mongo_uri = "mongodb+srv://Akash:Akash%405555@stockvertexventures.fxlf1gk.mongodb.net/?tls=true&tlsAllowInvalidCertificates=true"  # Replace with your MongoDB URI
#         self.mongo_client = motor.motor_asyncio.AsyncIOMotorClient(self.mongo_uri)
#         self.api_url = "https://jri4df7kaa.execute-api.ap-south-1.amazonaws.com/prod/interactive"
#         self.timezone = pytz.timezone('Asia/Kolkata')
#         self.mongodb_database = self.mongo_client["OdinApp"]
#         self.user_id = user_id
#         self.user_collection = self.mongodb_database['Users_Details'] 
#         self.user_res_collection = self.mongodb_database["User_Login_Response"]
#         self.Date = datetime.now().strftime("%Y-%m-%d")
#         self.login_status = False
#         self.access_token = None
#         self.api_key = None
#         self.tenant_id = None
#         self.ibt_connect = None

#     async def async_init(self):
#         try:
#             doc = await self.user_collection.find_one({"User_ID": self.user_id})
#             if not doc:
#                 logger.error(f"Document not found for User ID: {self.user_id}")
#                 return

#             userID = doc.get("User_ID")
#             password = doc.get("Password")
#             self.api_key = doc.get("API_Key")
#             otp_string = doc.get("Totp")
#             totp = pyotp.TOTP(otp_string).now()

#             self.ibt_connect = IBTConnect(params={
#                 "baseurl": self.api_url,
#                 "api_key": self.api_key,
#                 "debug": True
#             })

#             data = await self.user_res_collection.find_one({"data.user_id": self.user_id})
#             contains_today = (datetime.today().strftime("%Y-%b-%d")) in data["data"]["login_time"] if data else False
#             data = None if not contains_today else data

#             logon_response = self.ibt_connect.login(params={
#                 "userId": userID,
#                 "password": password,
#                 "totp": totp,
#                 "data": data
#             })

#             if logon_response.get("data") is not None:
#                 self.tenant_id = logon_response.get("data", {}).get("tenant_id")
#                 self.access_token = logon_response.get("data", {}).get("access_token")
#                 if not contains_today:
#                     update_trade_record = {
#                         "LoginStatus": True,
#                         "UpdatedDate": self.Date
#                     }
#                     await self.user_res_collection.replace_one(
#                         {"data.user_id": self.user_id},
#                         logon_response,
#                         upsert=True
#                     )
#                 self.login_status = True
#                 logger.info("Login Successful")
#             else:
#                 logger.error("Login response contains no data")
#         except Exception as e:
#             logger.error(f"Login failed: {e}")

#     async def fetch_vwap_data(self, tokens, exchange="NSE_EQ", time_period=15):
#         if not self.login_status:
#             logger.error("Cannot fetch VWAP data: Not logged in")
#             return

#         async with aiohttp.ClientSession() as session:
#             tasks = []
#             for token in tokens:
#                 url = f"{self.api_url}/nontransactional/{self.tenant_id}/v1/getVWAPData/{exchange}/{token}/{time_period}"
#                 headers = {
#                     "Authorization": f"Bearer {self.access_token}",
#                     "x-api-key": self.api_key
#                 }
#                 tasks.append(self._fetch_vwap(session, url, headers, token))
            
#             results = await asyncio.gather(*tasks, return_exceptions=True)
#             for token, result in zip(tokens, results):
#                 if isinstance(result, Exception):
#                     logger.error(f"Failed to fetch VWAP for token {token}: {result}")
#                 else:
#                     print(f"\nVWAP Data for Token {token} (Exchange: {exchange}, Time Period: {time_period} minutes):")
#                     print(json.dumps(result, indent=2))

#     async def _fetch_vwap(self, session, url, headers, token):
#         try:
#             async with session.get(url, headers=headers) as response:
#                 if response.status == 200:
#                     data = await response.json()
#                     if data.get("status") == "success":
#                         return data
#                     else:
#                         logger.error(f"API error for token {token}: {data.get('message')}")
#                         return data
#                 else:
#                     error_data = await response.text()  # Get raw response for detailed error
#                     logger.error(f"HTTP error for token {token}: Status {response.status}, Response: {error_data}")
#                     return {"status": "error", "message": f"HTTP {response.status}: {error_data}"}
#         except Exception as e:
#             logger.error(f"Exception fetching VWAP for token {token}: {e}")
#             return e

# if __name__ == "__main__":
#     user_id = "IS21052"  # Replace with your user ID
#     tokens = [14747, 2885, 2799]  # Your tokens

#     async def main():
#         login_obj = Login(user_id)
#         await login_obj.async_init()
#         print("Login Status:", login_obj.login_status)
#         if login_obj.login_status:
#             await login_obj.fetch_vwap_data(tokens, exchange="NSE_EQ", time_period=30)

#     asyncio.run(main())


import asyncio
import pyotp
import pandas as pd
import numpy as np
from pycloudrestapi import IBTConnect 
import logging
from pya3 import *
import motor.motor_asyncio
import aiohttp
from datetime import datetime
import pytz
import logging
logger = logging.getLogger(__name__)

class login():

    def __init__(self, user_id):
        self.mongo_uri = "mongodb+srv://Akash:Akash%405555@stockvertexventures.fxlf1gk.mongodb.net/?tls=true&tlsAllowInvalidCertificates=true" 
    
        self.mongo_client = motor.motor_asyncio.AsyncIOMotorClient(self.mongo_uri)
   
        self.api_url = "https://jri4df7kaa.execute-api.ap-south-1.amazonaws.com/prod/interactive"

        self.timezone = pytz.timezone('Asia/Kolkata')

        self.mongodb_database = self.mongo_client[str("OdinApp")]
        self.user_id = user_id
        self.user_collection = self.mongodb_database['Users_Details'] 
        self.user_res_collection = self.mongodb_database["User_Login_Response"]
        self.Date = datetime.now().strftime("%Y-%m-%d")
    
    async def async_init(self):
        self.login_status=False
        try:
            doc = await self.user_collection.find_one({"User_ID": self.user_id})
        
            if doc:
                
                userID = doc.get("User_ID")
                password = doc.get("Password")
                api_key = doc.get("API_Key")
                otp_string = doc.get("Totp")
                totp = pyotp.TOTP(otp_string).now()
                relogin = doc.get("LoginStatus")

            else:
                print(f"Document not found for User ID...: {self.user_id}")

            ibt_connect = IBTConnect(params={
            "baseurl": self.api_url,
            "api_key": api_key,
            "debug": True })

            
            data = await self.user_res_collection.find_one({"data.user_id": self.user_id})
            
            contains_today = (datetime.today().strftime("%Y-%b-%d")) in data["data"]["login_time"] if data else False  
            data = None if not contains_today else data

            

            logon_response = ibt_connect.login(params={
                "userId": userID,
                "password": password,
                "totp": totp,
                "data":data
            })
            print(logon_response)
            if logon_response.get("data") is not None:
                self.ibt_connect = ibt_connect
                
                self.tenant_id  = logon_response.get("data", {}).get("tenant_id")
                print( self.tenant_id)
    
              
                if not contains_today:
                    update_trade_record = {
                        
                        "LoginStatus":True,
                        "UpdatedDate":self.Date
                    }
                    
                    await self.user_res_collection.replace_one(
                        {"data.user_id": self.user_id},  
                        logon_response,             
                        upsert=True                 
                    )
                
                    result = await self.user_collection.update_one({"_id":doc.get("_id")}, {"$set": update_trade_record})
                self.login_status=True
                logger.info("Login Successful")
        except Exception as e:
            logger.error(f"Login failed. {e}")
import asyncio

if __name__ == "__main__":
    user_id = "IS21052"   # e.g. "ABC123"

    async def main():
        login_obj = login(user_id)
        await login_obj.async_init()
        print("Login Status:", login_obj.login_status)

    asyncio.run(main())