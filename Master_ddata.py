# import aiohttp
# import asyncio
# import json
# import logging
# from motor.motor_asyncio import AsyncIOMotorClient
# from datetime import datetime
# import time

# # Configure the root logger
# logging.basicConfig(
#     level=logging.INFO,
#     format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
#     handlers=[
#         logging.FileHandler('odin_scrip_master.log'),
#         logging.StreamHandler()
#     ]
# )

# class OdinScripMaster:
#     BASE_URL = "https://odinscripmaster.s3.ap-south-1.amazonaws.com/scripfiles/"
#     VERSION_PATHS = {"v1": "", "v2": "v2/"}

#     def __init__(self, segment_code, version, db_name="OdinMasterData"):
#         self.logger = logging.getLogger(self.__class__.__name__)
#         self.mongo_uri = "mongodb://localhost:27017/"
#         self.segment_code = segment_code
#         self.version = version
#         self.url = f"{self.BASE_URL}{self.VERSION_PATHS[version]}{segment_code}.json"
#         self.client = AsyncIOMotorClient(self.mongo_uri)
#         self.db = self.client[db_name]
#         self.collection = self.db["MasterData"]
#         self.metadata_collection = self.db["metadata"]

#     async def fetch_data(self):
#         """Fetch scrip master data from ODIN API asynchronously and transform it."""
#         self.logger.debug(f"Fetching data from URL: {self.url}")
#         try:
#             async with aiohttp.ClientSession() as session:
#                 async with session.get(self.url) as response:
#                     response.raise_for_status()
#                     raw_data = await response.read()
#                     data = json.loads(raw_data.decode("utf-8"))

#                     if isinstance(data, list) and len(data) > 1 and self.version == "v2":
#                         columns = data[0]
#                         records = [dict(zip(columns, row)) for row in data[1:]]
#                         self.logger.debug(f"Fetched {len(records)} records for {self.segment_code}")
#                         return records
#                     elif self.version == "v1":
#                         self.logger.debug(f"Fetched {len(data)} records for {self.segment_code}")
#                         return data
#                     else:
#                         self.logger.warning(f"Unexpected data format for {self.segment_code}")
#                         return None
#         except Exception as e:
#             self.logger.error(f"Error fetching data for {self.segment_code}: {e}")
#             return None

#     async def should_update(self):
#         """Check if data needs to be updated (only once per day)."""
#         metadata = await self.metadata_collection.find_one(
#             {"segment_code": self.segment_code}
#         )
#         today = datetime.now().date()
#         if metadata and "last_updated" in metadata:
#             last_updated_date = metadata["last_updated"].date()
#             if last_updated_date == today:
#                 self.logger.info(f"Data for {self.segment_code} already updated today.")
#                 return False
#         return True

#     async def save_to_mongo(self, collection, delete_flag=False):
#         """Save fetched scrip master data to MongoDB."""
#         if not await self.should_update():
#             return False

#         data = await self.fetch_data()
#         if data:
#             try:
#                 if delete_flag:
#                     # Delete all documents only once
#                     delete_result = await collection.delete_many({})
#                     self.logger.info(
#                         f"Deleted {delete_result.deleted_count} existing documents from MasterData"
#                     )

#                 if data:
#                     insert_result = await collection.insert_many(data)
#                     self.logger.info(
#                         f"Inserted {len(insert_result.inserted_ids)} new documents for {self.segment_code} into MasterData"
#                     )

#                 await self.metadata_collection.update_one(
#                     {"segment_code": self.segment_code},
#                     {"$set": {
#                         "last_updated": datetime.now()
#                     }},
#                     upsert=True,
#                 )
#                 self.logger.info(
#                     f"Data successfully saved to MongoDB collection: MasterData for {self.segment_code}"
#                 )
#                 return True
#             except Exception as e:
#                 self.logger.error(f"Error saving data for {self.segment_code}: {e}")
#                 return False
#         else:
#             self.logger.warning(f"No data to save for {self.segment_code}.")
#             return False

# async def main():
#     start_time = time.perf_counter()

#     segment_codes = ["NSE_EQ", "NSE_FO"]
#     # Create a single MongoDB client to share the collection
#     client = AsyncIOMotorClient("mongodb://localhost:27017/")
#     db = client["OdinMasterData"]
#     collection = db["MasterData"]

#     # Delete existing documents only once, before processing segments
#     delete_flag = True
#     tasks = [
#         OdinScripMaster(segment_code, version="v1").save_to_mongo(collection, delete_flag=(delete_flag and i == 0))
#         for i, segment_code in enumerate(segment_codes)
#     ]
#     results = await asyncio.gather(*tasks)

#     end_time = time.perf_counter()
#     total_time = end_time - start_time
#     logging.info(f"Full execution completed in {total_time:.2f} seconds.")

# if __name__ == "__main__":
#     asyncio.run(main())






