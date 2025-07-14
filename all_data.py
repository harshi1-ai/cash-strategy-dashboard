import aiohttp
import asyncio
import json
import logging
from motor.motor_asyncio import AsyncIOMotorClient
from datetime import datetime

# Configure the root logger
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('odin_scrip_master.log'),
        logging.StreamHandler()
    ]
)

class OdinMasterData:
    """Integrates Odin Scrip Master, Banned Scrips, and Holiday Master data into MongoDB."""
    BASE_URL_SCRIP = "https://odinscripmaster.s3.ap-south-1.amazonaws.com/scripfiles/"
    VERSION_PATHS = {"v1": "", "v2": "v2/"}
    BASE_URL_API = "https://jri4df7kaa.execute-api.ap-south-1.amazonaws.com/prod/interactive/nontransactional/{tenantid}/v1"
    TENANT_ID = "385"
    ACCESS_TOKEN= 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJtZW1iZXJJZCI6OTA5MDkwLCJ1c2VyaWQiOjkwOTA5MCwidGVuYW50aWQiOjkwOTA5MCwibWVtYmVySW5mbyI6eyJ0ZW5hbnRJZCI6IjM4NSIsImdyb3VwSWQiOiJITyIsInVzZXJJZCI6IklTMjEwNTIiLCJ0ZW1wbGF0ZUlkIjoiV0FWRSIsInVkSWQiOiJmYjViNDExMTgzYmM0ZDEyIiwib2NUb2tlbiI6IjB4MDE2OTgzODE1NUJGRDhDNDZBMzY2QUQyQ0E2MDk2IiwidXNlckNvZGUiOiJOWlNBRCIsImdyb3VwQ29kZSI6IkFBQUFBIiwiYXBpa2V5RGF0YSI6eyJzUHVibGlzaGVyQ29kZSI6ImN1IiwiQ3VzdG9tZXJJZCI6IjM4NSIsInNQYXJ0bmVyQXBwSWQiOiIwMUYwMEYiLCJzQXBwbGljYXRpb25Ub2tlbiI6IkluZGlyYVNlY3VyaXRpZXNCMkMxMDcwNDY0ZGVlZiIsIlB1Ymxpc2hlck5hbWUiOiJJbmRpcmEgU2VjdXJpdGllcyBQdnQgTHRkIC0gQjJDIiwiQnJva2VyTmFtZSI6IkluZGlyYSBTZWN1cml0aWVzIFB2dCBMdGQiLCJQcm9kdWN0U291cmNlIjoiIiwiQjJDIjoiWSIsInVzZXJJZCI6IklTMjEwNTIiLCJleHAiOjkyMDY1NTc1MDAsImlhdCI6MTc0MTU5NzUxNH0sInNvdXJjZSI6Ik1PQklMRUFQSSJ9LCJleHAiOjE3NTIxNzIxOTksImlhdCI6MTc1MjEyMjgwMH0.nM4Eq3AmWE1Dzjuj5akBeUcB-0o3CorWmrdeqp42PHU'
    API_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzUHVibGlzaGVyQ29kZSI6ImN1IiwiQ3VzdG9tZXJJZCI6IjM4NSIsInNQYXJ0bmVyQXBwSWQiOiIwMUYwMEYiLCJzQXBwbGljYXRpb25Ub2tlbiI6IkluZGlyYVNlY3VyaXRpZXNCMkMxMDcwNDY0ZGVlZiIsIlB1Ymxpc2hlck5hbWUiOiJJbmRpcmEgU2VjdXJpdGllcyBQdnQgTHRkIC0gQjJDIiwiQnJva2VyTmFtZSI6IkluZGlyYSBTZWN1cml0aWVzIFB2dCBMdGQiLCJQcm9kdWN0U291cmNlIjoiIiwiQjJDIjoiWSIsInVzZXJJZCI6IklTMTgwODkiLCJleHAiOjkyMTQ0MTIxNjAsImlhdCI6MTc0OTQ1MjE3M30.QsnWmQBYiHunBeh9pogNBKYj6AT6mU7i2j2hDB4Z0j4"

    def __init__(self, db_name="OdinMasterData"):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.mongo_uri = "mongodb://localhost:27017/"
        self.client_async = AsyncIOMotorClient(self.mongo_uri)
        self.db_async = self.client_async[db_name]
        self.collections = {
            "MasterData": self.db_async["MasterData"],
            "metadata": self.db_async["metadata"],
            "HolidayMaster": self.db_async["HolidayMaster"],
            "BannedScrips": self.db_async["BannedScrips"]
        }
        self.headers = {
            "Authorization": f"Bearer {self.ACCESS_TOKEN}",
            "x-api-key": self.API_KEY
        }
        self._deleted_collections_this_run = set()

    async def fetch_scrip_master_data(self, segment_code, version):
        """Fetch scrip master data from S3."""
        url = f"{self.BASE_URL_SCRIP}{self.VERSION_PATHS[version]}{segment_code}.json"
        self.logger.debug(f"Fetching data from URL: {url}")
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as response:
                    response.raise_for_status()
                    raw_data = await response.read()
                    data = json.loads(raw_data.decode("utf-8"))
                    if isinstance(data, list) and len(data) > 1 and version == "v2":
                        columns = data[0]
                        records = [dict(zip(columns, row)) for row in data[1:]]
                        self.logger.debug(f"Fetched {len(records)} records for {segment_code}")
                        return records
                    elif version == "v1":
                        self.logger.debug(f"Fetched {len(data)} records for {segment_code}")
                        return data
                    self.logger.warning(f"Unexpected data format for {segment_code}")
        except Exception as e:
            self.logger.error(f"Error fetching data for {segment_code}: {e}")
        return None

    async def should_update(self, segment_code):
        """Check if the segment_code needs to be updated today."""
        metadata = await self.collections["metadata"].find_one({"segment_code": segment_code})
        today = datetime.now().date()
        if metadata and "last_updated" in metadata:
            last_updated_date = metadata["last_updated"].date()
            if last_updated_date == today:
                self.logger.info(f"Data for {segment_code} already updated today.")
                return False
        return True

    async def save_to_mongo(self, data, collection_name, extra_keys=None, skip_delete=False):
        """
        Save data to MongoDB asynchronously. Deletes all documents first, then inserts new data, unless skip_delete is True.
        extra_keys: dict to add to each document.
        """
        collection = self.collections[collection_name]
        # Only delete if not already deleted in this run and skip_delete is False
        if not skip_delete and collection_name not in self._deleted_collections_this_run:
            delete_result = await collection.delete_many({})
            self.logger.info(f"Deleted {delete_result.deleted_count} existing documents from {collection_name}")
            self._deleted_collections_this_run.add(collection_name)
        if not data:
            self.logger.warning(f"No data to save for {collection_name}.")
            return False
        if extra_keys:
            for doc in data:
                doc.update(extra_keys)
        result = await collection.insert_many(data)
        self.logger.info(f"Inserted {len(result.inserted_ids)} documents into {collection_name}")
        return True

    async def update_scrip_master(self, segment_code, version, skip_delete=False):
        """Update scrip master data for a segment asynchronously."""
        if not await self.should_update(segment_code):
            return False
        data = await self.fetch_scrip_master_data(segment_code, version)
        if data:
            await self.save_to_mongo(data, "MasterData", skip_delete=skip_delete)
            await self.collections["metadata"].update_one(
                {"segment_code": segment_code},
                {"$set": {"last_updated": datetime.now()}},
                upsert=True
            )
            self.logger.info(f"Data successfully saved to MongoDB collection: MasterData for {segment_code}")
            return True
        self.logger.warning(f"No data to save for {segment_code}.")
        return False

    async def get_banned_scrips(self, exchange=None, token=None):
        """Fetch banned scrips from API asynchronously and return the data."""
        url = f"{self.BASE_URL_API}/BannedScrips".format(tenantid=self.TENANT_ID)
        params = {}
        if exchange:
            params["exchange"] = exchange
        if token:
            params["token"] = token
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=self.headers, params=params) as response:
                    response.raise_for_status()
                    data = await response.json()
                    if data.get("status") == "success":
                        scrips = data.get("data", [])
                        print(scrips)
                        if scrips:
                            for doc in scrips:
                                doc["exchange"] = exchange if exchange else "ALL"
                                doc["last_modified"] = datetime.now().isoformat()   
                            return scrips
                        self.logger.info("No banned scrips found.")
                    else:
                        self.logger.warning(f"API call failed: {data.get('message')}")
        except Exception as err:
            self.logger.error(f"Request error: {err}")
        return []

    async def get_holiday_master(self, exchange=None, year=None):
        """Fetch holiday master data from API asynchronously and return the data."""
        url = f"{self.BASE_URL_API}/holidaymaster".format(tenantid=self.TENANT_ID)
        params = {}
        if exchange:
            params["exchange"] = exchange
        if year:
            params["year"] = year
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=self.headers, params=params) as response:
                    response.raise_for_status()
                    data = await response.json()
                    if data.get("status") == "success":
                        holiday_data = data.get("data", {})
                        if holiday_data:
                            docs = []
                            for exch_key, dates in holiday_data.items():
                                doc = {
                                    "outer_key": "data",
                                    "sub_key": exch_key,
                                    "values": dates,
                                    "last_updated": datetime.now().isoformat()
                                }
                                docs.append(doc)
                            return docs
                        self.logger.info("No holidays found.")
                    else:
                        self.logger.warning(f"API call failed for {exchange}: {data.get('message')}")
        except Exception as err:
            self.logger.error(f"Request error for {exchange}: {err}")
        return []

    async def process_all(self):
        """Process all data integrations for the day asynchronously."""
        start_time = datetime.now()
        self.logger.info(f"Process started at: {start_time.isoformat()}")
        today = start_time.date()
        all_updated = True
        segment_codes = ["NSE_EQ", "NSE_FO"]
        for segment_code in segment_codes:
            metadata = await self.collections["metadata"].find_one({"segment_code": segment_code})
            if not (metadata and "last_updated" in metadata and metadata["last_updated"].date() == today):
                all_updated = False
                break
        if all_updated:
            self.logger.info("All collections already updated today. Skipping processing.")
            return
        # Gather all MasterData for all segments
        all_masterdata = []
        for segment_code in segment_codes:
            if await self.should_update(segment_code):
                data = await self.fetch_scrip_master_data(segment_code, version="v1")
                if data:
                    all_masterdata.extend(data)
                await self.collections["metadata"].update_one(
                    {"segment_code": segment_code},
                    {"$set": {"last_updated": datetime.now()}},
                    upsert=True
                )
        await self.save_to_mongo(all_masterdata, "MasterData")
        # Gather all BannedScrips for all exchanges
        exchanges = ["NSE_FO"]
        all_bannedscrips = []
        for exch in exchanges:
            scrips = await self.get_banned_scrips(exchange=exch)
            if scrips:
                all_bannedscrips.extend(scrips)
        await self.save_to_mongo(all_bannedscrips, "BannedScrips")
        # Gather all HolidayMaster data for all exchanges
        exchanges = ["NSE_EQ", "NSE_FO"]
        year = datetime.now().year
        all_holidays = []
        for exch in exchanges:
            holiday_docs = await self.get_holiday_master(exchange=exch, year=year)
            if holiday_docs:
                all_holidays.extend(holiday_docs)
        await self.save_to_mongo(all_holidays, "HolidayMaster")
        end_time = datetime.now()
        elapsed = (end_time - start_time).total_seconds()
        self.logger.info(f"Process ended at: {end_time.isoformat()}")
        self.logger.info(f"Total execution time: {elapsed:.2f} seconds")

async def main():
    integrator = OdinMasterData()
    await integrator.process_all()

if __name__ == "__main__":
    asyncio.run(main())



