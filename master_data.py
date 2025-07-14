import aiohttp
import asyncio
import json
import logging
from motor.motor_asyncio import AsyncIOMotorClient
from datetime import datetime
from pymongo import UpdateOne
from pymongo.errors import BulkWriteError

# Configure the root logger
logging.basicConfig(
    level=logging.INFO,  # Set to DEBUG for more detailed output
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('odin_scrip_master.log'),  # Log to a file
        logging.StreamHandler()  # Also log to the console
    ]
)

class OdinScripMaster:
    BASE_URL = "https://odinscripmaster.s3.ap-south-1.amazonaws.com/scripfiles/"
    VERSION_PATHS = {"v1": "", "v2": "v2/"}

    def __init__(self, segment_code, version="v1", db_name="OdinMasterData"):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.mongo_uri = (
            "mongodb+srv://Akash:Akash%405555@stockvertexventures.fxlf1gk.mongodb.net/"
            "?tls=true&tlsAllowInvalidCertificates=true"
        )
        self.segment_code = segment_code
        self.version = version
        self.url = f"{self.BASE_URL}{self.VERSION_PATHS[version]}{segment_code}.json"
        self.client = AsyncIOMotorClient(self.mongo_uri)
        self.db = self.client[db_name]
        self.collection = self.db[segment_code]
        self.metadata_collection = self.db["metadata"]
        # self.master_collection = self.db["MasterData"]

    async def fetch_data(self):
        """Fetch scrip master data from ODIN API asynchronously and transform it."""
        self.logger.debug(f"Fetching data from URL: {self.url}")
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(self.url) as response:
                    response.raise_for_status()
                    raw_data = await response.read()
                    data = json.loads(raw_data.decode("utf-8"))

                    if isinstance(data, list) and len(data) > 1 and self.version == "v2":
                        columns = data[0]
                        records = [dict(zip(columns, row)) for row in data[1:]]
                        self.logger.debug(f"Fetched {len(records)} records for {self.segment_code}")
                        return records
                    elif self.version == "v1":
                        self.logger.debug(f"Fetched {len(data)} records for {self.segment_code}")
                        return data
                    else:
                        self.logger.warning(f"Unexpected data format for {self.segment_code}")
                        return None
        except Exception as e:
            self.logger.error(f"Error fetching data for {self.segment_code}: {e}")
            return None

    async def should_update(self):
        """Check if data needs to be updated (only once per day)."""
        metadata = await self.metadata_collection.find_one(
            {"segment_code": self.segment_code}
        )
        today = datetime.now().date()
        if metadata and "last_updated" in metadata:
            last_updated_date = metadata["last_updated"].date()
            if last_updated_date == today:
                self.logger.info(f"Data for {self.segment_code} already updated today.")
                return False
        return True

    async def save_to_mongo(self):
        """Save fetched scrip master data to MongoDB only if not updated today."""
        if not await self.should_update():
            return

        data = await self.fetch_data()
        if data:
            try:
                # Create a bulk operations list for update_many
                bulk_ops = []
                for doc in data:
                    if isinstance(doc, dict) and "code" in doc:
                        filter_query = {"code": doc["code"]}
                        update_query = {"$set": doc}
                        bulk_ops.append(UpdateOne(filter_query, update_query, upsert=True))

                if bulk_ops:
                    result = await self.collection.bulk_write(bulk_ops)
                    self.logger.info(
                        f"Bulk write result for {self.segment_code}: "
                        f"inserted={result.upserted_count}, "
                        f"matched={result.matched_count}, "
                        f"modified={result.modified_count}"
                    )

                await self.metadata_collection.update_one(
                    {"segment_code": self.segment_code},
                    {"$set": {"last_updated": datetime.now()}},
                    upsert=True,
                )
                self.logger.info(
                    f"Data successfully saved to MongoDB collection: {self.collection.name}"
                )
            except BulkWriteError as bwe:
                self.logger.error(f"Bulk write error for {self.segment_code}: {bwe.details}")
        else:
            self.logger.warning(f"No data to save for {self.segment_code}.")


class DailyUpdationData:
    def __init__(self):
        routes = {
        "session": "/authentication/v1/user/session",
        "bannedScript":"/nontransactional/{tenantid}/v1/BannedScrips"
    }
async def main():
    """Type of segments: ["NSE_EQ", "NSE_FO", "BSE_EQ", "BSE_FO", "MCX_FO", "NCDEX_FO", "MSE_CUR", "NSE_CUR", "BSE_CUR", "BSE_COMM", "NSE_COMM"]"""

    segment_codes = ["NSE_EQ", "NSE_FO"]
    tasks = [
        OdinScripMaster(segment_code, version="v1").save_to_mongo()
        for segment_code in segment_codes
    ]
    await asyncio.gather(*tasks)

if __name__ == "__main__":
    asyncio.run(main())