import pandas as pd
import requests
import logging
import random
import concurrent.futures
import os
# import datetime
import time
from motor.motor_asyncio import AsyncIOMotorClient
import asyncio
from datetime import datetime
from gspread_dataframe import set_with_dataframe
from google.oauth2.service_account import Credentials
import gspread

class StockDataFetcher:

    def __init__(self, mongo_uri, db_name="OdinMasterData"):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.mongo_uri = mongo_uri

        self.client = AsyncIOMotorClient(self.mongo_uri)
        self.db = self.client[db_name]
        
        self.MCapcollection = self.db['McapAPIData']
        self.metadata_collection = self.db["metadata"]
        
        self.no_of_dates = 150
        self.logger = self.setup_logger('stock_data_fetcher.log')
        self.session = requests.Session()  # Use a session to reuse the same connection for all requests
        self.max_retries = 3
        self.timeout = 1000
        self.today_date = datetime.now()
        # self.FScore_path = "D:\Live_Algo_Project\Live_Algo_Project_16Aug2024\OdinStrategyExecutionTesting\Data\Excelfile\F_Score.xlsx"
        self.FScore_path = "D:\OdinStrategyExecutionTesting\Data\Excelfile\F_Score.xlsx"
       
        scope = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"
        ]
        # cradentialsFileJson = "D:\Live_Algo_Project\Live_Algo_Project_16Aug2024\OdinStrategyExecutionTesting\Data\AlgoLiveProjectCredentials.json"
        cradentialsFileJson = "D:\OdinStrategyExecutionTesting\Data\AlgoLiveProjectCredentials.json"
        creds = Credentials.from_service_account_file(cradentialsFileJson, scopes=scope)

        client = gspread.authorize(creds)

        self.spreadsheet = client.open_by_url('https://docs.google.com/spreadsheets/d/1HG3DvsTNNHQzeczEyiaFpNuxPT8oxg_m7ePKyQ6nPmA/edit?gid=936248785#gid=936248785')

        self.PEandNetProfit_Sheet_Name = "PEandNetProfitSheet"
        self.IndicesGradeRange_Sheet_Name = "IndicesGradeRange"

    def setup_logger(self, log_file):
        """
        Set up the logger to log messages to a file and the console.
        """
        logger = logging.getLogger(self.__class__.__name__)
        logger.setLevel(logging.INFO)

        # File handler for logging to a file
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(logging.INFO)

        # Console handler for logging to the console
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)

        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)

        # Add handlers to the logger
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)
        
        return logger

    def fetch_data_with_retry(self, url, retries=3, delay=2):
        for _ in range(retries):
            try:
                response = self.session.get(url, timeout=self.timeout)
                if response.status_code == 200:
                    return response.json()
                if response.status_code == 504:
                    time.sleep(delay * random.uniform(1, 2))
                    delay *= 2
                else:
                    self.logger.error(f"Error fetching data: {response.status_code}")
                    break
            except Exception as e:
                self.logger.error(f"Error during API call: {e}")
                time.sleep(delay)
                delay *= 2
        return None
    
    def fetchcompanymaster(self):
        """
        Fetch stock price data from the external API.
        """
        url = f"https://admin.stocksemoji.com/api/cmot/companymaster"
        data = self.fetch_data_with_retry(url)
        if data and isinstance(data, dict):
            return data.get("data", [])
        else:
            self.logger.error(f"Failed to fetch companymaster.")
        return []
    
    def fetch_stock_data(self, co_code):
        """
        Fetch stock price data from the external API.
        """
        url = f"https://admin.stocksemoji.com/api/exchangePriceHistory/NSE/{co_code}/d/{self.no_of_dates}"
        data = self.fetch_data_with_retry(url)
        if data and isinstance(data, dict) and data.get("success", False):
            return data.get("data", [])
        else:
            pass
            # self.logger.error(f"Failed to fetch stock data for co_code {co_code}.")
        return []

    def fetch_TechScore_data(self, co_code):
        """
        Fetch technical score data from the external API.
        """
        url = f"https://admin.stocksemoji.com/api/getHistoricalTechnicalScores/{co_code}/ALL"
        data = self.fetch_data_with_retry(url)
        if data and isinstance(data, list):
            self.logger.info(f"Successfully fetched technical score data for co_code {co_code}")
            return data
        else:
            self.logger.error(f"Failed to fetch tech score data for co_code {co_code}.")
        return []

    def process_data(self, df, flag_tscore):
        self.logger.info("Processing stock data...")
        merged_data, techscore_dict, nodata_co_codelist = [], {}, []

        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            
            futures = {executor.submit(self.fetch_TechScore_data, row['co_code']): row for _, row in df.iterrows()} if flag_tscore else {}
            futures.update({executor.submit(self.fetch_stock_data, row['co_code']): row for _, row in df.iterrows()})

            for future in concurrent.futures.as_completed(futures):
                row = futures[future]
                co_code = row['co_code']
                result = future.result()
                if 'tscore' in result and flag_tscore:
                    techscore_dict[co_code] = [{'Tscore': item.get('tscore'), 'Symbol': item.get('symbol'), 'Date': item.get('date')} for item in result]
                elif isinstance(result, list) and result:
                    merged_data.extend([{
                         'co_code': co_code, 'Open': data.get('OPEN'), 'High': data.get('HIGH'),
                        'Low': data.get('LOW'), 'Close': data.get('Close'), 'TradeDate': data.get('Date'),
                        'Totalvolume': data.get('Volume'), 'Tradedvalue': data.get('Tradedvalue')
                    } for data in result])
                else:
                    nodata_co_codelist.append(co_code)
        if flag_tscore:
            tscoredf =  pd.DataFrame([{'co_code': co, **entry} for co, entries in techscore_dict.items() for entry in entries])
            tscoredf.to_csv('Data/datafiles/TechScore.csv')
            self.logger.info(f"Successfully processed {len(tscoredf)} rows of Tech score data and make csv.")
        merged_df = pd.DataFrame(merged_data)
        merged_df.to_csv(f'Data/datafiles/StockData{self.today_date.date()}.csv')

        # self.logger.info(f"Successfully processed {len(merged_data)} rows of stock data.")
        # self.logger.info(f"No data found for {len(nodata_co_codelist)} co_codes.")
        # self.logger.info(f"List of co_codes with no data: {nodata_co_codelist}.")
        return merged_df

    def save_to_csv(self, merged_data, filename='merged_price_data.csv'):
        """
        Save merged data to a CSV file.
        """
        # self.logger.info(f"Saving merged data to {filename}")
        merged_data.to_csv(filename, index=False)
        # self.logger.info(f"Data saved to {filename}")



    def run(self):
        if not os.path.exists(f'Data\datafiles\stocksemoji-db.companylist1{self.today_date}.csv'):
            datalist = self.fetchcompanymaster()
            stockdetailsdf = pd.DataFrame(datalist)
            stockdetailsdf = stockdetailsdf.drop_duplicates(subset=['co_code']).sort_values(by=['co_code'])
            stockdetailsdf.to_csv(f'Data\datafiles\stocksemoji-db.companylist1{self.today_date.date()}.csv')
        else:
            stockdetailsdf = pd.read_csv(f'Data\datafiles\stocksemoji-db.companylist1{self.today_date.date()}.csv')
        flag_tscore = False # if you want new Tscore data then true it
        ohlcdata = self.process_data(stockdetailsdf, flag_tscore)
        if not ohlcdata.empty:
            
            ohlcdata['TradeDate'] = pd.to_datetime(ohlcdata['TradeDate'])

            ohlcdata = ohlcdata.sort_values(by=['co_code', 'TradeDate'])

            ohlcdata['Avg30DayTradeValue'] = (
                ohlcdata.groupby('co_code')['Tradedvalue']
                .transform(lambda x: x.rolling(window=30, min_periods=30).mean())
            )
            ohlcdata.to_csv('ohlcdata.csv')

            ohlcdata = (
                ohlcdata.loc[ohlcdata.groupby('co_code')['TradeDate'].idxmax()]
                .reset_index(drop=True)
            )

            # Merge mcap, isin, NSEStatus from stockdetailsdf
            metadata_cols = ['co_code', 'mcap', 'isin', 'NSEStatus', 'nsesymbol']
            ohlcdata = ohlcdata.merge(
                stockdetailsdf[metadata_cols], on='co_code', how='left'
            )

            # Merge FScore data with score renamed to FScore
            if hasattr(self, 'FScore_path') and os.path.exists(self.FScore_path):
                fscore_df = pd.read_excel(self.FScore_path)

                if not fscore_df.empty:
                    ohlcdata = ohlcdata.merge(
                        fscore_df[['co_code', 'score']].rename(columns={'score': 'FScore'}),
                        on='co_code', how='left'
                    )
            worksheet = self.spreadsheet.worksheet(self.PEandNetProfit_Sheet_Name)
            pe_np_records = worksheet.get_all_records()
            pe_np_records_df = pd.DataFrame(pe_np_records)

            if not pe_np_records_df.empty:

                pe_np_records_df = pe_np_records_df.rename(columns={
                        'CD_NSE Symbol': 'nsesymbol',
                        'SC_TTM PE': 'PE',
                        'TTM_TTM PAT Consolidated':'NetProfit'
                    })
                pe_np_records_df['PE'] = pe_np_records_df['PE'].round(2)
                pe_np_records_df['NetProfit'] = pe_np_records_df['NetProfit'].round(2)

                if not pe_np_records_df.empty:
                    ohlcdata = ohlcdata.merge(
                        pe_np_records_df[['nsesymbol', 'PE', 'NetProfit']], 
                        on='nsesymbol', how='left'
                        )        
            
        return ohlcdata
    

    async def HistoricOHLCdata(self):
        collections = await self.db.list_collection_names()
        metadata_exists = 'metadata' in collections

        already_updated = False

        if metadata_exists:
            metadata_doc = await self.metadata_collection.find_one({'segment_code': 'NSE_EQ_HISTORIC'})
            if metadata_doc:
                last_update = metadata_doc.get('last_updated')
                if last_update and last_update.date() == self.today_date.date():
                    self.logger.info("Already updated today for segment_code = NSE_EQ_HISTORIC.")
                    return
            else:
                self.logger.info("No metadata found for segment_code = NSE_EQ_HISTORIC. Will create it later.")
        else:
            self.logger.info("Metadata collection does not exist. Will create it later.")

        self.logger.info("Fetching fresh OHLC data...")
        ohlc_data = self.run()

        if ohlc_data is not None and not ohlc_data.empty:
            # Ensure MCapcollection exists
            if not hasattr(self, 'MCapcollection') or self.MCapcollection is None:
                self.MCapcollection = self.db['MCapcollection']
                self.logger.info("MCapcollection was not initialized. Created from DB.")

            await self.MCapcollection.delete_many({})
            records = ohlc_data.to_dict(orient='records')

            if records:
                await self.MCapcollection.insert_many(records)
                self.logger.info(f"Inserted {len(records)} records into MCapcollection.")

                # ✅ Update metadata after successful insert
                await self.metadata_collection.update_one(
                    {'segment_code': 'NSE_EQ_HISTORIC'},
                    {'$set': {'last_updated': self.today_date, 'segment_code': 'NSE_EQ_HISTORIC'}},
                    upsert=True
                )
                self.logger.info("Metadata updated after successful data insert.")
            else:
                self.logger.warning("No records to insert into MCapcollection.")
        else:
            self.logger.warning("No OHLC data returned from run().")

        
        