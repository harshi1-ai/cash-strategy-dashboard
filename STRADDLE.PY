import pandas as pd
import numpy as np
from gspread_dataframe import set_with_dataframe
from google.oauth2.service_account import Credentials
import gspread
from pya3 import *
from datetime import *
import pymongo
from nsepython import *
from OdinFunctions import *
from ApiCashDataUpdate import *
from PositionManagerFunc import *
from master_data import OdinScripMaster
from ApiCashDataUpdate import StockDataFetcher
import logging
import calendar
from typing import List, Dict
logger = logging.getLogger(__name__)
import concurrent.futures

mongo_uri = "mongodb+srv://Akash:Akash%405555@stockvertexventures.fxlf1gk.mongodb.net/?tls=true&tlsAllowInvalidCertificates=true"  

  
class StraddleSL(PositionManager):

    def __init__(self, ClientId, UserId, User_inputDf ):

        super().__init__(UserId, User_inputDf.loc[0,"StrategyID"])
        print(User_inputDf.loc[0,"StrategyID"])
        self.user_input_df = User_inputDf
        self.client_id = ClientId
        self.user_id=UserId
        self.algo_id = "ABC"
        self.Date = datetime.now().strftime("%Y-%m-%d")
        self.updater = DataFrameUpdater()
        self.execution = "papertrade"
        self.fut_series = "FUTIDX"
        self.opt_series = "OPTIDX"
        self.token_list = []
    
    async def process_socket_data(self,token:int=None, token_list:list=[]):
        """
        Coroutine to process data from the socket asynchronously.
        """
        
        
        if token is not None and str(token) not in self.token_list:
            self.token_list = await self.odin_function.manage_token_file([token])

        elif token_list is not None:

            new_tokens = [t for t in token_list if str(t) not in self.token_list]
            if new_tokens:
                self.token_list = await self.odin_function.manage_token_file(new_tokens)

            # self.token_list = await self.odin_function.manage_token_file(token_list)
        else:
            logger.error("Token required to get current price returning None")
            return pd.DataFrame()
        try:
            df = self.updater.get_df_copy()
            # print(df)

            if df.empty:
                self.updater.ready_event.wait(timeout=5)
            
            if token is not None:
                df_filter = df.loc[df["ExchangeInstrumentID"] == token].reset_index(drop=True)
                if df_filter.empty:
                    return None
                return df_filter.loc[df_filter.index[-1],"LastTradedPrice"]
            else:
                token_list = list(map(int, token_list))

                # print(token_list)
                
                
                df_filter = df.loc[df["ExchangeInstrumentID"].isin(token_list)].reset_index(drop=True) 
                
                if df_filter.empty:
                    return None
                return df_filter         
  
        except Exception as e:
            print("Exception occured",e)
            return None
        
    
    async def date_filter_pipeline(self, filter_date: str, filter_list: List[Dict]) -> List[Dict]:
        """
        Create a MongoDB aggregation pipeline to filter documents by date and conditions.
        """
        if not filter_list:
            logger.warning("Empty filter_list provided, returning empty pipeline")
            return []

        try:
            filter_date = pd.to_datetime(filter_date).strftime("%Y-%m-%d")
        except ValueError:
            logger.error(f"Invalid filter_date: {filter_date}")
            return []

        pipeline = [
            {
                '$match': {
                    "$and": filter_list + [
                        {
                            "$or": [
                                {"ExDTime": {"$elemMatch": {"$regex": f"^{filter_date}"}}},
                                {"ExDTime": {"$exists": False}},
                                {"ExDTime": []}
                            ]
                        }
                    ]
                }
            }
        ]
        return pipeline
    
    async def safe_int_convert(self, value) -> int:
        """Safely convert value to int, handling lists and None values."""
        if value is None:
            return 0
        if isinstance(value, list):
            return int(value[0]) if value else 0
        try:
            return int(value)
        except (ValueError, TypeError):
            logger.warning(f"Cannot convert {value} to int, returning 0")
            return 0
    
    async def Executionfunc(self, user_input):


        orders_list=[]; open_pos_df=pd.DataFrame(); close_pos_df=pd.DataFrame();self.token_list=[];entry=False
        ce_stoploss = None
        pe_stoploss = None
        ce_entry_price = None
        pe_entry_price = None
        indicator = f'{user_input["Symbol"]}StraddleSL'
        identifier = f'{self.client_id}{self.user_id}{user_input["StrategyID"]}{indicator}'
        
        try:

            logger.info(f"Code Start for Live Strategy {identifier}")
            print(identifier)
            
            Future_exp = pd.to_datetime(await self.odin_function.get_details(key="exp", Exchangeid=None, Exchange='NSE_FO', Instrument=self.fut_series, Symbol=user_input["Symbol"],expiry_idx=0)).strftime("%d%b%Y").upper()
            print("Future_exp", Future_exp)
            FToken = await self.odin_function.get_details(key="code", Exchangeid=None, Exchange="NSE_FO", Instrument=self.fut_series, Symbol=user_input["Symbol"], expiry_idx=0)
            print("FToken", FToken)
            option_exp_idx = 0 if user_input["OptionExpiry"] in ["Weekly"] or user_input["StrategyType"] == "Intraday" else 1 if user_input["OptionExpiry"] == "NextWeek" else None if user_input["OptionExpiry"] == "Monthly" else 0
            print("option_exp_idx", option_exp_idx)
            if option_exp_idx is not None:

                option_exp_ce = await self.odin_function.get_details(key="exp", Exchangeid=None, Exchange='NSE_FO', Instrument=self.opt_series,Symbol=user_input["Symbol"], Option="CE", expiry_idx=option_exp_idx)
                option_exp_date_ce = pd.to_datetime(option_exp_ce).strftime("%d%b%Y").upper() if option_exp_ce else Future_exp
                option_exp_pe = await self.odin_function.get_details(key="exp", Exchangeid=None, Exchange='NSE_FO', Instrument=self.opt_series,Symbol=user_input["Symbol"], Option="PE", expiry_idx=option_exp_idx)
                option_exp_date_pe = pd.to_datetime(option_exp_pe).strftime("%d%b%Y").upper() if option_exp_pe else Future_exp

            print("option_exp_date_ce:" ,option_exp_date_ce , "option_exp_date_pe :" ,option_exp_date_pe)    
            self.Rejection_col.delete_many(filter={"StrategyID": user_input["StrategyID"]})
            open_pos_df = pd.DataFrame(await self.Position_col.find({"$and": [{"StrategyID":user_input["StrategyID"]}, {"Identifier":identifier}, {"Pos":"open"}, {"Symbol":user_input["Symbol"]}]}).to_list(length=None))

            if not open_pos_df.empty:
                ce_df = open_pos_df[open_pos_df['Option'] == 'CE']
                pe_df = open_pos_df[open_pos_df['Option'] == 'PE']
                if not ce_df.empty:
                    ce_stoploss = ce_df.iloc[0].get("StopLoss", None)
                    ce_entry_price = ce_df.iloc[0]["SellPrice"][0] if ce_df.iloc[0]["SellPrice"] else None
                if not pe_df.empty:
                    pe_stoploss = pe_df.iloc[0].get("StopLoss", None)
                    pe_entry_price = pe_df.iloc[0]["SellPrice"][0] if pe_df.iloc[0]["SellPrice"] else None
                entry = True


            while True: 

                Time = datetime.now().strftime("%H:%M:%S")

                print(Time, end='\r')

                if Time<user_input["EntryTime"]:
                
                    continue
                

                if Time>=user_input["EntryTime"] and Time<user_input["ExitTime"]:
                    
                    rejected_order = pd.DataFrame(await self.Rejection_col.find({"$and": [{"StrategyID":user_input["StrategyID"]}, {"Date": self.Date}]}).to_list(length=None))

                    if not rejected_order.empty:
                        break

                    open_pos_df = pd.DataFrame(await self.Position_col.find({"$and": [{"StrategyID":user_input["StrategyID"]}, {"Identifier":identifier}, {"Pos":"open"}, {"Symbol":user_input["Symbol"]}]}).to_list(length=None))

                    if open_pos_df.empty and not entry:
                        # print("Inside if")

                        close_pos_df = pd.DataFrame(await self.Position_col.aggregate(await (self.date_filter_pipeline(self.Date, [{"Identifier":identifier},{"Pos":"close"}]))).to_list(length=None))
                        # print(close_pos_df)

                        if not close_pos_df.empty:
                            # print("close pos df=",close_pos_df)
                            print("Position already closed for today")
                            break


                        try:
                            Fprice = await self.process_socket_data(token=int(FToken))
                            
                            # print("Fprice" ,Fprice)
                            strike = round(Fprice//100)*100 if Fprice is not None else None
                            # strike = 23000
                            # print("strike" ,strike)

                            ce_token = await self.odin_function.get_details(key="code",Exchange="NSE_FO",Instrument=self.opt_series,Symbol=user_input["Symbol"].upper(),Option="CE",Strike=float(strike),expiry_idx=int(option_exp_idx))
                            pe_token = await self.odin_function.get_details(key="code",Exchange="NSE_FO",Instrument=self.opt_series,Symbol=user_input["Symbol"].upper(),Option="PE",Strike=float(strike),expiry_idx=int(option_exp_idx))
                            
                            if ce_token is None or pe_token is None:
                                continue
                        except:
                            continue

                        try:

                            CePrice = await self.process_socket_data(token=ce_token)

                            PePrice = await self.process_socket_data(token=pe_token)

                            # print(ce_token, CePrice, pe_token, pePrice)
                            # print("ce_token:", ce_token,"CePrice:",CePrice, "pe_token:",pe_token,"pePrice:",PePrice)

                            if (CePrice is None) or (PePrice is None):
                                continue
                            
                            # elif CePrice <= 10 or pePrice <= 10:
                            #     print(f"Premium of any one option is less than 10 CE premium = {CePrice} PE premium = {pePrice}")
                            #     break

                        except:
                            continue


                        orders_list = []

                                
                        orders_list = [
                            {"Exchange": "NSE_FO","Script": ce_token,"Transaction": "SELL","Symbol": user_input["Symbol"].upper(),"Series": self.fut_series,"ExpiryDT": option_exp_date_ce,"Option": "CE","Strike": float(strike),"Price": float(CePrice),"OrderLots": int(user_input["Lots"]),"Identifier": identifier},
                            {"Exchange": "NSE_FO","Script": pe_token,"Transaction": "SELL","Symbol": user_input["Symbol"].upper(),"Series": self.fut_series,"ExpiryDT": option_exp_date_pe,"Option": "PE","Strike": float(strike),"Price": float(PePrice),"OrderLots": int(user_input["Lots"]),"Identifier": identifier}
                        ]
                        # print("orders_list:" ,orders_list )

                        pos_flag = await self.create_position(orders_list, "open", self.algo_id, execution_flag=self.execution)
             
                        if pos_flag:
                            open_pos_df = pd.DataFrame(await self.Position_col.find({"Identifier": identifier, "Pos": "open"}).to_list(length=None))
                            if not open_pos_df.empty:
                                ce_df = open_pos_df[open_pos_df['Option'] == 'CE'].reset_index(drop=True)
                                pe_df = open_pos_df[open_pos_df['Option'] == 'PE'].reset_index(drop=True)
                                if not ce_df.empty:
                                    ce_entry_price = ce_df.iloc[0]["SellPrice"][0]
                                    ce_stoploss = ce_entry_price * (1 + user_input["StopLoss"] * 0.01)
                                    print("ce_stoploss",ce_stoploss)
                                    await self.Position_col.update_one({"StrategyID": self.StrategyID,"ExchangeInstrumentID": int(ce_df.iloc[0]["ExchangeInstrumentID"]),"Pos": "open","Identifier": identifier},{"$set": {"StopLoss": float(ce_stoploss)}})
                                if not pe_df.empty:
                                    pe_entry_price = pe_df.iloc[0]["SellPrice"][0]
                                    pe_stoploss = pe_entry_price * (1 + user_input["StopLoss"] * 0.01)
                                    print("pe_stoploss",pe_stoploss)
                                    await self.Position_col.update_one({"StrategyID": self.StrategyID,"ExchangeInstrumentID": int(pe_df.iloc[0]["ExchangeInstrumentID"]),"Pos": "open","Identifier": identifier},{"$set": {"StopLoss": float(pe_stoploss)}})
                                print("Position created\n", open_pos_df)
                                entry = True
                                logger.info(f"Position created in Database for {identifier}")
                                
                    if not open_pos_df.empty:
                        pe_df = open_pos_df[open_pos_df['Option'] == 'PE'].reset_index(drop=True)
                        ce_df = open_pos_df[open_pos_df['Option'] == 'CE'].reset_index(drop=True)
                        # print("pe_df" ,pe_df  ,"ce_df" , ce_df)
                        
                        if not pe_df.empty and  pe_stoploss is not None:
                            ltp_pe = await self.process_socket_data(token=int(pe_df.loc[0, 'ExchangeInstrumentID']))
                            # logger.info(f"ltp_pe {ltp_pe} Less Than pe_stoploss {pe_stoploss}")
                            if ltp_pe is None:
                                logger.warning(f"No LTP for PE token {pe_df.loc[0, 'ExchangeInstrumentID']}")
                                continue
                            
                            
                            elif ltp_pe >= pe_stoploss:
                               
                                print(f"ltp_pe {ltp_pe} >= pe_stoploss {pe_stoploss}")
                                orders_list = [{
                                    "Exchange": "NSE_FO",
                                    "Script": pe_df.loc[0, "ExchangeInstrumentID"],
                                    "OrderLots": pe_df.loc[0, "RemainingQty"],
                                    "Transaction": "BUY",
                                    "Symbol": pe_df.loc[0, "Symbol"],
                                    "UpdatedIds": [pe_df.loc[0, "_id"]],
                                    "Identifier": identifier,
                                    "Price": float(ltp_pe)
                                }]
                                close_flag = await self.close_position(orders_list, "close", algo_id=self.algo_id, execution_flag=self.execution)
                                if close_flag:
                                    print("close_flag pe stoploss hit ")
                                    pe_remaining_qty = self.safe_int_convert(pe_df.loc[0, "RemainingQty"])
                                    pe_en_qty = self.safe_int_convert(pe_df.loc[0, "EnQty"])
                                    new_pe_remaining_qty = max(pe_remaining_qty - pe_en_qty, 0)
                                    await self.Position_col.update_one({"StrategyID": self.StrategyID,"ExchangeInstrumentID": int(pe_df.loc[0, "ExchangeInstrumentID"]),"Identifier": identifier,"Option": "PE","EnOrderSide": "SELL"},{"$set": {"RemainingQty": (new_pe_remaining_qty)}})
                                    print("PE Position Closed")
                                    logger.info(f"PE Position closed for {identifier}")
                                    
                                    remaining_open_pos = pd.DataFrame(await self.Position_col.find({"$and": [{"StrategyID": user_input["StrategyID"]},{"Identifier": identifier},{"EnOrderSide": "SELL"},{"Pos": "open"},]}).to_list(length=None))

                                    if not remaining_open_pos.empty and not ce_df.empty and ce_entry_price:
                                        ce_stoploss = ce_entry_price  # Update the variable
                                        await self.Position_col.update_one({"StrategyID": self.StrategyID,"ExchangeInstrumentID": int(ce_df.loc[0, "ExchangeInstrumentID"]),"Identifier": identifier,"Option": "CE","EnOrderSide": "SELL"},{"$set": {"StopLoss": float(ce_entry_price)}})
                                        print(f"CE StopLoss updated to cost-to-cost: {ce_entry_price}")
                                        logger.info(f"CE StopLoss set to cost-to-cost for {identifier}")
                                    else:
                                        entry = False
                        
                        if not ce_df.empty and ce_stoploss is not None:
                            ltp_ce = await self.process_socket_data(token=int(ce_df.loc[0, 'ExchangeInstrumentID']))
                            # logger.info(f"ltp_ce {ltp_ce} Less Than ce_stoploss {ce_stoploss}")
                            if ltp_ce is None:
                                logger.warning(f"No LTP for CE token {ce_df.loc[0, 'ExchangeInstrumentID']}")
                                continue
                            elif ltp_ce >= ce_stoploss:
                                print(f"ltp_ce {ltp_ce} >= ce_stoploss {ce_stoploss}")
                                orders_list = [{"Exchange": "NSE_FO","Script": ce_df.loc[0, "ExchangeInstrumentID"],"OrderLots": ce_df.loc[0, "RemainingQty"],"Transaction": "BUY","Symbol": ce_df.loc[0, "Symbol"],"UpdatedIds": [ce_df.loc[0, "_id"]],"Identifier": identifier,"Price": float(ltp_ce)}]
                                close_flag = await self.close_position(orders_list, "close", algo_id=self.algo_id, execution_flag=self.execution)
                                if close_flag:
                                    print("close_flag ce stoploss hit ")

                                    ce_remaining_qty = self.safe_int_convert(ce_df.loc[0, "RemainingQty"])
                                    ce_en_qty = self.safe_int_convert(ce_df.loc[0, "EnQty"])
                                    new_ce_remaining_qty = max(ce_remaining_qty - ce_en_qty, 0)
                                    await self.Position_col.update_one({"StrategyID": self.StrategyID,"ExchangeInstrumentID": int(ce_df.loc[0, "ExchangeInstrumentID"]),"Identifier": identifier,"Option": "CE","EnOrderSide": "SELL"},{"$set": {"RemainingQty": (new_ce_remaining_qty)}})
                                    print("CE Position Closed")
                                    logger.info(f"CE Position closed for {identifier}")
                                    
                                    remaining_open_pos = pd.DataFrame(await self.Position_col.find({"$and": [{"StrategyID": user_input["StrategyID"]},{"Identifier": identifier},{"EnOrderSide": "SELL"},{"Pos": "open"}]}).to_list(length=None))

                                    if not remaining_open_pos.empty and not pe_df.empty and pe_entry_price:
                                        pe_stoploss = pe_entry_price  # Update the variable
                                        await self.Position_col.update_one({"StrategyID": self.StrategyID,"ExchangeInstrumentID": int(pe_df.loc[0, "ExchangeInstrumentID"]),"Identifier": identifier,"Option": "PE","EnOrderSide": "SELL"},{"$set": {"StopLoss": float(pe_entry_price)}})
                                        print(f"PE StopLoss updated to cost-to-cost: {pe_entry_price}")
                                        logger.info(f"PE StopLoss set to cost-to-cost for {identifier}")
                                    else:
                                        entry = False
                                    

                if Time >= user_input["ExitTime"]:
                    
                    logger.info(f"Exit time reached for {user_input['Symbol']}")
                    open_pos_df = pd.DataFrame(await self.Position_col.find({"$and": [{"StrategyID": user_input["StrategyID"]},{"Pos": "open"},{"Identifier": identifier},{"Symbol": user_input["Symbol"]}]}).to_list(length=None))
                    print(open_pos_df)
                    if not open_pos_df.empty:
                        logger.info(f"Closing positions at exit time: {open_pos_df}")
                        orders_list = []
                        for i in range(len(open_pos_df)):
                            ltp = await self.process_socket_data(token=int(open_pos_df.loc[i, "ExchangeInstrumentID"]))
                            if ltp is None:
                                logger.warning(f"No LTP available for token {open_pos_df.loc[i, 'ExchangeInstrumentID']}, skipping order")
                                continue
                            order_side = "BUY" if open_pos_df.loc[i, "EnOrderSide"][0] == "SELL" else "SELL"
                            
                            # print("________________________________________", open_pos_df.loc[i, "EnOrderSide"])
                            orders_list.append({
                                "Exchange": "NSE_FO",
                                "Script": open_pos_df.loc[i, "ExchangeInstrumentID"],
                                "OrderLots": open_pos_df.loc[i, "RemainingQty"],
                                "Transaction": order_side, 
                                "Symbol": open_pos_df.loc[i, "Symbol"],
                                "Series": open_pos_df.loc[i, "Series"],
                                "ExpiryDT": open_pos_df.loc[i, "ExpiryDT"],
                                "Option": open_pos_df.loc[i, "Option"],
                                "Strike": open_pos_df.loc[i, "Strike"],
                                "UpdatedIds": [open_pos_df.loc[i, "_id"]],
                                "Identifier": identifier,
                                "Price": float(ltp)
                            })
                        
                        if orders_list:
                            print(orders_list)
                            close_flag = await self.close_position(orders_list, "close", algo_id=self.algo_id, execution_flag=self.execution)
                            if close_flag:
                                for i in range(len(open_pos_df)):
                                    # Update remaining quantity to 0
                                    await self.Position_col.update_many(
                                        {
                                            "StrategyID": self.StrategyID,
                                            "RemainingQty": {"$ne": 0},
                                            "Identifier": identifier,
                                            "ExchangeInstrumentID": int(open_pos_df.loc[i, "ExchangeInstrumentID"])
                                            
                                        },
                                        {"$set": {"RemainingQty": 0}}
                                    )
                                
                                # Check if all positions are closed
                                remaining_open_pos = pd.DataFrame(await self.Position_col.find({
                                    "$and": [
                                        {"StrategyID": user_input["StrategyID"]}, 
                                        {"Identifier": identifier}, 
                                        {"Symbol": user_input["Symbol"]},
                                        {"Pos": "open"}, 
                                        
                                    ]
                                }).sort("_id", 1).to_list(length=None))
                                
                                if remaining_open_pos.empty:
                                    print('All positions closed successfully')
                                    logger.info(f"All positions closed for {identifier}")
                                else:
                                    print(f'Warning: {len(remaining_open_pos)} positions still open in database')
                                    logger.warning(f"Failed to close all positions for {identifier}")
                                logger.info(f"All positions closed at exit time for {identifier}")
                            else:
                                logger.warning(f"Failed to close positions for {identifier}")
                        else:
                            logger.warning(f"No valid orders to close for {identifier}")
                    print('Time END')
                    logger.info(f"Code Stop for Live {identifier}")
                    break

                await asyncio.sleep(0.1)
        except Exception as e:
            logger.error(f"Live Strategy {identifier} Stopped with an Error: {e}")
            print(f"Error in execution function: {e}")
            raise
    
    async def execution_func(self):
        """
        This coroutine executes all strategies concurrently and processes socket data.
        """
        
        # Create tasks for strategy execution
        strategy_tasks = [
            self.Executionfunc(self.user_input_df.loc[i])
            for i in range(len(self.user_input_df))
        ]

        # Run all tasks concurrently
        await asyncio.gather(*strategy_tasks)
       
    def run(self):
        """
        Entry point to start the concurrent execution."""
        with concurrent.futures.ThreadPoolExecutor() as executor:
        # Run the blocking Redis consumption function in a separate thread
            
            executor.submit(self.updater.consume_and_update)

            # Now run the asyncio event loop for other async tasks
            loop = asyncio.get_event_loop()
            loop.run_until_complete(self.execution_func())
        # updater = DataFrameUpdater()
     