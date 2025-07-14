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
import gspread
from gspread_dataframe import set_with_dataframe, get_as_dataframe
from google.oauth2.service_account import Credentials
import concurrent.futures
from positionmanager import PositionManager


class CashStrategy(PositionManager):

    def __init__(self, ClientId, UserId, User_inputDf ):

        super().__init__(UserId, User_inputDf.loc[0,"StrategyID"])
  
        self.user_input_df = User_inputDf
        self.client_id = ClientId
        self.user_id=UserId; self.token_list=[]

        self.Date = datetime.now().strftime("%Y-%m-%d")
        print(self.Date)
        self.updater = DataFrameUpdater()

    async def stats_calculation(self, output_df):

        try:
        
            output_df["LTP"] = pd.to_numeric(output_df["LTP"])
            # output_df["BuyPriceFloat"] = output_df["BuyPrice"].str[0]
            
            output_df["BuyPrice"] = output_df["BuyPrice"].apply(lambda x: x if isinstance(x, list) else [])
            output_df["SellPrice"] = output_df["SellPrice"].apply(lambda x: x if isinstance(x, list) else [])
            output_df["ExQty"] = output_df["ExQty"].apply(lambda x: x if isinstance(x, list) else [])
            output_df["EnQty"] = output_df["EnQty"].apply(lambda x: x if isinstance(x, list) else [])

            # output_df["CapitalUsed"] = output_df["BuyPriceFloat"]*output_df["EnQty"]
            output_df["CapitalUsed"] = output_df.apply(
                lambda row: [p * q for p, q in zip(row["BuyPrice"], row["EnQty"])],
                axis=1
            )
            print("CapitalUsed:", output_df["CapitalUsed"])

            # Convert to NumPy arrays with dtype=object
            sell_prices = np.array(output_df["SellPrice"].tolist(), dtype=object)
            ex_qtys = np.array(output_df["ExQty"].tolist(), dtype=object)
            buy_prices = np.array(output_df["BuyPrice"].tolist(), dtype=object)

            # Compute profit from sell transactions using list comprehension
            # sell_profits = np.array([
            #     sum((np.array(sp) - bp) * np.array(eq)) if sp and eq else 0  
            #     for sp, eq, bp in zip(sell_prices, ex_qtys, buy_prices)
            # ])

            print("buy_prices type:", type(buy_prices))
            print("sell_prices type:", (sell_prices))
            for sp, eq in zip(sell_prices, ex_qtys):
                print("sell turnover type:", (sp), eq)
            for bp, enqty in zip(buy_prices, np.array(output_df["EnQty"].tolist(), dtype=object)):
                print("buy turnover type", bp, enqty)
            
            buy_turnover = sum(np.sum(np.array(bp) * np.array(enqty)) for bp, enqty in zip(buy_prices, np.array(output_df["EnQty"].tolist(), dtype=object)))

            


            # buy_turnover = sum(np.array(bp)*enqty for bp, enqty in zip(buy_prices, np.array(output_df["EnQty"].tolist(), dtype=int)))

            # Sell side turnover (skip None values)
            sell_turnover = sum(np.sum(np.array(sp) * np.array(eq)) for sp, eq in zip(sell_prices, ex_qtys) if sp and eq)

            print("reached here")

            # Total turnover
            total_turnover = buy_turnover + sell_turnover

            print(buy_turnover, sell_turnover)

            output_df["Profit"] = output_df.apply(
                                lambda row: (
                                    (row["LTP"] - np.mean(row["BuyPrice"])) * row["RemainingQty"] +  
                                    sum((np.array(row["SellPrice"]) - np.array(row["BuyPrice"])) * np.array(row["ExQty"]))
                                    if not pd.isna(row["LTP"]) else
                                    sum((np.array(row["SellPrice"]) - np.array(row["BuyPrice"])) * np.array(row["ExQty"]))
                                ),
                                axis=1
                            )
            # Compute final profit based on LTP availability
            # output_df["Profit"] = np.where(
            #     ~output_df["LTP"].isna(),  # If LTP is available
            #     np.where(
            #         output_df["RemainingQty"] == output_df["EnQty"],  
            #         (output_df["LTP"] - output_df["BuyPriceFloat"]) * output_df["RemainingQty"],  
            #         (output_df["LTP"] - output_df["BuyPriceFloat"]) * output_df["RemainingQty"] + sell_profits
            #     ),
            #     sell_profits  # Case: No LTP available
            # )
            profits = output_df["Profit"]

            profit_trades = profits[profits >= 0]
            loss_trades = profits[profits < 0]

            output_df["UnrealisedMTM"] = output_df.apply(
                                lambda row: (
                                    (row["LTP"] - np.mean(row["BuyPrice"])) * row["RemainingQty"]  # Scalar multiplication for RemainingQty
                                    if not pd.isna(row["LTP"]) else 0
                                ),
                                axis=1
                            )
        
        except Exception as e:
            logger.error(f"Error occured while calculation in stats_calculation function, {e}")

        stats = {
            "Investment Amount":self.user_input_df["TotalFund"].iloc[0],
            "number_of_open_pos": output_df["Pos"].value_counts().get("open", 0),
            "number_of_close_pos": output_df["Pos"].value_counts().get("close", 0),
            "Total Profit": profit_trades.sum(),
            "Total Loss": loss_trades.sum(),
            "No. of Profit Positions": len(profit_trades),
            "No. of Loss Positions": len(loss_trades),
            "Avg Profit": profit_trades.mean(),
            "Avg Loss": loss_trades.mean(),
            "Total Trades": len(profits),
            "Profit Factor": abs(profit_trades.sum() / loss_trades.sum()) if loss_trades.sum() != 0 else float("inf"),
            "Capital Utilization":output_df.loc[output_df["Pos"]=="open","CapitalUsed"].sum(),
            "Capital Utilization %":(output_df.loc[output_df["Pos"]=="open","CapitalUsed"].sum()/self.user_input_df["TotalFund"].iloc[0])*100,
            "Turnover":total_turnover

        }
        def format_value(x):
            if isinstance(x, (int, float)):
                if abs(x) >= 0:  # Format large numbers with 2 decimal places and commas
                    return f"{x:,.2f}"
                else:  # Format small numbers in scientific notation
                    return f"{x:.2e}"
            return x 

        # Create DataFrame
        stats_df = pd.DataFrame(
                                [(k, format_value(v)) for k, v in stats.items()],
                                columns=["Metric", "Value"]
                            )
        

        file_path = "CashStrategyOutput.csv"

        file_exists = os.path.isfile(file_path)
        if file_exists:
            last_modified_date = datetime.fromtimestamp(os.path.getmtime(file_path)).date()
            today_date = datetime.today().date()
            
            if last_modified_date == today_date:
                print("File already updated today. Skipping append.")
            else:
                output_df.to_csv(file_path, mode="a", header=False, index=False)
        else:
            output_df.to_csv(file_path, mode="w", header=True, index=False)
       
        return output_df,stats_df
  
    
    async def process_socket_data(self,token):
        """
        Coroutine to process data from the socket asynchronously.
        """

        df=self.updater.df

        if str(token) not in self.token_list:
            # print("inside if")
            res = await self.marketdata_function.send_subscription([{'exchangeSegment': 1, 'exchangeInstrumentID':int(token)}], None)
            if res:
                self.token_list = self.marketdata_function.manage_token_file(token, None)
        try:
            # print(df)

            df_filter=df.loc[df["ExchangeInstrumentID"]==token].reset_index(drop=True)
            if not df_filter.empty:
                
                return df_filter.loc[df_filter.index[-1],"LastTradedPrice"]
            else:
                return None
        except Exception as e:
            print("Exception occured",e)
            return None
    
    
    
    async def execution_func(self):
        
            try:
                output_sheet = self.spreadsheet.worksheet("Output")
                stats_sheet = self.spreadsheet.worksheet("Stats")
                output_df = pd.DataFrame(await self.Position_col.find({"StrategyID":"CST0001"}).to_list(length=None))

                output_df = output_df.drop(columns=["SlType", "SLQty", "Target", "TgtType", "TgtQty"])

                  # Merge and drop original column

                print(output_df)

                # Async function to get LTP data
                async def get_ltp(token):
                    ltp=None;count=0

                    while True:
                        
                        
                        if ltp is not None:
                            
                            return ltp
                            
                        else:
                            
                            await asyncio.sleep(1)
                            
                            if count>=5:
                                ltp = await self.marketdata_function.get_ltp(1, token, None)
                            elif count>10:
                                return None
                            else:
                                ltp =  await self.process_socket_data(token=token)
                                print(ltp)
                            count+=1

                    

                # Create async tasks
                tasks = [get_ltp(row["ExchangeInstrumentID"]) if row["Pos"] == "open" else asyncio.sleep(0, result=None) 
                        for _, row in output_df.iterrows()]

                # Gather results
                results = await asyncio.gather(*tasks)

                print(results)

                # Assign LTP values to DataFrame
                output_df["LTP"] = results

                output_df_updated, stats_df = await self.stats_calculation(output_df)
                

                list_columns = ["SellPrice", "ExDTime", "ExStatus", "ExOrderId", "ExOrderSide", "ExQty",
                                "TargetMon", "TargetQtr", "TargetWeek"]

                # Expand each list column into separate columns
                for col in list_columns:
                    df_expanded = pd.DataFrame(output_df_updated[col].tolist())  # Convert list values into separate columns
                    df_expanded.columns = [f"{col}_{i+1}" for i in range(df_expanded.shape[1])]  # Rename columns
                    output_df_updated = pd.concat([output_df_updated.drop(columns=[col]), df_expanded], axis=1)

                # Write the DataFrame to the worksheet
                output_df_updated = output_df_updated.applymap(lambda x: int(x) if isinstance(x, np.int64) else x)
                stats_df = stats_df.applymap(lambda x: int(x) if isinstance(x, np.int64) else x)
                print(output_df_updated["TargetMon_1"], stats_df)
                output_df_updated = output_df_updated.astype(str)
                stats_df = stats_df.astype(str)

                set_with_dataframe(output_sheet, output_df_updated, include_index=False, resize=True)
                set_with_dataframe(stats_sheet, stats_df, include_index=False, resize=True)
            except Exception as e:
                logger.error(f"Exeption occured while sending output, {e}")

   
   