import asyncio
import pandas as pd
import numpy as np
import motor.motor_asyncio
import logging
from datetime import datetime
import pytz
import os
from threading import Lock

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class CashStrategy:
   

    def __init__(self, client_id: str, user_id: str, strategy_id: str, total_fund: float, mongo_uri: str = "mongodb://localhost:27017"):
       
        self.client_id = client_id
        self.user_id = user_id
        self.strategy_id = strategy_id
        self.total_fund = total_fund
        self.date = datetime.now(pytz.timezone("Asia/Kolkata")).strftime("%Y-%m-%d")
        self.token_list = []
        self.file_lock = Lock()

        # Initialize MongoDB client
        self.mongo_client = motor.motor_asyncio.AsyncIOMotorClient(mongo_uri)
        self.db = self.mongo_client["Strategy"]  # Database name
        self.position_col = self.db["Position_col"]  # Collection name

        logger.info(f"Initialized CashStrategy for StrategyID: {self.strategy_id}")

    async def fetch_ltp(self, token: str) -> float:
       
        try:
            if token not in self.token_list:
                logger.info(f"Subscribing to token: {token}")
                self.token_list.append(token)
                # Simulate subscription to market data (replace with actual API call if available)
                await asyncio.sleep(0.1)  # Simulate async operation

            # Simulate LTP retrieval (replace with actual market data API/WebSocket)
            ltp = 100.0 + float(token) % 10  # Dummy LTP based on token
            logger.debug(f"Fetched LTP {ltp} for token {token}")
            return ltp
        except Exception as e:
            logger.error(f"Error fetching LTP for token {token}: {e}")
            return None

    async def stats_calculation(self, output_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
        
        try:
            # Ensure numeric LTP
            output_df["LTP"] = pd.to_numeric(output_df["LTP"], errors="coerce")

            # Validate list columns
            list_columns = ["BuyPrice", "SellPrice", "ExQty", "EnQty"]
            for col in list_columns:
                output_df[col] = output_df[col].apply(lambda x: x if isinstance(x, list) else [])

            # Calculate Capital Used
            output_df["CapitalUsed"] = output_df.apply(
                lambda row: sum(p * q for p, q in zip(row["BuyPrice"], row["EnQty"]) if p and q),
                axis=1
            )

            # Calculate Turnover
            buy_turnover = sum(
                sum(p * q for p, q in zip(row["BuyPrice"], row["EnQty"]) if p and q)
                for _, row in output_df.iterrows()
            )
            sell_turnover = sum(
                sum(p * q for p, q in zip(row["SellPrice"], row["ExQty"]) if p and q)
                for _, row in output_df.iterrows()
            )
            total_turnover = buy_turnover + sell_turnover

            # Calculate Profit
            output_df["Profit"] = output_df.apply(
                lambda row: (
                    (row["LTP"] - (np.mean(row["BuyPrice"]) if row["BuyPrice"] else 0)) * row["RemainingQty"]
                    + sum((p_s - p_b) * q for p_s, p_b, q in zip(row["SellPrice"], row["BuyPrice"], row["ExQty"]) if p_s and p_b and q)
                    if not pd.isna(row["LTP"])
                    else sum((p_s - p_b) * q for p_s, p_b, q in zip(row["SellPrice"], row["BuyPrice"], row["ExQty"]) if p_s and p_b and q)
                ),
                axis=1
            )

            # Calculate Unrealized MTM
            output_df["UnrealisedMTM"] = output_df.apply(
                lambda row: (
                    (row["LTP"] - (np.mean(row["BuyPrice"]) if row["BuyPrice"] else 0)) * row["RemainingQty"]
                    if not pd.isna(row["LTP"]) else 0
                ),
                axis=1
            )

            # Compute stats
            profits = output_df["Profit"]
            profit_trades = profits[profits >= 0]
            loss_trades = profits[profits < 0]

            stats = {
                "Investment Amount": self.total_fund,
                "number_of_open_pos": output_df["Pos"].value_counts().get("open", 0),
                "number_of_close_pos": output_df["Pos"].value_counts().get("close", 0),
                "Total Profit": profit_trades.sum(),
                "Total Loss": loss_trades.sum(),
                "No. of Profit Positions": len(profit_trades),
                "No. of Loss Positions": len(loss_trades),
                "Avg Profit": profit_trades.mean() if len(profit_trades) > 0 else 0,
                "Avg Loss": loss_trades.mean() if len(loss_trades) > 0 else 0,
                "Total Trades": len(profits),
                "Profit Factor": abs(profit_trades.sum() / loss_trades.sum()) if loss_trades.sum() != 0 else float("inf"),
                "Capital Utilization": output_df.loc[output_df["Pos"] == "open", "CapitalUsed"].sum(),
                "Capital Utilization %": (output_df.loc[output_df["Pos"] == "open", "CapitalUsed"].sum() / self.total_fund * 100),
                "Turnover": total_turnover
            }

            # Format stats
            def format_value(x):
                if isinstance(x, (int, float)) and not pd.isna(x):
                    return f"{x:,.2f}"
                return x

            stats_df = pd.DataFrame([(k, format_value(v)) for k, v in stats.items()], columns=["Metric", "Value"])

            # Save to CSV
            file_path = f"CashStrategyOutput_{self.strategy_id}.csv"
            with self.file_lock:
                file_exists = os.path.isfile(file_path)
                if file_exists:
                    last_modified_date = datetime.fromtimestamp(os.path.getmtime(file_path)).date()
                    today_date = datetime.today().date()
                    if last_modified_date == today_date:
                        logger.info("File already updated today. Skipping append.")
                    else:
                        output_df.to_csv(file_path, mode="a", header=False, index=False)
                else:
                    output_df.to_csv(file_path, mode="w", header=True, index=False)

            return output_df, stats_df

        except Exception as e:
            logger.error(f"Error in stats_calculation: {e}")
            return output_df, pd.DataFrame()

    async def execution_func(self):
        """
        Execute the trading strategy: fetch positions, get LTPs, calculate stats, and save to CSV.
        """
        try:
            # Fetch position data from MongoDB
            logger.info(f"Fetching positions for StrategyID: {self.strategy_id}")
            output_df = pd.DataFrame(await self.position_col.find({"StrategyID": self.strategy_id}).to_list(length=None))

            if output_df.empty:
                logger.warning(f"No positions found for StrategyID: {self.strategy_id}")
                return

            # Define expected columns
            expected_columns = ["ExchangeInstrumentID", "Pos", "BuyPrice", "SellPrice", "EnQty", "ExQty", "RemainingQty"]
            for col in expected_columns:
                if col not in output_df.columns:
                    output_df[col] = [[] if col in ["BuyPrice", "SellPrice", "EnQty", "ExQty"] else 0 for _ in range(len(output_df))]

            # Fetch LTPs for open positions
            async def get_ltp(token: str) -> float:
                for attempt in range(5):
                    ltp = await self.fetch_ltp(token)
                    if ltp is not None:
                        return ltp
                    await asyncio.sleep(1)
                logger.warning(f"Failed to fetch LTP for token {token} after 5 attempts")
                return None

            tasks = [
                get_ltp(row["ExchangeInstrumentID"]) if row["Pos"] == "open" else asyncio.sleep(0, result=None)
                for _, row in output_df.iterrows()
            ]
            output_df["LTP"] = await asyncio.gather(*tasks)

            # Calculate statistics
            output_df, stats_df = await self.stats_calculation(output_df)

            # Save stats to a separate CSV
            stats_file_path = f"CashStrategyStats_{self.strategy_id}.csv"
            with self.file_lock:
                stats_df.to_csv(stats_file_path, mode="w", header=True, index=False)
                logger.info(f"Stats saved to {stats_file_path}")

            logger.info("Strategy execution completed successfully")

        except Exception as e:
            logger.error(f"Error in execute_strategy: {e}")

    async def close(self):
        """Close MongoDB client."""
        self.mongo_client.close()
        logger.info("MongoDB client closed")

async def main():
    # Example usage
    strategy = CashStrategy(
        client_id="CLIENT123",
        user_id="USER456",
        strategy_id="CST0002",  # Replace with desired StrategyID
        total_fund=100000.0,
        mongo_uri="mongodb://localhost:27017"
    )
    await strategy.execution_func()
    await strategy.close()

if __name__ == "__main__":
    asyncio.run(main())
        


