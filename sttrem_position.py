
import asyncio
import pandas as pd
import numpy as np
import motor.motor_asyncio
import logging
import re
from datetime import datetime
import pytz
import os
from threading import Lock
import streamlit as st

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class CashStrategy:
    def __init__(self, mongo_uri: str = "mongodb://localhost:27017"):
        self.date = datetime.now(pytz.timezone("Asia/Kolkata")).strftime("%Y-%m-%d %I:%M %p IST")
        self.token_list = []
        self.file_lock = Lock()

        # Initialize MongoDB client
        self.mongo_client = motor.motor_asyncio.AsyncIOMotorClient(mongo_uri)
        self.db = self.mongo_client["OdinApp"]
        self.all_positions_col = self.db["all_positions"]
        self.position_col = self.db["position_col"]
        self.close_col = self.db["close_col"]

        logger.info("Initialized CashStrategy for all data")

    async def fetch_ltp(self, token: str) -> float:
        try:
            if token not in self.token_list:
                logger.info(f"Subscribing to token: {token}")
                self.token_list.append(token)
                await asyncio.sleep(0.1)

            ltp = 100.0 + float(token) % 10
            logger.debug(f"Fetched LTP {ltp} for token {token}")
            return ltp
        except Exception as e:
            logger.error(f"Error fetching LTP for token {token}: {e}")
            return None

    async def calculate_stats(self, output_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
        try:
            output_df["LTP"] = pd.to_numeric(output_df["LTP"], errors="coerce")

            list_columns = ["BuyPrice", "SellPrice", "ExQty", "EnQty"]
            for col in list_columns:
                output_df[col] = output_df[col].apply(lambda x: x if isinstance(x, list) else [])

            output_df["CapitalUsed"] = output_df.apply(
                lambda row: sum(p * q for p, q in zip(row["BuyPrice"], row["EnQty"]) if p and q),
                axis=1
            )

            buy_turnover = sum(sum(p * q for p, q in zip(row["BuyPrice"], row["EnQty"]) if p and q) for _, row in output_df.iterrows())
            sell_turnover = sum(sum(p * q for p, q in zip(row["SellPrice"], row["ExQty"]) if p and q) for _, row in output_df.iterrows())
            total_turnover = buy_turnover + sell_turnover

            output_df["PNL"] = output_df.apply(
                lambda row: (
                    (row["LTP"] - (np.mean(row["BuyPrice"]) if row["BuyPrice"] else 0)) * row["RemainingQty"]
                    + sum((p_s - p_b) * q for p_s, p_b, q in zip(row["SellPrice"], row["BuyPrice"], row["ExQty"]) if p_s and p_b and q)
                    if not pd.isna(row["LTP"])
                    else sum((p_s - p_b) * q for p_s, p_b, q in zip(row["SellPrice"], row["BuyPrice"], row["ExQty"]) if p_s and p_b and q)
                ),
                axis=1
            )

            output_df["UnrealisedMTM"] = output_df.apply(
                lambda row: (
                    (row["LTP"] - (np.mean(row["BuyPrice"]) if row["BuyPrice"] else 0)) * row["RemainingQty"]
                    if not pd.isna(row["LTP"]) else 0
                ),
                axis=1
            )

            profits = output_df["PNL"]
            profit_trades = profits[profits >= 0]
            loss_trades = profits[profits < 0]

            # Use a default total_fund or calculate per strategy if available
            default_total_fund = 100000.0
            # Ensure profit_trades and loss_trades are pandas Series for robust calculations
            profit_trades = pd.Series(profit_trades)
            loss_trades = pd.Series(loss_trades)
            stats = {
                "Investment Amount": default_total_fund,
                "number_of_open_pos": output_df["Pos"].value_counts().get("open", 0),
                "number_of_close_pos": output_df["Pos"].value_counts().get("close", 0),
                "Total Profit": profit_trades.sum(skipna=True) if not profit_trades.empty else 0,
                "Total Loss": loss_trades.sum(skipna=True) if not loss_trades.empty else 0,
                "No. of Profit Positions": len(profit_trades),
                "No. of Loss Positions": len(loss_trades),
                "Avg Profit": profit_trades.mean() if len(profit_trades) > 0 else 0,
                "Avg Loss": loss_trades.mean() if len(loss_trades) > 0 else 0,
                "Total Trades": len(profits),
                "Profit Factor": abs(profit_trades.sum() / loss_trades.sum()) if loss_trades.sum() != 0 else float("inf"),
                "Capital Utilization": output_df.loc[output_df["Pos"] == "open", "CapitalUsed"].sum(),
                "Capital Utilization %": (output_df.loc[output_df["Pos"] == "open", "CapitalUsed"].sum() / default_total_fund * 100),
                "Turnover": total_turnover
            }

            def format_value(x):
                if isinstance(x, (int, float)) and not pd.isna(x):
                    return f"{x:,.2f}"
                return x

            stats_df = pd.DataFrame([(k, format_value(v)) for k, v in stats.items()], columns=["Metric", "Value"])

            return output_df, stats_df

        except Exception as e:
            logger.error(f"Error in calculate_stats: {e}")
            return output_df, pd.DataFrame()

    async def update_positions(self):
        try:
            logger.info("Fetching all positions from all_positions_col")
            all_positions = pd.DataFrame(await self.all_positions_col.find().to_list(length=None))

            if all_positions.empty:
                logger.warning("No positions found in all_positions_col")
                return pd.DataFrame(), pd.DataFrame()

            # Generate identifier for each document using client_id and user_id from docs if available
            all_positions["identifier"] = all_positions.apply(
                lambda row: f"{row.get('client_id', 'UNKNOWN')}{row.get('user_id', 'UNKNOWN')}{row.get('StrategyID', 'UNKNOWN')}default",
                axis=1
            )

            # Separate open and closed positions
            open_positions = all_positions[all_positions["Pos"] == "open"].copy()
            closed_positions = all_positions[all_positions["Pos"] == "close"].copy()

            expected_columns = ["ExchangeInstrumentID", "Pos", "BuyPrice", "SellPrice", "EnQty", "ExQty", "RemainingQty", "Symbol", "identifier"]
            for col in expected_columns:
                if col not in open_positions.columns:
                    open_positions[col] = [[] if col in ["BuyPrice", "SellPrice", "EnQty", "ExQty"] else 0 for _ in range(len(open_positions))]
                if col not in closed_positions.columns:
                    closed_positions[col] = [[] if col in ["BuyPrice", "SellPrice", "EnQty", "ExQty"] else 0 for _ in range(len(closed_positions))]

            # Save to respective collections with identifier
            if not open_positions.empty:
                await self.position_col.delete_many({})
                await self.position_col.insert_many(open_positions.to_dict("records"))
                logger.info(f"Updated {len(open_positions)} open positions in position_col")

            if not closed_positions.empty:
                await self.close_col.delete_many({})
                await self.close_col.insert_many(closed_positions.to_dict("records"))
                logger.info(f"Updated {len(closed_positions)} closed positions in close_col")

            return open_positions, closed_positions

        except Exception as e:
            logger.error(f"Error in update_positions: {e}")
            return pd.DataFrame(), pd.DataFrame()

    async def execution_func(self, client_id_input=None):
        try:
            open_positions, closed_positions = await self.update_positions()

            output_df = pd.concat([open_positions, closed_positions], ignore_index=True)

            if output_df.empty:
                logger.warning("No positions to process")
                st.warning("No positions available to display.")
                return

            # Fetch LTPs for open positions only
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

            output_df, stats_df = await self.calculate_stats(output_df)

            # Ensure output_df is a DataFrame
            if not isinstance(output_df, pd.DataFrame):
                output_df = pd.DataFrame(output_df)

            # Check if 'PNL' exists
            if "PNL" not in output_df.columns:
                st.error("PNL column missing from output_df. Cannot display profit and loss.")
                return

            # Prepare data for dashboard with identifier-based filtering
            dashboard_columns = ["Symbol", "BuyPrice", "SellPrice", "EnQty", "Pos", "PNL", "identifier"]
            for col in dashboard_columns:
                if col not in output_df.columns:
                    output_df[col] = None
            dashboard_df = output_df[dashboard_columns].copy()

            # Calculate Qty column robustly
            dashboard_df["Qty"] = dashboard_df["EnQty"].apply(lambda x: sum(x) if isinstance(x, list) and len(x) > 0 else (x if isinstance(x, (int, float)) else 0))

            # Ensure dashboard_df is a pandas DataFrame for string operations
            if not isinstance(dashboard_df, pd.DataFrame):
                dashboard_df = pd.DataFrame(dashboard_df)
            dashboard_df["identifier"] = dashboard_df["identifier"].astype(str)
            # Debug output for dashboard_df and identifier values
            st.write("dashboard_df sample:", dashboard_df.head(10))
            # st.write("dashboard_df identifier values:", dashboard_df['identifier'].unique())

            # Use a safe extraction function for ClientID
            def safe_extract_client_id(identifier):
                if isinstance(identifier, str) and len(identifier) >= 6:
                    return identifier[:6]
                return "UNKNOW"
            dashboard_df["ClientID"] = dashboard_df["identifier"].apply(safe_extract_client_id)

            # Extract Client ID from identifier (before first space)
            def extract_client_id(identifier):
                if isinstance(identifier, str) and ' ' in identifier:
                    return identifier.split(' ')[0]
                elif isinstance(identifier, str):
                    return identifier  # fallback if no space
                return "UNKNOWN"
            dashboard_df["ClientID"] = dashboard_df["identifier"].apply(extract_client_id)

            # Filter DataFrame by entered Client ID
            if client_id_input:
                filtered_df = dashboard_df[dashboard_df["ClientID"] == client_id_input]
            else:
                filtered_df = dashboard_df  # Show all if no input

            # Select only the required columns
            display_columns = ["Symbol", "BuyPrice", "SellPrice", "Pos", "PNL", "Qty"]

            # Display the table with color formatting for PNL
            if not filtered_df.empty:
                display_df = filtered_df[display_columns]
                if not isinstance(display_df, pd.DataFrame):
                    display_df = pd.DataFrame(display_df)
                st.dataframe(
                    display_df.style.apply(
                        lambda x: [
                            'background-color: #90EE90' if v > 0 else 'background-color: #FF6347' if v < 0 else ''
                            for v in x['PNL']
                        ],
                        subset=['PNL']
                    )
                )
            else:
                st.warning("No data found for the entered Client ID.")

            st.write("### Strategy Statistics (Aggregated)")
            st.dataframe(stats_df)

        except Exception as e:
            logger.error(f"Error in execute_strategy: {e}")
            st.error(f"An error occurred: {e}")

    async def close(self):
        self.mongo_client.close()
        logger.info("MongoDB client closed")

def run_dashboard():
    st.set_page_config(layout="wide")
    # Prompt for Client ID at the very top
    client_id_input = st.text_input("Enter Client ID to view dashboard:")
    if not client_id_input:
        st.warning("Please enter a Client ID to view the dashboard.")
        return
    strategy = CashStrategy(mongo_uri="mongodb://localhost:27017")
    asyncio.run(strategy.execution_func(client_id_input))
    asyncio.run(strategy.close())

if __name__ == "__main__":
    run_dashboard()