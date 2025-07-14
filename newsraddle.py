
# import asyncio
# import pandas as pd
# import numpy as np
# import motor.motor_asyncio
# import logging
# import re
# from datetime import datetime
# import pytz
# import os
# from threading import Lock
# import streamlit as st

# # Configure logging
# logging.basicConfig(level=logging.INFO)
# logger = logging.getLogger(__name__)

# class CashStrategy:
#     def __init__(self, mongo_uri: str = "mongodb://localhost:27017"):
#         self.date = datetime.now(pytz.timezone("Asia/Kolkata")).strftime("%Y-%m-%d %I:%M %p IST")
#         self.token_list = []
#         self.file_lock = Lock()
#         self.mongo_client = motor.motor_asyncio.AsyncIOMotorClient(mongo_uri)
#         self.db = self.mongo_client["OdinApp"]
#         self.position_col = self.db["all_positions"]
#         logger.info("Initialized CashStrategy for dashboard")

#     async def fetch_ltp(self, token: str) -> float:
#         try:
#             if token not in self.token_list:
#                 logger.info(f"Subscribing to token: {token}")
#                 self.token_list.append(token)
#                 await asyncio.sleep(0.1)
#             ltp = 100.0 + float(token) % 10
#             logger.debug(f"Fetched LTP {ltp} for token {token}")
#             return ltp
#         except Exception as e:
#             logger.error(f"Error fetching LTP for token {token}: {e}")
#             return np.nan

#     async def stats_calculation(self, output_df: pd.DataFrame, total_fund: float) -> tuple[pd.DataFrame, pd.DataFrame]:
#         # --- DO NOT MODIFY THIS FUNCTION, just use it as required ---
#         try:
#             output_df["LTP"] = pd.to_numeric(output_df["LTP"], errors="coerce")
#             list_columns = ["BuyPrice", "SellPrice", "ExQty", "EnQty"]
#             for col in list_columns:
#                 output_df[col] = output_df[col].apply(lambda x: x if isinstance(x, list) else [])
#             output_df["CapitalUsed"] = output_df.apply(
#                 lambda row: sum(p * q for p, q in zip(row["BuyPrice"], row["EnQty"]) if p and q), axis=1)
#             buy_turnover = sum(sum(p * q for p, q in zip(row["BuyPrice"], row["EnQty"]) if p and q) for _, row in output_df.iterrows())
#             sell_turnover = sum(sum(p * q for p, q in zip(row["SellPrice"], row["ExQty"]) if p and q) for _, row in output_df.iterrows())
#             total_turnover = buy_turnover + sell_turnover
#             output_df["Profit"] = output_df.apply(
#                 lambda row: (
#                     (row["LTP"] - (np.mean(row["BuyPrice"]) if row["BuyPrice"] else 0)) * row["RemainingQty"]
#                     + sum((p_s - p_b) * q for p_s, p_b, q in zip(row["SellPrice"], row["BuyPrice"], row["ExQty"]) if p_s and p_b and q)
#                     if not pd.isna(row["LTP"])
#                     else sum((p_s - p_b) * q for p_s, p_b, q in zip(row["SellPrice"], row["BuyPrice"], row["ExQty"]) if p_s and p_b and q)
#                 ), axis=1)
#             output_df["UnrealisedMTM"] = output_df.apply(
#                 lambda row: (
#                     (row["LTP"] - (np.mean(row["BuyPrice"]) if row["BuyPrice"] else 0)) * row["RemainingQty"]
#                     if not pd.isna(row["LTP"]) else 0
#                 ), axis=1)
#             profits = output_df["Profit"]
#             profit_trades = profits[profits >= 0]
#             loss_trades = profits[profits < 0]
#             stats = {
#                 "Investment Amount": total_fund,
#                 "number_of_open_pos": output_df["Pos"].value_counts().get("open", 0),
#                 "number_of_close_pos": output_df["Pos"].value_counts().get("close", 0),
#                 "Total Profit": profit_trades.sum(),
#                 "Total Loss": loss_trades.sum(),
#                 "No. of Profit Positions": len(profit_trades),
#                 "No. of Loss Positions": len(loss_trades),
#                 "Avg Profit": profit_trades.mean() if len(profit_trades) > 0 else 0,
#                 "Avg Loss": loss_trades.mean() if len(loss_trades) > 0 else 0,
#                 "Total Trades": len(profits),
#                 "Profit Factor": abs(profit_trades.sum() / loss_trades.sum()) if loss_trades.sum() != 0 else float("inf"),
#                 "Capital Utilization": output_df.loc[output_df["Pos"] == "open", "CapitalUsed"].sum(),
#                 "Capital Utilization %": (output_df.loc[output_df["Pos"] == "open", "CapitalUsed"].sum() / total_fund * 100),
#                 "Turnover": total_turnover
#             }
#             def format_value(x):
#                 if isinstance(x, (int, float)) and not pd.isna(x):
#                     return f"{x:,.2f}"
#                 return x
#             stats_df = pd.DataFrame([(k, format_value(v)) for k, v in stats.items()], columns=["Metric", "Value"])
#             return output_df, stats_df
#         except Exception as e:
#             logger.error(f"Error in stats_calculation: {e}")
#             try:
#                 import streamlit as st
#                 st.error(f"Error in stats_calculation: {e}")
#                 st.write("DataFrame at error:", output_df.head())
#             except Exception:
#                 pass
#             return output_df, pd.DataFrame()

#     async def fetch_all_positions(self):
#         logger.info("Fetching all positions from MongoDB")
#         all_positions = pd.DataFrame(await self.position_col.find().to_list(length=None))
#         return all_positions

#     async def close(self):
#         self.mongo_client.close()
#         logger.info("MongoDB client closed")

# # Streamlit dashboard

# def run_dashboard():
#     st.set_page_config(layout="wide")
#     st.title("Cash Strategy Dashboard")
#     st.write(f"Updated: {datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%Y-%m-%d %I:%M %p IST')}")
#     strategy = CashStrategy(mongo_uri="mongodb://localhost:27017")
#     all_positions = asyncio.run(strategy.fetch_all_positions())
#     if all_positions.empty:
#         st.warning("No positions found in the database.")
#         return
#     # Extract client IDs and strategy IDs
#     def extract_client_id(identifier):
#         if isinstance(identifier, str) and ' ' in identifier:
#             return identifier.split(' ')[0]
#         elif isinstance(identifier, str):
#             return identifier
#         return "UNKNOWN"
#     all_positions["ClientID"] = all_positions["Identifier"].apply(extract_client_id)
#     all_positions["StrategyID"] = all_positions["StrategyID"].astype(str)
#     # Input for multiple client IDs (comma-separated)
#     client_id_input = st.text_input("Enter Client IDs (comma-separated)")
#     if not client_id_input:
#         st.warning("Please enter at least one Client ID to proceed.")
#         return
#     client_ids = [cid.strip() for cid in client_id_input.split(",") if cid.strip()]
#     client_df = all_positions[all_positions["ClientID"].isin(client_ids)]
#     if not isinstance(client_df, pd.DataFrame):
#         client_df = pd.DataFrame(client_df)
#     if client_df.empty:
#         st.warning("No data found for the entered Client IDs.")
#         return
#     # Multi-select for relevant strategy IDs
#     strategy_ids = sorted(pd.Series(client_df["StrategyID"]).dropna().unique())
#     selected_strategies = st.multiselect("Select Strategy IDs", strategy_ids, default=strategy_ids)
#     filtered_df = client_df[client_df["StrategyID"].isin(selected_strategies)]
#     if not isinstance(filtered_df, pd.DataFrame):
#         filtered_df = pd.DataFrame(filtered_df)
#     if filtered_df.empty:
#         st.warning("No data found for the selected strategies and clients.")
#         return
#     # Fetch LTPs for open positions
#     async def return_value(val):
#         return val
#     async def get_ltp_list(df):
#         tasks = [
#             strategy.fetch_ltp(row["ExchangeInstrumentID"]) if row["Pos"] == "open" else return_value(row.get("LTP", np.nan))
#             for _, row in df.iterrows()
#         ]
#         return await asyncio.gather(*tasks)
#     filtered_df["LTP"] = asyncio.run(get_ltp_list(filtered_df))
#     # Use StockInvestment as total fund if available, else 100000
#     stock_investment = None
#     if "StockInvestment" in filtered_df.columns:
#         stock_investment_series = pd.Series(filtered_df["StockInvestment"])
#         stock_investment_series = stock_investment_series.dropna()
#         if not stock_investment_series.empty:
#             stock_investment = stock_investment_series.iloc[0]
#     total_fund = stock_investment if stock_investment is not None else 100000.0

#     # Ensure filtered_df is a DataFrame
#     if not isinstance(filtered_df, pd.DataFrame):
#         filtered_df = pd.DataFrame(filtered_df)

#     # Ensure Qty column is present and derived from EnQty
#     if "EnQty" in filtered_df.columns:
#         filtered_df["Qty"] = filtered_df["EnQty"].apply(
#             lambda x: sum(x) if isinstance(x, list) and len(x) > 0 else (x if isinstance(x, (int, float)) else 0)
#         )
#     else:
#         filtered_df["Qty"] = 0  # fallback if EnQty is missing

#     # Calculate stats and Profit
#     filtered_df, stats_df = asyncio.run(strategy.stats_calculation(filtered_df, total_fund))

#     # Debug output: show columns and head after stats_calculation
#     # st.write("Columns after stats_calculation:", filtered_df.columns.tolist())
#     # st.write(filtered_df.head())

#     # Only display the table if 'Profit' exists
#     if "Profit" not in filtered_df.columns:
#         st.error("Profit column missing from the data. Available columns: " + str(filtered_df.columns.tolist()))
#         st.write(filtered_df.head())
#         return

#     display_columns = ["StrategyID", "ClientID", "Symbol", "Pos", "Profit", "BuyPrice", "SellPrice", "Qty"]
#     # Ensure all columns exist before selecting
#     missing_cols = [col for col in display_columns if col not in filtered_df.columns]
#     if missing_cols:
#         st.error(f"Missing columns in data: {missing_cols}")
#         st.write(filtered_df.head())
#         return

#     st.write("### Position Table")
#     display_df = filtered_df[display_columns].copy().reset_index(drop=True)
#     def profit_color(val):
#         if val > 0:
#             return 'background-color: #98FF98'
#         elif val < 0:
#             return 'background-color: #FFB6C1'
#         return ''
#     st.dataframe(
#         display_df.style.applymap(profit_color, subset=['Profit'])
#     )
#     # Display stats
#     st.write("### Strategy Statistics (Aggregated)")
#     st.dataframe(stats_df)
#     asyncio.run(strategy.close())

# if __name__ == "__main__":
#     run_dashboard()






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
        self.mongo_client = motor.motor_asyncio.AsyncIOMotorClient(mongo_uri)
        self.db = self.mongo_client["OdinApp"]
        self.position_col = self.db["all_positions"]
        logger.info("Initialized CashStrategy for dashboard")

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
            return np.nan

    async def stats_calculation(self, output_df: pd.DataFrame, total_fund: float) -> tuple[pd.DataFrame, pd.DataFrame]:
        # --- DO NOT MODIFY THIS FUNCTION, just use it as required ---
        try:
            output_df["LTP"] = pd.to_numeric(output_df["LTP"], errors="coerce")
            list_columns = ["BuyPrice", "SellPrice", "ExQty", "EnQty"]
            for col in list_columns:
                output_df[col] = output_df[col].apply(lambda x: x if isinstance(x, list) else [])
            output_df["CapitalUsed"] = output_df.apply(
                lambda row: sum(p * q for p, q in zip(row["BuyPrice"], row["EnQty"]) if p and q), axis=1)
            buy_turnover = sum(sum(p * q for p, q in zip(row["BuyPrice"], row["EnQty"]) if p and q) for _, row in output_df.iterrows())
            sell_turnover = sum(sum(p * q for p, q in zip(row["SellPrice"], row["ExQty"]) if p and q) for _, row in output_df.iterrows())
            total_turnover = buy_turnover + sell_turnover
            output_df["Profit"] = output_df.apply(
                lambda row: (
                    (row["LTP"] - (np.mean(row["BuyPrice"]) if row["BuyPrice"] else 0)) * row["RemainingQty"]
                    + sum((p_s - p_b) * q for p_s, p_b, q in zip(row["SellPrice"], row["BuyPrice"], row["ExQty"]) if p_s and p_b and q)
                    if not pd.isna(row["LTP"])
                    else sum((p_s - p_b) * q for p_s, p_b, q in zip(row["SellPrice"], row["BuyPrice"], row["ExQty"]) if p_s and p_b and q)
                ), axis=1)
            output_df["UnrealisedMTM"] = output_df.apply(
                lambda row: (
                    (row["LTP"] - (np.mean(row["BuyPrice"]) if row["BuyPrice"] else 0)) * row["RemainingQty"]
                    if not pd.isna(row["LTP"]) else 0
                ), axis=1)
            profits = output_df["Profit"]
            profit_trades = profits[profits >= 0]
            loss_trades = profits[profits < 0]
            stats = {
                "Investment Amount": total_fund,
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
                "Capital Utilization %": (output_df.loc[output_df["Pos"] == "open", "CapitalUsed"].sum() / total_fund * 100),
                "Turnover": total_turnover
            }
            def format_value(x):
                if isinstance(x, (int, float)) and not pd.isna(x):
                    return f"{x:,.2f}"
                return x
            stats_df = pd.DataFrame([(k, format_value(v)) for k, v in stats.items()], columns=["Metric", "Value"])
            return output_df, stats_df
        except Exception as e:
            logger.error(f"Error in stats_calculation: {e}")
            try:
                import streamlit as st
                st.error(f"Error in stats_calculation: {e}")
                st.write("DataFrame at error:", output_df.head())
            except Exception:
                pass
            return output_df, pd.DataFrame()

    async def fetch_all_positions(self):
        logger.info("Fetching all positions from MongoDB")
        all_positions = pd.DataFrame(await self.position_col.find().to_list(length=None))
        return all_positions

    async def close(self):
        self.mongo_client.close()
        logger.info("MongoDB client closed")

# Streamlit dashboard

def run_dashboard():
    st.set_page_config(layout="wide", page_title="Cash Strategy Dashboard", page_icon="💹")
    # Sidebar for filters
    with st.sidebar:
        st.title("Filters")
        st.markdown("---")
        client_id_input = st.text_input("Enter Client IDs (comma-separated)")
        st.markdown(":grey[Tip: Separate multiple IDs with commas]")
    st.markdown("""
        <style>
        .big-font {font-size:32px !important; font-weight:700;}
        .subtitle {font-size:18px !important; color: #555;}
        .kpi-card {background: #f8f9fa; border-radius: 8px; padding: 18px 12px; margin: 0 8px 12px 0; text-align: center; box-shadow: 0 1px 3px rgba(0,0,0,0.04);}
        </style>
    """, unsafe_allow_html=True)
    st.markdown('<div class="big-font">Cash Strategy Dashboard</div>', unsafe_allow_html=True)
    st.markdown('<div class="subtitle">Monitor your cash strategy performance and positions in real time.</div>', unsafe_allow_html=True)
    st.write(f"Updated: {datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%Y-%m-%d %I:%M %p IST')}")
    strategy = CashStrategy(mongo_uri="mongodb://localhost:27017")
    all_positions = asyncio.run(strategy.fetch_all_positions())
    if all_positions.empty:
        st.warning("No positions found in the database.")
        return
    def extract_client_id(identifier):
        if isinstance(identifier, str) and ' ' in identifier:
            return identifier.split(' ')[0]
        elif isinstance(identifier, str):
            return identifier
        return "UNKNOWN"
    all_positions["ClientID"] = all_positions["Identifier"].apply(extract_client_id)
    all_positions["StrategyID"] = all_positions["StrategyID"].astype(str)
    if not client_id_input:
        st.warning("Please enter at least one Client ID to proceed.")
        return
    client_ids = [cid.strip() for cid in client_id_input.split(",") if cid.strip()]
    client_df = all_positions[all_positions["ClientID"].isin(client_ids)]
    if not isinstance(client_df, pd.DataFrame):
        client_df = pd.DataFrame(client_df)
    if client_df.empty:
        st.warning("No data found for the entered Client IDs.")
        return
    strategy_ids = sorted(pd.Series(client_df["StrategyID"]).dropna().unique())
    with st.sidebar:
        selected_strategies = st.multiselect("Select Strategy IDs", strategy_ids, default=strategy_ids)
    filtered_df = client_df[client_df["StrategyID"].isin(selected_strategies)]
    if not isinstance(filtered_df, pd.DataFrame):
        filtered_df = pd.DataFrame(filtered_df)
    if filtered_df.empty:
        st.warning("No data found for the selected strategies and clients.")
        return
    async def return_value(val):
        return val
    async def get_ltp_list(df):
        tasks = [
            strategy.fetch_ltp(row["ExchangeInstrumentID"]) if row["Pos"] == "open" else return_value(row.get("LTP", np.nan))
            for _, row in df.iterrows()
        ]
        return await asyncio.gather(*tasks)
    filtered_df["LTP"] = asyncio.run(get_ltp_list(filtered_df))
    # Calculate total_fund: use StockInvestment if available, else sum(BuyPrice * EnQty) from all_positions
    stock_investment = None
    if "StockInvestment" in filtered_df.columns:
        stock_investment_series = pd.Series(filtered_df["StockInvestment"])
        stock_investment_series = stock_investment_series.dropna()
        if not stock_investment_series.empty:
            stock_investment = stock_investment_series.iloc[0]
    if stock_investment is not None:
        total_fund = stock_investment
    else:
        # Multiply only the first value of BuyPrice and EnQty for each row (if both exist)
        def multiply_first_buyprice_enqty(row):
            buy_prices = row["BuyPrice"] if "BuyPrice" in row and isinstance(row["BuyPrice"], list) else []
            en_qtys = row["EnQty"] if "EnQty" in row and isinstance(row["EnQty"], list) else []
            return (buy_prices[0] * en_qtys[0]) if buy_prices and en_qtys else 0
        total_fund = all_positions.apply(multiply_first_buyprice_enqty, axis=1).sum()
    if not isinstance(filtered_df, pd.DataFrame):
        filtered_df = pd.DataFrame(filtered_df)
    if "EnQty" in filtered_df.columns:
        filtered_df["Qty"] = filtered_df["EnQty"].apply(
            lambda x: sum(x) if isinstance(x, list) and len(x) > 0 else (x if isinstance(x, (int, float)) else 0)
        )
    else:
        filtered_df["Qty"] = 0
    filtered_df, stats_df = asyncio.run(strategy.stats_calculation(filtered_df, total_fund))
    if "Profit" not in filtered_df.columns:
        st.error("Profit column missing from the data. Available columns: " + str(filtered_df.columns.tolist()))
        st.write(filtered_df.head())
        return
    display_columns = ["StrategyID", "ClientID", "Symbol", "Pos", "Profit", "BuyPrice", "SellPrice", "Qty"]
    missing_cols = [col for col in display_columns if col not in filtered_df.columns]
    if missing_cols:
        st.error(f"Missing columns in data: {missing_cols}")
        st.write(filtered_df.head())
        return
    # KPIs as cards (Investment Amount, Total Profit, Total Loss, number_of_open_pos, number_of_close_pos)
    def get_kpi_value(df, metric):
        series = pd.Series(df[df["Metric"] == metric]["Value"])
        return series.iloc[0] if not series.empty else "-"
    kpi_metrics = [
        ("Investment Amount", get_kpi_value(stats_df, "Investment Amount")),
        ("Total Profit", get_kpi_value(stats_df, "Total Profit")),
        ("Total Loss", get_kpi_value(stats_df, "Total Loss")),
        ("number_of_open_pos", get_kpi_value(stats_df, "number_of_open_pos")),
        ("number_of_close_pos", get_kpi_value(stats_df, "number_of_close_pos"))
    ]
    kpi_cols = st.columns(len(kpi_metrics))
    for i, (label, value) in enumerate(kpi_metrics):
        kpi_cols[i].markdown(f'<div class="kpi-card"><div style="font-size:15px; color:#888;">{label}</div><div style="font-size:22px; font-weight:700; color:#222;">{value}</div></div>', unsafe_allow_html=True)
    # Tabs for tables and stats
    tab1, tab2 = st.tabs(["Position Table", "Strategy Statistics"])
    with tab1:
        st.write("")
        display_df = filtered_df[display_columns].copy().reset_index(drop=True)
        def profit_color(val):
            if val > 0:
                return 'background-color: #98FF98'
            elif val < 0:
                return 'background-color: #FFB6C1'
            return ''
        st.dataframe(
            display_df.style.applymap(profit_color, subset=['Profit'])
        )
    with tab2:
        st.dataframe(stats_df, hide_index=True)
    asyncio.run(strategy.close())

if __name__ == "__main__":
    run_dashboard()



