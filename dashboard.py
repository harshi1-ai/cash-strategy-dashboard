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
                "Number of Open Positions": output_df["Pos"].value_counts().get("open", 0),
                "Number of Closed Positions": output_df["Pos"].value_counts().get("close", 0),
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
            stats_df = pd.DataFrame([(k, format_value(v)) for k, v in stats.items()], columns=pd.Index(["Metric", "Value"]))
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
    .stApp {background-color: var(--background-color);}
    section[data-testid="stSidebar"] {
        background-color: var(--secondary-background-color);
        padding: 32px 16px 16px 16px;
    }
    .main-title {
        font-size: 2.6rem; font-weight: 800;
        color: var(--text-color); text-align: center; margin-bottom: 0.1em;
        font-family: "Roboto", Arial, sans-serif;
    }
    .subtitle {
        font-size: 1.3rem; color: var(--text-color);
        text-align: center; margin-bottom: 1.5em;
        font-family: "Roboto", Arial, sans-serif;
    }
    .kpi-row {display: flex; justify-content: center; gap: 24px; margin-bottom: 2em;}
    .kpi-card {
        background: var(--secondary-background-color);
        border-radius: 12px; padding: 22px 18px; min-width: 180px;
        box-shadow: 0 4px 24px 0 rgba(30, 64, 175, 0.18), 0 1.5px 6px 0 rgba(0,0,0,0.10);
        text-align: center;
        transition: box-shadow 0.2s;
        font-family: "Roboto", Arial, sans-serif;
    }
    .kpi-label {font-size: 1.05rem; color: var(--text-color); font-weight: 600; margin-bottom: 0.2em;}
    .kpi-value {font-size: 2rem; font-weight: 800; color: var(--primary-color);}
    .section-heading {font-size: 1.3rem; font-weight: 700; color: var(--primary-color); margin-top: 2em; margin-bottom: 0.5em; letter-spacing: 0.5px;
        font-family: "Roboto", Arial, sans-serif;
    }
    .custom-hr {border: none; border-top: 2px solid var(--primary-color); margin: 2em 0 1.5em 0;}
    /* Table header and row coloring */
    .stDataFrame thead tr th {background-color: var(--secondary-background-color) !important; color: var(--text-color) !important; font-size: 1.05rem; font-weight: 700; border-bottom: 2px solid #222 !important;
        font-family: "Roboto", Arial, sans-serif;
    }
    .stDataFrame tbody tr {border-bottom: 1.5px solid #222 !important;}
    .stDataFrame tbody tr:nth-child(even) {background-color: rgba(200, 200, 255, 0.04) !important;}
    .stDataFrame tbody tr:nth-child(odd) {background-color: var(--background-color) !important;}
    /* Profit coloring for light/dark themes */
    @media (prefers-color-scheme: dark) {
        .profit-positive {background-color: #214d36 !important; color: #fff !important;}
        .profit-negative {background-color: #4d2121 !important; color: #fff !important;}
    }
    @media (prefers-color-scheme: light) {
        .profit-positive {background-color: #27ae60 !important; color: #fff !important;}
        .profit-negative {background-color: #e74c3c !important; color: #fff !important;}
    }
    /* Sidebar input box styling */
    section[data-testid="stSidebar"] input,
    section[data-testid="stSidebar"] textarea,
    section[data-testid="stSidebar"] .stMultiSelect {
        border: 2px solid #cccccc !important;
        background: #f3f3f3 !important;
        color: #222 !important;
        border-radius: 8px !important;
        margin-bottom: 12px !important;
        box-shadow: none !important;
        outline: none !important;
        box-sizing: border-box !important;
        height: 36px !important;
        transition: border 0.2s;
        font-family: "Roboto", Arial, sans-serif;
    }
    @media (prefers-color-scheme: dark) {
        section[data-testid="stSidebar"] input,
        section[data-testid="stSidebar"] textarea,
        section[data-testid="stSidebar"] .stMultiSelect {
            background: #222 !important;
            color: #fff !important;
            border: 2px solid #cccccc !important;
        }
    }
    section[data-testid="stSidebar"] input:focus,
    section[data-testid="stSidebar"] input:active,
    section[data-testid="stSidebar"] input:invalid,
    section[data-testid="stSidebar"] textarea:focus,
    section[data-testid="stSidebar"] textarea:active,
    section[data-testid="stSidebar"] textarea:invalid {
        border: 2px solid #cccccc !important;
        box-shadow: none !important;
        outline: none !important;
        height: 36px !important;
    }
    section[data-testid="stSidebar"] .stMultiSelect {
        padding: 4px 0 4px 0 !important;
    }
    /* Professional, subtle style for selected tags in the multiselect (robust selector) */
    section[data-testid="stSidebar"] .stMultiSelect [data-baseweb="tag"] {
        background: #f3f3f3 !important;
        color: #222 !important;
        border: 1px solid #cccccc !important;
        border-radius: 6px !important;
        box-shadow: none !important;
        font-weight: 500 !important;
        font-size: 0.95rem !important;
        padding: 4px 10px !important;
        margin: 2px 0 !important;
        font-family: "Roboto", Arial, sans-serif;
    }
    @media (prefers-color-scheme: dark) {
        section[data-testid="stSidebar"] .stMultiSelect [data-baseweb="tag"] {
            background: #222 !important;
            color: #fff !important;
            border: 1px solid #cccccc !important;
        }
    }
    /* Remove red border from the multiselect itself in all states */
    section[data-testid="stSidebar"] .stMultiSelect:focus-within,
    section[data-testid="stSidebar"] .stMultiSelect:active,
    section[data-testid="stSidebar"] .stMultiSelect:invalid {
        border: 2px solid #cccccc !important;
        box-shadow: none !important;
        outline: none !important;
    }
    /* Remove border from the multiselect container */
    section[data-testid="stSidebar"] .stMultiSelect {
        border: none !important;
        box-shadow: none !important;
        outline: none !important;
    }
    /* Add border only to the input area inside the multiselect */
    section[data-testid="stSidebar"] .stMultiSelect div[data-baseweb="select"] > div {
        border: 2px solid #cccccc !important;
        border-radius: 8px !important;
        background: #f3f3f3 !important;
        box-shadow: none !important;
        outline: none !important;
        min-height: 36px !important;
        font-family: "Roboto", Arial, sans-serif;
    }
    @media (prefers-color-scheme: dark) {
        section[data-testid="stSidebar"] .stMultiSelect div[data-baseweb="select"] > div {
            background: #222 !important;
            color: #fff !important;
            border: 2px solid #cccccc !important;
        }
    }
    </style>
""", unsafe_allow_html=True)
                
    st.markdown('<div class="main-title">Cash Strategy Insights</div>', unsafe_allow_html=True)
    st.markdown('<div class="subtitle">Monitor your cash strategy performance and positions in real time.</div>', unsafe_allow_html=True)
    st.write(f"<div style='text-align:center; color:var(--text-color); font-size:1rem;'>Updated: {datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%Y-%m-%d %I:%M %p IST')}</div>", unsafe_allow_html=True)
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
    # For open positions, set SellPrice's last value to LTP (robust version)
    def update_sellprice_with_ltp(row):
        if row["Pos"] == "open":
            sell_prices = row["SellPrice"]
            if not isinstance(sell_prices, list):
                sell_prices = [] if pd.isna(sell_prices) else [sell_prices]
            ltp = row.get("LTP", np.nan)
            if pd.isna(ltp):
                return sell_prices
            if sell_prices:
                sell_prices = sell_prices[:-1] + [ltp]
            else:
                sell_prices = [ltp]
            return sell_prices
        return row["SellPrice"]
    filtered_df["SellPrice"] = filtered_df.apply(update_sellprice_with_ltp, axis=1)
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
    display_columns = ["StrategyID", "ClientID", "Symbol", "Pos", "Profit", "BuyPrice", "SellPrice", "LTP", "Qty"]
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
        ("Number of Open Positions", get_kpi_value(stats_df, "number_of_open_pos")),
        ("Number of Closed Positions", get_kpi_value(stats_df, "number_of_close_pos"))
    ]
    st.markdown('<div class="kpi-row">' + ''.join([
        f'<div class="kpi-card"><div class="kpi-label">{label}</div><div class="kpi-value">{value}</div></div>'
        for label, value in kpi_metrics
    ]) + '</div>', unsafe_allow_html=True)
    st.markdown('<hr class="custom-hr" />', unsafe_allow_html=True)
    # Tabs for tables and stats
    tab1, tab2 = st.tabs(["Position Table", "Strategy Statistics"])
    with tab1:
        st.markdown('<div class="section-heading">Position Table</div>', unsafe_allow_html=True)
        display_df = filtered_df[display_columns].copy().reset_index(drop=True)
        def profit_style(val):
            if val > 0:
                return 'background-color: #27ae60; color: #fff;'
            elif val < 0:
                return 'background-color: #e74c3c; color: #fff;'
            return ''
        st.dataframe(
            display_df.style.applymap(profit_style, subset=['Profit'])
        )
    with tab2:
        st.markdown('<div class="section-heading">Strategy Statistics (Aggregated)</div>', unsafe_allow_html=True)
        st.dataframe(stats_df, hide_index=True)
    asyncio.run(strategy.close())

if __name__ == "__main__":
    run_dashboard()
