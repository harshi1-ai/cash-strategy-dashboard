
import asyncio
import aiohttp
import pandas as pd
import logging
from datetime import datetime, date

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()]
)

async def fetch_vwap_data(tokens, access_token, tenant_id, api_key, api_url="https://jri4df7kaa.execute-api.ap-south-1.amazonaws.com/prod/interactive", exchange="NSE_EQ", time_periods=None):
    
    logger = logging.getLogger(__name__)
    time_periods = time_periods or [15]
    all_data_rows = []

    async with aiohttp.ClientSession() as session:
        tasks = [
            session.get(
                f"{api_url}/nontransactional/{tenant_id}/v1/getVWAPData/{exchange}/{token}/{time_period}",
                headers={"Authorization": f"Bearer {access_token}", "x-api-key": api_key}
            )
            for time_period in time_periods
            for token in tokens
        ]

        responses = await asyncio.gather(*tasks, return_exceptions=True)

        for i, response in enumerate(responses):
            token = tokens[i // len(time_periods)]
            time_period = time_periods[i % len(time_periods)]

            if isinstance(response, Exception):
                logger.error(f"Failed to fetch VWAP for token {token} ({time_period}m): {response}")
                continue

            try:
                if response.status == 200:
                    data = await response.json()
                    print
                    if data.get("status") == "success" and data.get("data"):
                        all_data_rows.extend(
                            {**row,  "exchange": exchange}
                            for row in data["data"]
                        )

                    else:
                        logger.warning(f"No VWAP data for token {token} ({time_period}m)")
                else:
                    logger.error(f"HTTP error for token {token} ({time_period}m): Status {response.status}, Response: {await response.text()}")
            except Exception as e:
                logger.error(f"Exception for token {token} ({time_period}m): {e}")

    if all_data_rows:
        df = pd.DataFrame(all_data_rows)
        output_filename = f"VWAP_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        df.to_csv(output_filename, index=False)
        logger.info(f"VWAP data saved to {output_filename}")
        print(f"VWAP data saved to {output_filename}")
    else:
        logger.warning("No VWAP data retrieved.")

async def main():
    tokens = [1964, 2885, 2799, 2303]
    time_periods = [15,30,60]
    access_token = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJtZW1iZXJJZCI6OTA5MDkwLCJ1c2VyaWQiOjkwOTA5MCwidGVuYW50aWQiOjkwOTA5MCwibWVtYmVySW5mbyI6eyJ0ZW5hbnRJZCI6IjM4NSIsImdyb3VwSWQiOiJITyIsInVzZXJJZCI6IklTMjEwNTIiLCJ0ZW1wbGF0ZUlkIjoiV0FWRSIsInVkSWQiOiJmYjViNDExMTgzYmM0ZDEyIiwib2NUb2tlbiI6IjB4MDFBMjZFNjcyMzREMzNBMjY3NzFDMEFEQ0MzNzdGIiwidXNlckCoZGUiOiJOWlNBRCIsImdyb3VwQ29kZSI6IkFBQUFBIiwiYXBpa2V5RGF0YSI6eyJzUHVibGlzaGVyQ29kZSI6ImN1IiwiQ3VzdG9tZXJJZCI6IjM4NSIsInNQYXJ0bmVyQXBwSWQiOiIwMUYwMEYiLCJzQXBwbGljYXRpb25Ub2tlbiI6IkluZGlyYVNlY3VyaXRpZXNCMkMxMDcwNDY0ZGVlZiIsIlB1Ymxpc2hlck5hbWUiOiJJbmRpcmEgU2VjdXJpdGllcyBQdnQgTHRkIC0gQjJDIiwiQnJva2VyTmFtZSI6IkluZGlyYSBTZWN1cml0aWVzIFB2dCBMdGQiLCJQcm9kdWN0U291cmNlIjoiIiwiQjJDIjoiWSIsInVzZXJJZCI6IklTMjEwNTIiLCJleHAiOjkyMDY1NTc1MDAsImlhdCI6MTc0MTU5NzUxNH0sInNvdXJjZSI6Ik1PQklMRUFQSSJ9LCJleHAiOjE3NTEzOTQ1OTksImlhdCI6MTc1MTM0ODg2OH0.pLhQF23xbdVU0sP4tlEbRcTwII-LIT1fOy0YDz4l7MQ"
    tenant_id = "385"
    api_key = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzUHVibGlzaGVyQ29kZSI6ImN1IiwiQ3VzdG9tZXJJZCI6IjM4NSIsInNQYXJ0bmVyQXBwSWQiOiIwMUYwMEYiLCJzQXBwbGljYXRpb25Ub2tlbiI6IkluZGlyYVNlY3VyaXRpZXNCMkMxMDcwNDY0ZGVlZiIsIlB1Ymxpc2hlck5hbWUiOiJJbmRpcmEgU2VjdXJpdGllcyBQdnQgTHRkIC0gQjJDIiwiQnJva2VyTmFtZSI6IkluZGlyYSBTZWN1cml0aWVzIFB2dCBMdGQiLCJQcm9kdWN0U291cmNlIjoiIiwiQjJDIjoiWSIsInVzZXJJZCI6IklTMjEwNTIiLCJleHAiOjkyMDY1NTc1MDAsImlhdCI6MTc0MTU5NzUxNH0.bpDvWPT460MXChAkZMICaMh5E8CXDXWJ_neY3bPGTg4"

    await fetch_vwap_data(
        tokens=tokens,
        access_token=access_token,
        tenant_id=tenant_id,
        api_key=api_key,
        time_periods=time_periods
    )

if __name__ == "__main__":
    asyncio.run(main())
