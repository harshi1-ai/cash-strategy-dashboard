from odin_function import *

async def test_fetch_trade_data():
    trade_manager = login("AG33")
    result = await trade_manager.async_init()
    print(result)  # Output: Trade Data Received

# Run the test
asyncio.run(test_fetch_trade_data())