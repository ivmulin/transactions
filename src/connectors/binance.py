# libs
from aiohttp import ClientError, ClientSession

# Custom
from src.config.service import (
    BINANCE_FETCHER_SETTINGS,
    BINANCE_TICKER_PARAM,
    BINANCE_URL,
)
from src.utils.decorators import async_timeout_retry


@async_timeout_retry(**BINANCE_FETCHER_SETTINGS)
async def fetch_price(session: ClientSession, ticker: str):
    """
    Fetch current course info via Binance API given ticker

    Params:
        session
        ticker
    """

    url = BINANCE_URL
    params = {BINANCE_TICKER_PARAM: ticker}

    try:
        async with session.get(url, params=params) as response:
            if response.status != 200:
                print(f"Client error at {ticker}: status {response.status}")
                return None

            data = await response.json()
            return data

    except ClientError as e:
        print(f"Network error for {ticker}: {e}")
        return None
