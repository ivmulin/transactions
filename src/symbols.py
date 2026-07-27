import requests

# Это просто файл для получения списка тикеров. К проекту не относится!!!!!!!


def get_binance_tickers():
    url = "https://api.binance.com/api/v3/exchangeInfo"
    response = requests.get(url).json()

    # Фильтруем только спотовые пары, которые торгуются к USDT и сейчас активны
    tickers = [
        s["symbol"]
        for s in response["symbols"]
        if s["quoteAsset"] == "USDT" and s["status"] == "TRADING"
    ]

    return tickers


if __name__ == "__main__":
    all_usdt_tickers = get_binance_tickers()
    print(f"Всего пар к USDT: {len(all_usdt_tickers)}")
    print("Пример первых 15 пар:", all_usdt_tickers[:15])
