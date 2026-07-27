# libs
import asyncio
import json
import logging

from aiohttp import ClientSession
from aiokafka import AIOKafkaProducer

# Custom
from src.config.code import ASYNCIO_DELAY, BATCH_SIZE
from src.config.service import (
    BINANCE_TICKERS,
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC_PRICES,
)
from src.connectors.binance import fetch_price
from src.connectors.kafka import send_price_to_kafka
from src.utils.generators import batcher

# Настраиваем базовый уровень логирования для всего приложения
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)


async def main():

    # Создаем продюсера.
    # value_serializer автоматически превращает dict в байты JSON
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    # Подключение к брокеру
    await producer.start()

    try:
        async with ClientSession() as session:
            while True:
                # Нарезаем данные на мини-батчи
                batch_configs = batcher(BINANCE_TICKERS, batch_size=BATCH_SIZE)

                # Пробегаемся по батчам
                for batch_conf in batch_configs:
                    # Добавляем задания на отправку
                    tasks = [fetch_price(session, ticker) for ticker in batch_conf]

                    # Собираем данные
                    batch_results = await asyncio.gather(*tasks, return_exceptions=True)

                    for res in batch_results:
                        await send_price_to_kafka(producer, KAFKA_TOPIC_PRICES, res)

                await asyncio.sleep(ASYNCIO_DELAY)

    finally:
        await producer.stop()


if __name__ == "__main__":
    asyncio.run(main())
