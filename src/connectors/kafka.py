import asyncio
import json
import logging

from aiokafka import AIOKafkaProducer

from src.config.service import BINANCE_TICKER_PARAM

kafka_logger = logging.getLogger(__name__)


async def send_price_to_kafka(
    producer: AIOKafkaProducer, topic: str, data: dict
) -> None:
    """
    Отправляет тик в Apache Kafka.
    Использует имя тикера в качестве байтового ключа для Key-based Partitioning.
    """

    # Защита от пустого ответа (если fetch_price вернул None из-за ошибки сети)
    if not data or isinstance(data, Exception):
        return

    # Достаём тикер
    ticker = data.get(BINANCE_TICKER_PARAM)
    if not ticker:
        kafka_logger.warning(
            f"Пропущена отправка в Kafka: данные не содержат параметр '{BINANCE_TICKER_PARAM}': {data}"
        )
        return

    # Выделяем ключ
    ticker_bytes = ticker.encode("utf-8")

    try:
        # Отправляем сообщение
        await producer.send_and_wait(topic=topic, key=ticker_bytes, value=data)

        kafka_logger.info(f"Успешно отправлено в Kafka [{topic}] -> {ticker}")

    except Exception as e:
        kafka_logger.error(f"Ошибка при отправке {ticker} в Kafka: {e}")
