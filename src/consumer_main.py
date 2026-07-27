# libs
import asyncio
import json
import logging
import time
from datetime import datetime, timezone

from aiokafka import AIOKafkaConsumer

# Custom
from src.config.code import (
    FLUSH_INTERVAL,
    MAX_BATCH_SIZE,
)
from src.config.service import (
    CONSUMER_MAX_RECORDS,
    CONSUMER_TIMEOUT_MS,
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_CONSUMER_GROUP,
    KAFKA_TOPIC_PRICES,
)
from src.connectors.postgres import create_postgres_pool, flush_batch_data, query
from src.utils.shutdown import register_stop_event

# Настраиваем базовый уровень логирования для всего приложения
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)

consumer_logger = logging.getLogger(__name__)


async def run_consumer(pool):
    """
    Запускает потребителя для текущего пула соединения
    """

    # Добавляем обработку GraceFul Shutdown
    stop_event = register_stop_event()

    # Создаём потребителя
    consumer = AIOKafkaConsumer(
        KAFKA_TOPIC_PRICES,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=KAFKA_CONSUMER_GROUP,
        enable_auto_commit=False,  # ОТКЛЮЧАЕМ автокоммит!
        auto_offset_reset="earliest",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
    )

    await consumer.start()

    current_batch = []

    try:
        previous_transaction_time = time.monotonic()
        while not stop_event.is_set():
            # Вычитываем батч с небольшим timeout_ms,
            # чтобы цикл регулярно возвращался к проверке stop_event
            batch = await consumer.getmany(
                timeout_ms=CONSUMER_TIMEOUT_MS, max_records=CONSUMER_MAX_RECORDS
            )

            if not batch:
                # Ничего не пришло - переходим на следующий круг
                continue

            for tp, messages in batch.items():
                consumer_logger.info(
                    f"Топик {tp.topic} - {tp.partition}, получено сообщений: {len(messages)}"
                )
                for msg in messages:
                    values = msg.value
                    ticker = values.get("symbol")
                    price = float(values.get("price"))
                    # Преобразуем timestamp из миллисекунд Kafka в datetime/timestamp для PG
                    created_at = datetime.fromtimestamp(
                        msg.timestamp / 1000.0, tz=timezone.utc
                    )

                    current_batch.append((ticker, price, created_at))

            # Отправляем данные на DWH
            current_time = time.monotonic()
            delta_transactions = current_time - previous_transaction_time

            # Если буфер переполнен или прошло много времени,
            # отправляем данные на сервер
            should_flush = len(current_batch) > MAX_BATCH_SIZE or (
                delta_transactions > FLUSH_INTERVAL and len(current_batch) > 0
            )

            if should_flush:
                flushed = False
                for attempt in range(3):
                    try:
                        await flush_batch_data(current_batch, query, pool)
                        flushed = True
                        break
                    except Exception as e:
                        consumer_logger.error(
                            f"[DWH] Ошибка записи (попытка {attempt + 1}/3): {e}"
                        )
                        await asyncio.sleep(1)

                if flushed:
                    # Фиксируем офсеты в Kafka ПОСЛЕ успешной вставки
                    await consumer.commit()
                    consumer_logger.info(
                        f"[DWH] {len(current_batch)} записей успешно отправлено в DWH"
                    )
                    # Очищаем буфер и сбрасываем таймер ТОЛЬКО при успешной отправке
                    current_batch.clear()
                    previous_transaction_time = current_time
                else:
                    consumer_logger.critical(
                        "[DWH] Не удалось записать батч в Postgres за 3 попытки!"
                    )

    finally:
        print("[Shutdown] Закрываем соединение с Kafka Consumer...")
        await consumer.stop()
        print("[Shutdown] Consumer успешно остановлен!")


async def main():
    """
    Обработка консьюмерами данных с сервера
    """

    pool = await create_postgres_pool()

    try:
        # Передаем пул в консьюмер
        await run_consumer(pool)
    finally:
        # При завершении работы приложения закрываем все соединения
        await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
