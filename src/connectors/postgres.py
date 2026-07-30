import asyncio
import logging
import os

import asyncpg
from dotenv import load_dotenv

from src.config.code import DWH_NAME, MAX_POOL_SIZE, MIN_POOL_SIZE
from src.config.service import (
    PSQL_DB_NAME,
    PSQL_HOST_NAME,
    PSQL_PORT_NAME,
    PSQL_PWD_NAME,
    PSQL_USER_NAME,
)

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("transformer")

query = f"""
    INSERT INTO {DWH_NAME} (ticker, price, created_at)
    VALUES ($1, $2, $3);
"""


async def flush_batch_data(batch, query, pool):
    # Запрашиваем соединение, затем создаём транзакцию
    async with pool.acquire() as connection, connection.transaction():
        await connection.executemany(query, batch)


async def create_postgres_pool():
    """
    Создаёт подключение Postgres
    """

    # Создаём подключение

    load_dotenv()

    PSQL_DB = os.getenv(PSQL_DB_NAME)
    PSQL_USER = os.getenv(PSQL_USER_NAME)
    PSQL_PWD = os.getenv(PSQL_PWD_NAME)
    PSQL_HOST = os.getenv(PSQL_HOST_NAME, "localhost")
    PSQL_PORT = os.getenv(PSQL_PORT_NAME, "5432")

    DSN = f"postgres://{PSQL_USER}:{PSQL_PWD}@{PSQL_HOST}:{PSQL_PORT}/{PSQL_DB}"

    while True:
        try:
            pool = await asyncpg.create_pool(
                dsn=DSN, min_size=MIN_POOL_SIZE, max_size=MAX_POOL_SIZE
            )
            logger.info("Successfully connected to PostgreSQL DWH")
            return pool
        except Exception as e:
            logger.warning(f"Database unavailable ({e}), retrying in 5 seconds...")
            await asyncio.sleep(5)

    return pool
