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

    dsn = f"postgres://{PSQL_USER}:{PSQL_PWD}@{PSQL_HOST}:{PSQL_PORT}/{PSQL_DB}"

    pool = await asyncpg.create_pool(
        dsn=dsn,
        min_size=MIN_POOL_SIZE,  # Минимальное число готовых соединений
        max_size=MAX_POOL_SIZE,  # Максимальное число соединений
    )

    return pool
