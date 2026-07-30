import asyncio
import logging
import os
from pathlib import Path

from src.config.code import SQL_AGGREGATOR_1M_FILE, SQL_AGGREGATOR_1M_FOLDER, SQL_FOLDER
from src.config.service import SQL_UPDATE_SLEEP
from src.connectors.postgres import create_postgres_pool

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("transformer")


PG_HOST = os.getenv("POSTGRES_HOST", "dwh_postgres")
PG_PORT = os.getenv("POSTGRES_PORT", "5432")
PG_DB = os.getenv("POSTGRES_DB", "crypto_dwh")
PG_USER = os.getenv("POSTGRES_USER", "postgres")
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")

DSN = f"postgres://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}"

# Путь к SQL-файлу относительного текущего скрипта
SQL_FILE_PATH = (
    Path(__file__).resolve().parent.parent
    / SQL_FOLDER
    / SQL_AGGREGATOR_1M_FOLDER
    / SQL_AGGREGATOR_1M_FILE
)


async def run_transformation_loop():
    pool = await create_postgres_pool()

    if not SQL_FILE_PATH.exists():
        logger.error(f"SQL file not found at {SQL_FILE_PATH}")
        return

    logger.info(f"Starting transformation loop (interval: {SQL_UPDATE_SLEEP}s)...")

    try:
        while True:
            try:
                # Читаем свежий SQL-запрос из файла
                query = SQL_FILE_PATH.read_text(encoding="utf-8")

                # Выполняем агрегацию через пул соединений
                async with pool.acquire() as conn:
                    status = await conn.execute(query)
                    logger.info(f"Transformation executed successfully: {status}")

            except Exception as e:
                logger.error(
                    f"Error during transformation execution: {e}", exc_info=True
                )

            # Ждем 60 секунд до следующего запуска
            await asyncio.sleep(SQL_UPDATE_SLEEP)

    finally:
        await pool.close()


if __name__ == "__main__":
    asyncio.run(run_transformation_loop())
