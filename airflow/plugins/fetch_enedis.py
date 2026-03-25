from aiolimiter import AsyncLimiter
from airflow.providers.postgres.hooks.postgres import PostgresHook
import httpx
from tenacity import retry, stop_after_attempt, retry_if_exception, wait_exponential

import asyncio
import json
import logging
from pathlib import Path
from typing import Optional


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


BASE_URL = "https://opendata.enedis.fr/data-fair/api/v1/datasets/bilan-electrique-demi-heure/"
HEADERS = {"Accept": "application/json"}
RATE_LIMIT = AsyncLimiter(max_rate=600, time_period=1)

DB_PARAMS = {
    "host": "postgres-dw",
    "port":5432,
    "dbname": "medallion_datawarehouse",
    "user": "data_engineer",
    "password": "data_engineer"
}

DATA_DIR = Path("/opt/airflow/data")
CHECKPOINT_FILE = DATA_DIR / "checkpoint.json"
API_DATA_SCHEMA = DATA_DIR / "schema.json"


def get_checkpoint() -> Optional[str]:
    if CHECKPOINT_FILE.exists():
        try:
            with open(CHECKPOINT_FILE, "r") as f:
                return json.load(f).get("last_horodate")
        except Exception as e:
            logger.warning(f"Failed to read checkpoint: {e}")
    return None

def save_checkpoint(horodate: str):
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    try:
        with open(CHECKPOINT_FILE, "w") as f:
            json.dump({"last_horodate": horodate}, f)
        logger.info(f"Checkpoint saved: {horodate}")
    except Exception as e:
        logger.error(f"Failed to save checkpoint: {e}")


async def log_request(request: httpx.Request):
    logger.debug(f"Request event hook: {request.method} - {request.url}")

async def log_response(response: httpx.Response):
    logger.debug(f"Response Status Code: {response.status_code}")


@retry(
    retry=(retry_if_exception(httpx.HTTPStatusError) | retry_if_exception(httpx.RequestError)),
    stop=stop_after_attempt(5),
    wait=wait_exponential(min=1, max=10)
)
async def get_schema(client: httpx.AsyncClient, base_url):
    if not API_DATA_SCHEMA.exists():
        try:
            res = await client.get(base_url)
            res.raise_for_status()
            schema = res.json()
            with open(API_DATA_SCHEMA, "w") as f:
                json.dump(schema, f)
                logger.info("Schema successfully loaded")
        except (httpx.HTTPStatusError, httpx.RequestError) as e:
            logger.warning(e)
        except Exception as e:
            logger.warning(e)
    else:
        logger.info("Schema already provided")


async def fetch_data(base_url: str, last_horodate: Optional[str] = None):
    params = {"sort": "horodate"}
    if last_horodate:
        params["horodate_gt"] = last_horodate
    
    async with httpx.AsyncClient(
        headers=HEADERS,
        event_hooks={"request": [log_request], "response": [log_response]}
    ) as client:
        await get_schema(client, base_url + "schema")
        current_url = base_url + "lines"
        while current_url:
            for attempt in range(5):
                try:
                    await RATE_LIMIT.acquire()
                    res = await client.get(
                        current_url,
                        params=params if current_url == base_url + "lines" else None
                    )
                    res.raise_for_status()
                    
                    data = res.json()
                    results = data.get("results", [])
                    for row in results:
                        yield row
                    
                    current_url = data.get("next")
                    break
                except (httpx.HTTPStatusError, httpx.RequestError) as e:
                    if attempt == 4:
                        raise e
                    wait_time = 2 ** attempt
                    logger.warning(f"Error fetching data: {e}. Retrying in {wait_time}s... (Attempt {attempt + 1}/5)")
                    await asyncio.sleep(wait_time)
                except Exception as e:
                    raise e


def save_batch_to_bronze(batch):

    if not batch:
        return

    conn = None
    cursor = None

    try:
        logger.info("Connecting the Medallion Architecture...")

        postgres_hook = PostgresHook(postgres_conn_id="postgres-dw")

        conn = postgres_hook.get_conn()
        cursor = conn.cursor()

        # Create schema and table if not exists
        cursor.execute("""
        CREATE SCHEMA IF NOT EXISTS staging;
        """)
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS staging.staging (
            id SERIAL PRIMARY KEY,
            ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            raw_payload JSONB NOT NULL
        );
        """)

        insert_query = """
        INSERT INTO staging.staging (raw_payload)
        VALUES (%s);
        """

        # insert batch of rows
        cursor.executemany(
            insert_query,
            [(json.dumps(row),) for row in batch]
        )

        conn.commit()

        logger.info(f"Inserted {len(batch)} rows into staging.staging")

    except Exception as e:

        logger.error(f"Postgres load failed: {e}")

        if conn:
            conn.rollback()

        raise

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


async def main():
    last_horodate = get_checkpoint()
    if last_horodate:
        logger.info(f"Resuming from checkpoint: {last_horodate}")
    else:
        logger.info("No checkpoint found. Fetching all data.")

    batch = []
    batch_size = 3000
    max_horodate = last_horodate

    try:
        async for item in fetch_data(BASE_URL, last_horodate):
            batch.append(item)
            
            current_horodate = item.get("horodate")
            if current_horodate and (not max_horodate or current_horodate > max_horodate):
                max_horodate = current_horodate

            # Stop after batch_size rows
            if len(batch) >= batch_size:
                save_batch_to_bronze(batch)
                logger.info(f"Batch of {batch_size} rows saved. Stopping fetch for now.")
                break
        
        # Save smaller final batch if exists
        if batch and len(batch) < batch_size:
            save_batch_to_bronze(batch)
            logger.info(f"Final batch of {len(batch)} rows saved.")

        # Save checkpoint
        if max_horodate and max_horodate != last_horodate:
            save_checkpoint(max_horodate)

    except Exception as e:
        if max_horodate:
            save_checkpoint(max_horodate)
        logger.error(f"Pipeline failed: {e}")
        raise


def fetch():
    asyncio.run(main())

if __name__ == "__main__":
    asyncio.run(main())