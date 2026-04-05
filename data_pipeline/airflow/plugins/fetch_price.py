import httpx
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from airflow.providers.postgres.hooks.postgres import PostgresHook
import logging
import os

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

BASE_URL = "https://web-api.tp.entsoe.eu/api"
API_KEY = os.environ.get("ENTSOE_API_KEY")
FRANCE_EIC = "10YFR-RTE------C"
POSTGRES_CONN_ID = "postgres-dw"
NS = {"ns": "urn:iec62325.351:tc57wg16:451-3:publicationdocument:7:3"}


def get_text(element, tag):
    el = element.find(f"ns:{tag}", NS)
    return el.text.strip() if el is not None and el.text else None


def fetch_spot_prices(data_interval_start=None):
    if not data_interval_start:
        data_interval_start = datetime.now(timezone.utc) - timedelta(days=1)

    if isinstance(data_interval_start, str):
        data_interval_start = datetime.fromisoformat(data_interval_start)

    period_start = data_interval_start.replace(hour=22, minute=0, second=0, microsecond=0) - timedelta(days=1)
    period_end = period_start + timedelta(days=1)

    rows = fetch_period(period_start, period_end)

    if not rows:
        raise ValueError(f"No price data found for {period_start} to {period_end}")

    load_to_postgres(rows)
    logger.info(f"Successfully loaded {len(rows)} price rows")


def fetch_period(period_start, period_end):
    params = {
        "securityToken": API_KEY,
        "documentType": "A44",
        "in_Domain": FRANCE_EIC,
        "out_Domain": FRANCE_EIC,
        "periodStart": period_start.strftime("%Y%m%d%H%M"),
        "periodEnd": period_end.strftime("%Y%m%d%H%M"),
        "contract_MarketAgreement.type": "A01",
    }

    logger.info(f"Fetching {period_start.strftime('%Y-%m-%d')} to {period_end.strftime('%Y-%m-%d')}")

    with httpx.Client() as client:
        response = client.get(BASE_URL, params=params, timeout=30)
        response.raise_for_status()

    return parse_xml(response.text)


def parse_xml(xml_text):
    root = ET.fromstring(xml_text)
    rows = []

    for ts in root.findall("ns:TimeSeries", NS):
        period = ts.find("ns:Period", NS)
        if period is None:
            continue

        resolution = get_text(period, "resolution")
        if resolution is None:
            continue

        if resolution == "PT60M":
            minutes_per_period = 60
        elif resolution == "PT30M":
            minutes_per_period = 30
        elif resolution == "PT15M":
            minutes_per_period = 15
        else:
            logger.warning(f"Unknown resolution: {resolution}, skipping")
            continue

        interval_start_el = period.find("ns:timeInterval/ns:start", NS)
        if interval_start_el is None:
            continue
        interval_start = datetime.fromisoformat(interval_start_el.text.replace("Z", "+00:00"))

        for point in period.findall("ns:Point", NS):
            position = int(get_text(point, "position"))
            price = float(get_text(point, "price.amount"))
            timestamp = interval_start + timedelta(minutes=(position - 1) * minutes_per_period)
            rows.append((timestamp, price, resolution, position))

    return rows


def load_to_postgres(rows):
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    hook.run("""
        CREATE SCHEMA IF NOT EXISTS staging;
        CREATE TABLE IF NOT EXISTS staging.rte_spot_prices (
            id            SERIAL PRIMARY KEY,
            timestamp     TIMESTAMP WITH TIME ZONE NOT NULL UNIQUE,
            ingested_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            price_eur_mwh DOUBLE PRECISION,
            resolution    VARCHAR,
            position      INTEGER
        );
    """, autocommit=True)

    hook.insert_rows(
        table="staging.rte_spot_prices",
        rows=rows,
        target_fields=["timestamp", "price_eur_mwh", "resolution", "position"],
        replace=True,
        replace_index="timestamp",
    )