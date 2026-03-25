import httpx
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
import psycopg2
import logging
import os
import time

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

BASE_URL = "https://web-api.tp.entsoe.eu/api"
API_KEY = os.environ.get("ENTSOE_API_KEY")
FRANCE_EIC = "10YFR-RTE------C"
NS = {"ns": "urn:iec62325.351:tc57wg16:451-3:publicationdocument:7:3"}

DB_PARAMS = {
    "host": "localhost",
    "port": 5432,
    "dbname": os.environ.get("DWH_DB"),
    "user": os.environ.get("POSTGRES_USER"),
    "password": os.environ.get("POSTGRES_PASSWORD"),
}

BACKFILL_RANGES = [
    (datetime(2021, 1, 17, tzinfo=timezone.utc), datetime(2022, 1, 17, tzinfo=timezone.utc)),
    (datetime(2022, 1, 17, tzinfo=timezone.utc), datetime(2023, 1, 17, tzinfo=timezone.utc)),
    (datetime(2023, 1, 17, tzinfo=timezone.utc), datetime(2024, 1, 17, tzinfo=timezone.utc)),
    (datetime(2024, 1, 17, tzinfo=timezone.utc), datetime(2025, 1, 17, tzinfo=timezone.utc)),
    (datetime(2025, 1, 17, tzinfo=timezone.utc), datetime(2026, 1, 17, tzinfo=timezone.utc)),
    (datetime(2026, 1, 17, tzinfo=timezone.utc), datetime.now(timezone.utc)),
]


def get_text(element, tag):
    el = element.find(f"ns:{tag}", NS)
    return el.text.strip() if el is not None and el.text else None


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
        response = client.get(BASE_URL, params=params, timeout=60)
        response.raise_for_status()

    return parse_xml(response.text)


def parse_xml(xml_text):
    root = ET.fromstring(xml_text)
    rows = []

    # document-level fields
    doc_mrid           = get_text(root, "mRID")
    revision_number    = get_text(root, "revisionNumber")
    doc_type           = get_text(root, "type")
    created_datetime   = get_text(root, "createdDateTime")

    doc_period_start_el = root.find("ns:period.timeInterval/ns:start", NS)
    doc_period_end_el   = root.find("ns:period.timeInterval/ns:end", NS)
    doc_period_start   = doc_period_start_el.text if doc_period_start_el is not None else None
    doc_period_end     = doc_period_end_el.text if doc_period_end_el is not None else None

    for ts in root.findall("ns:TimeSeries", NS):
        ts_mrid               = get_text(ts, "mRID")
        auction_type          = get_text(ts, "auction.type")
        business_type         = get_text(ts, "businessType")
        in_domain             = get_text(ts, "in_Domain.mRID")
        out_domain            = get_text(ts, "out_Domain.mRID")
        market_agreement_type = get_text(ts, "contract_MarketAgreement.type")
        currency              = get_text(ts, "currency_Unit.name")
        price_measure_unit    = get_text(ts, "price_Measure_Unit.name")
        curve_type            = get_text(ts, "curveType")
        classification_pos    = get_text(ts, "classificationSequence_AttributeInstanceComponent.position")

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
        interval_end_el   = period.find("ns:timeInterval/ns:end", NS)
        if interval_start_el is None:
            continue

        interval_start = datetime.fromisoformat(interval_start_el.text.replace("Z", "+00:00"))
        interval_end   = interval_end_el.text if interval_end_el is not None else None

        for point in period.findall("ns:Point", NS):
            position  = int(get_text(point, "position"))
            price     = float(get_text(point, "price.amount"))
            timestamp = interval_start + timedelta(minutes=(position - 1) * minutes_per_period)

            rows.append((
                timestamp,
                doc_mrid,
                revision_number,
                doc_type,
                created_datetime,
                doc_period_start,
                doc_period_end,
                ts_mrid,
                auction_type,
                business_type,
                in_domain,
                out_domain,
                market_agreement_type,
                currency,
                price_measure_unit,
                curve_type,
                classification_pos,
                resolution,
                interval_start.isoformat(),
                interval_end,
                position,
                price,
            ))

    return rows


def load_to_postgres(rows):
    conn = psycopg2.connect(**DB_PARAMS)
    cursor = conn.cursor()

    try:
        cursor.execute("""
            CREATE SCHEMA IF NOT EXISTS staging;
            CREATE TABLE IF NOT EXISTS staging.rte_prices (
                id                      SERIAL PRIMARY KEY,
                timestamp               TIMESTAMP WITH TIME ZONE NOT NULL UNIQUE,
                ingested_at             TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                doc_mrid                VARCHAR,
                revision_number         VARCHAR,
                doc_type                VARCHAR,
                created_datetime        VARCHAR,
                doc_period_start        VARCHAR,
                doc_period_end          VARCHAR,
                ts_mrid                 VARCHAR,
                auction_type            VARCHAR,
                business_type           VARCHAR,
                in_domain               VARCHAR,
                out_domain              VARCHAR,
                market_agreement_type   VARCHAR,
                currency                VARCHAR,
                price_measure_unit      VARCHAR,
                curve_type              VARCHAR,
                classification_pos      VARCHAR,
                resolution              VARCHAR,
                interval_start          VARCHAR,
                interval_end            VARCHAR,
                position                INTEGER,
                price_eur_mwh           DOUBLE PRECISION
            );
        """)
        conn.commit()

        cursor.executemany("""
            INSERT INTO staging.rte_prices (
                timestamp, doc_mrid, revision_number, doc_type, created_datetime,
                doc_period_start, doc_period_end, ts_mrid, auction_type, business_type,
                in_domain, out_domain, market_agreement_type, currency, price_measure_unit,
                curve_type, classification_pos, resolution, interval_start, interval_end,
                position, price_eur_mwh
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (timestamp) DO UPDATE SET
                price_eur_mwh         = EXCLUDED.price_eur_mwh,
                resolution            = EXCLUDED.resolution,
                ingested_at           = CURRENT_TIMESTAMP;
        """, rows)
        conn.commit()
        logger.info(f"Inserted {len(rows)} rows")

    except Exception as e:
        conn.rollback()
        logger.error(f"Failed to insert rows: {e}", exc_info=True)
        raise
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    for period_start, period_end in BACKFILL_RANGES:
        try:
            rows = fetch_period(period_start, period_end)
            if rows:
                load_to_postgres(rows)
            else:
                logger.warning(f"No data for {period_start} to {period_end}")
        except Exception as e:
            logger.error(f"Failed for period {period_start} to {period_end}: {e}", exc_info=True)
        time.sleep(2)

    logger.info("Backfill complete")