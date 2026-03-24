import asyncio
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone

from airflow.providers.postgres.hooks.postgres import PostgresHook
from playwright.async_api import async_playwright

TARGET_URL = "https://www.rte-france.com/donnees-publications/eco2mix-donnees-temps-reel/donnees-marche"
XHR_MARKER = "<donneesMarche"
POSTGRES_CONN_ID = "postgres-dw"


def scrape_rte_spot_prices(data_interval_start=None):
    if not data_interval_start:
        data_interval_start = datetime.now()
    target_date = data_interval_start.date()

    async def run() -> list[str]:
        captured_xml: list[str] = []

        async def handle_response(response):
            try:
                body = await response.text()
                if XHR_MARKER in body:
                    captured_xml.append(body)
            except Exception:
                pass

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()
            page.on("response", handle_response)

            await page.goto(TARGET_URL, wait_until="networkidle")

            await page.click(".graph-previous-button")

            await page.wait_for_timeout(2500)
            await browser.close()

        return captured_xml

    xml_bodies = asyncio.run(run())

    if not xml_bodies:
        raise ValueError(f"No XML captured for {target_date}")

    rows = []
    for xml_text in xml_bodies:
        root = ET.fromstring(xml_text)
        for donnees in root.iter("donneesMarche"):
            date_str = donnees.attrib.get("date")
            if not date_str:
                continue
            base_dt = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            fr_node = donnees.find(".//type[@perimetre='FR']")
            if fr_node is None:
                continue
            for valeur in fr_node.findall("valeur"):
                if valeur.text is None:
                    continue
                periode = int(valeur.attrib["periode"])
                ts = base_dt + timedelta(minutes=periode * 15)
                rows.append((ts, float(valeur.text), date_str, periode))

    if not rows:
        raise ValueError(f"XML captured but no FR rows parsed for {target_date}")

    sql = """
        CREATE SCHEMA IF NOT EXISTS staging;
        CREATE TABLE IF NOT EXISTS staging.rte_spot_prices(
            timestamp TIMESTAMP PRIMARY KEY,
            price_eur_mwh DOUBLE PRECISION,
            source_date DATE,
            periode INT
        );
        INSERT INTO staging.rte_spot_prices (timestamp, price_eur_mwh, source_date, periode)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (timestamp) DO UPDATE SET
            price_eur_mwh = EXCLUDED.price_eur_mwh,
            source_date   = EXCLUDED.source_date,
            periode       = EXCLUDED.periode;
    """
    PostgresHook(postgres_conn_id=POSTGRES_CONN_ID).run(sql, parameters=rows, autocommit=True)
    print(f"Upserted {len(rows)} rows for {target_date}")
    print(rows)