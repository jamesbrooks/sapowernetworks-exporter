"""Main entry point for SAPN Exporter.

Scrapes electricity interval data from SA Power Networks and stores it in InfluxDB
for visualization in Grafana.
"""

import logging
import os
import sys
import time
from typing import Optional

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from dotenv import load_dotenv

from src.scraper import SAPNScraper, SAPNError
from src.nem12_parser import parse_nem12, NEM12ParseError
from src.influxdb_exporter import InfluxDBExporter

logger = logging.getLogger(__name__)

exporter: Optional[InfluxDBExporter] = None

config = {
    "username": "",
    "password": "",
    "nmi": "",
    "scrape_hour": 4,
    "influxdb_url": "http://localhost:8086",
    "influxdb_token": "",
    "influxdb_org": "sapn",
    "influxdb_bucket": "electricity",
}


def load_config() -> bool:
    """Load configuration from environment variables.

    Required:
        SAPN_USERNAME: Portal username
        SAPN_PASSWORD: Portal password
        SAPN_NMI: National Metering Identifier
        INFLUXDB_TOKEN: InfluxDB API token

    Optional:
        SCRAPE_HOUR: Hour to run daily scrape (default: 4)
        INFLUXDB_URL: InfluxDB server URL (default: http://localhost:8086)
        INFLUXDB_ORG: InfluxDB organization (default: sapn)
        INFLUXDB_BUCKET: InfluxDB bucket (default: electricity)

    Returns:
        True if all required config loaded, False otherwise
    """
    config["username"] = os.getenv("SAPN_USERNAME", "")
    config["password"] = os.getenv("SAPN_PASSWORD", "")
    config["nmi"] = os.getenv("SAPN_NMI", "")

    try:
        config["scrape_hour"] = int(os.getenv("SCRAPE_HOUR", "4"))
    except ValueError:
        config["scrape_hour"] = 4

    config["influxdb_url"] = os.getenv("INFLUXDB_URL", "http://localhost:8086")
    config["influxdb_token"] = os.getenv("INFLUXDB_TOKEN", "")
    config["influxdb_org"] = os.getenv("INFLUXDB_ORG", "sapn")
    config["influxdb_bucket"] = os.getenv("INFLUXDB_BUCKET", "electricity")

    missing = []
    if not config["username"]:
        missing.append("SAPN_USERNAME")
    if not config["password"]:
        missing.append("SAPN_PASSWORD")
    if not config["nmi"]:
        missing.append("SAPN_NMI")
    if not config["influxdb_token"]:
        missing.append("INFLUXDB_TOKEN")

    if missing:
        logger.error(f"Missing required environment variables: {', '.join(missing)}")
        return False

    logger.info(f"Configuration loaded: NMI={config['nmi']}, scrape_hour={config['scrape_hour']}")
    return True


def run_scrape() -> bool:
    """Execute the scrape, parse, and export flow.

    Returns:
        True if scrape succeeded, False otherwise
    """
    global exporter

    logger.info("Starting scheduled scrape")
    start_time = time.time()
    readings_count = 0

    try:
        scraper = SAPNScraper(
            username=config["username"],
            password=config["password"],
            nmi=config["nmi"]
        )

        logger.info(f"Logging in as {config['username']}")
        scraper.login()

        logger.info("Downloading NEM12 data")
        csv_content = scraper.download_nem12()

        logger.info("Parsing NEM12 data")
        data = parse_nem12(csv_content)
        logger.info(f"Parsed {len(data.readings)} readings for NMI {data.nmi}")

        if exporter:
            readings_count, daily_count = exporter.write_all(data)
            logger.info(f"Wrote to InfluxDB: {readings_count} intervals, {daily_count} daily totals")
            exporter.write_scrape_status(
                nmi=config["nmi"],
                success=True,
                duration=time.time() - start_time,
                readings_count=readings_count
            )

        logger.info("Scrape completed successfully")
        return True

    except (SAPNError, NEM12ParseError) as e:
        logger.error(f"Scrape failed: {e}")
        if exporter:
            exporter.write_scrape_status(
                nmi=config["nmi"],
                success=False,
                duration=time.time() - start_time
            )
        return False

    except Exception as e:
        logger.error(f"Scrape failed (unexpected): {e}")
        if exporter:
            exporter.write_scrape_status(
                nmi=config["nmi"],
                success=False,
                duration=time.time() - start_time
            )
        return False


def main() -> int:
    """Main entry point.

    Returns:
        Exit code (0 for success, 1 for failure)
    """
    global exporter

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)]
    )

    logger.info("SAPN Exporter starting")

    load_dotenv()

    if not load_config():
        return 1

    exporter = InfluxDBExporter(
        url=config["influxdb_url"],
        token=config["influxdb_token"],
        org=config["influxdb_org"],
        bucket=config["influxdb_bucket"],
    )

    if not exporter.connect():
        logger.error("Failed to connect to InfluxDB")
        return 1

    scheduler = BlockingScheduler()
    trigger = CronTrigger(hour=config["scrape_hour"], minute=0)
    scheduler.add_job(run_scrape, trigger=trigger, id="daily_scrape")
    logger.info(f"Scheduled daily scrape at {config['scrape_hour']}:00")

    logger.info("Running initial scrape")
    run_scrape()

    logger.info("Starting scheduler")
    try:
        scheduler.start()
    except KeyboardInterrupt:
        logger.info("Shutting down")
        scheduler.shutdown()
        exporter.close()

    return 0


if __name__ == "__main__":
    sys.exit(main())
