"""Main entry point for SAPN Exporter.

This module handles:
- Loading configuration from environment variables
- Scheduling periodic data fetches with APScheduler
- Coordinating scraper, parser, and exporter components
- Pushing data to InfluxDB for proper time-series graphing
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
from src.exporter import SAPNExporter
from src.influxdb_exporter import InfluxDBExporter

# Configure module logger
logger = logging.getLogger(__name__)

# Global exporter instances (shared across scrape runs)
prometheus_exporter: Optional[SAPNExporter] = None
influxdb_exporter: Optional[InfluxDBExporter] = None

# Configuration from environment
config = {
    "username": "",
    "password": "",
    "nmi": "",
    "scrape_hour": 4,
    "exporter_port": 9120,
    # InfluxDB config
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
        EXPORTER_PORT: Prometheus port (default: 9120)
        INFLUXDB_URL: InfluxDB server URL (default: http://localhost:8086)
        INFLUXDB_ORG: InfluxDB organization (default: sapn)
        INFLUXDB_BUCKET: InfluxDB bucket (default: electricity)

    Returns:
        True if all required config loaded, False otherwise
    """
    config["username"] = os.getenv("SAPN_USERNAME", "")
    config["password"] = os.getenv("SAPN_PASSWORD", "")
    config["nmi"] = os.getenv("SAPN_NMI", "")

    # Optional with defaults
    try:
        config["scrape_hour"] = int(os.getenv("SCRAPE_HOUR", "4"))
    except ValueError:
        logger.warning("Invalid SCRAPE_HOUR, using default: 4")
        config["scrape_hour"] = 4

    try:
        config["exporter_port"] = int(os.getenv("EXPORTER_PORT", "9120"))
    except ValueError:
        logger.warning("Invalid EXPORTER_PORT, using default: 9120")
        config["exporter_port"] = 9120

    # InfluxDB configuration
    config["influxdb_url"] = os.getenv("INFLUXDB_URL", "http://localhost:8086")
    config["influxdb_token"] = os.getenv("INFLUXDB_TOKEN", "")
    config["influxdb_org"] = os.getenv("INFLUXDB_ORG", "sapn")
    config["influxdb_bucket"] = os.getenv("INFLUXDB_BUCKET", "electricity")

    # Validate required config
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

    logger.info(f"Configuration loaded: NMI={config['nmi']}, "
                f"scrape_hour={config['scrape_hour']}, "
                f"influxdb_url={config['influxdb_url']}")
    return True


def run_scrape() -> bool:
    """Execute the scrape, parse, and export flow.

    This function:
    1. Creates SAPNScraper and logs in
    2. Downloads NEM12 data
    3. Parses with parse_nem12()
    4. Pushes data to InfluxDB with proper timestamps
    5. Updates Prometheus operational metrics
    6. Logs success/failure

    Returns:
        True if scrape succeeded, False otherwise
    """
    global prometheus_exporter, influxdb_exporter

    logger.info("Starting scheduled scrape")
    start_time = time.time()

    try:
        # Create scraper and authenticate
        scraper = SAPNScraper(
            username=config["username"],
            password=config["password"],
            nmi=config["nmi"]
        )

        logger.info(f"Logging in as {config['username']}")
        scraper.login()

        # Download NEM12 data
        logger.info("Downloading NEM12 data")
        csv_content = scraper.download_nem12()

        # Parse the data
        logger.info("Parsing NEM12 data")
        data = parse_nem12(csv_content)

        logger.info(f"Parsed {len(data.readings)} readings for NMI {data.nmi}")

        # Push to InfluxDB (main data storage)
        if influxdb_exporter:
            interval_count, daily_count = influxdb_exporter.write_all(data)
            logger.info(f"Wrote to InfluxDB: {interval_count} intervals, {daily_count} daily totals")

        # Update Prometheus operational metrics
        if prometheus_exporter:
            prometheus_exporter.update_metrics(data)
            prometheus_exporter.set_scrape_success(True, time.time() - start_time)

        logger.info("Scrape completed successfully")
        return True

    except SAPNError as e:
        logger.error(f"Scrape failed (SAPN error): {e}")
        if prometheus_exporter:
            prometheus_exporter.set_scrape_success(False, time.time() - start_time)
        return False

    except NEM12ParseError as e:
        logger.error(f"Scrape failed (parse error): {e}")
        if prometheus_exporter:
            prometheus_exporter.set_scrape_success(False, time.time() - start_time)
        return False

    except Exception as e:
        logger.error(f"Scrape failed (unexpected error): {e}")
        if prometheus_exporter:
            prometheus_exporter.set_scrape_success(False, time.time() - start_time)
        return False


def main() -> int:
    """Main entry point.

    1. Load .env file with python-dotenv
    2. Load and validate configuration
    3. Connect to InfluxDB
    4. Start Prometheus HTTP server (for operational metrics)
    5. Start scheduler with daily scrape job
    6. Run initial scrape at startup
    7. Keep running (block on scheduler)

    Returns:
        Exit code (0 for success, 1 for failure)
    """
    global prometheus_exporter, influxdb_exporter

    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)]
    )

    logger.info("SAPN Exporter starting")

    # Load .env file
    load_dotenv()
    logger.info("Loaded .env file")

    # Load configuration
    if not load_config():
        logger.error("Configuration failed, exiting")
        return 1

    # Initialize InfluxDB exporter
    influxdb_exporter = InfluxDBExporter(
        url=config["influxdb_url"],
        token=config["influxdb_token"],
        org=config["influxdb_org"],
        bucket=config["influxdb_bucket"],
    )

    if not influxdb_exporter.connect():
        logger.error("Failed to connect to InfluxDB, exiting")
        return 1

    logger.info(f"Connected to InfluxDB at {config['influxdb_url']}")

    # Initialize Prometheus exporter (for operational metrics)
    prometheus_exporter = SAPNExporter(port=config["exporter_port"])
    prometheus_exporter.start()
    logger.info(f"Prometheus metrics available at http://localhost:{config['exporter_port']}/metrics")

    # Create scheduler
    scheduler = BlockingScheduler()

    # Add daily scrape job
    trigger = CronTrigger(hour=config["scrape_hour"], minute=0)
    scheduler.add_job(
        run_scrape,
        trigger=trigger,
        id="daily_scrape",
        name=f"Daily scrape at {config['scrape_hour']}:00"
    )
    logger.info(f"Scheduled daily scrape at {config['scrape_hour']}:00")

    # Run initial scrape at startup
    logger.info("Running initial scrape at startup")
    run_scrape()

    # Start scheduler (blocks)
    logger.info("Starting scheduler, press Ctrl+C to exit")
    try:
        scheduler.start()
    except KeyboardInterrupt:
        logger.info("Received interrupt, shutting down")
        scheduler.shutdown()
        if influxdb_exporter:
            influxdb_exporter.close()

    return 0


if __name__ == "__main__":
    sys.exit(main())
