"""Main entry point for SAPN Prometheus Scraper.

This module handles:
- Loading configuration from environment variables
- Scheduling periodic data fetches with APScheduler
- Coordinating scraper, parser, and exporter components
"""

import logging
import os
import sys
from typing import Optional

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from dotenv import load_dotenv

from src.scraper import SAPNScraper, SAPNError
from src.nem12_parser import parse_nem12, NEM12ParseError
from src.exporter import SAPNExporter

# Configure module logger
logger = logging.getLogger(__name__)

# Global exporter instance (shared across scrape runs)
exporter: Optional[SAPNExporter] = None

# Configuration from environment
config = {
    "username": "",
    "password": "",
    "nmi": "",
    "scrape_hour": 4,
    "exporter_port": 9120,
}


def load_config() -> bool:
    """Load configuration from environment variables.

    Required:
        SAPN_USERNAME: Portal username
        SAPN_PASSWORD: Portal password
        SAPN_NMI: National Metering Identifier

    Optional:
        SCRAPE_HOUR: Hour to run daily scrape (default: 4)
        EXPORTER_PORT: Prometheus port (default: 9120)

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

    # Validate required config
    missing = []
    if not config["username"]:
        missing.append("SAPN_USERNAME")
    if not config["password"]:
        missing.append("SAPN_PASSWORD")
    if not config["nmi"]:
        missing.append("SAPN_NMI")

    if missing:
        logger.error(f"Missing required environment variables: {', '.join(missing)}")
        return False

    logger.info(f"Configuration loaded: NMI={config['nmi']}, "
                f"scrape_hour={config['scrape_hour']}, "
                f"exporter_port={config['exporter_port']}")
    return True


def run_scrape() -> bool:
    """Execute the scrape, parse, and metrics update flow.

    This function:
    1. Creates SAPNScraper and logs in
    2. Downloads NEM12 data
    3. Parses with parse_nem12()
    4. Updates exporter metrics
    5. Logs success/failure
    6. Sets scrape_success metric

    Returns:
        True if scrape succeeded, False otherwise
    """
    global exporter

    logger.info("Starting scheduled scrape")

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

        # Update metrics
        if exporter:
            exporter.update_metrics(data)
            exporter.set_scrape_success(True)

        logger.info("Scrape completed successfully")
        return True

    except SAPNError as e:
        logger.error(f"Scrape failed (SAPN error): {e}")
        if exporter:
            exporter.set_scrape_success(False)
        return False

    except NEM12ParseError as e:
        logger.error(f"Scrape failed (parse error): {e}")
        if exporter:
            exporter.set_scrape_success(False)
        return False

    except Exception as e:
        logger.error(f"Scrape failed (unexpected error): {e}")
        if exporter:
            exporter.set_scrape_success(False)
        return False


def main() -> int:
    """Main entry point.

    1. Load .env file with python-dotenv
    2. Load and validate configuration
    3. Start exporter HTTP server
    4. Start scheduler with daily scrape job
    5. Run initial scrape at startup
    6. Keep running (block on scheduler)

    Returns:
        Exit code (0 for success, 1 for failure)
    """
    global exporter

    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)]
    )

    logger.info("SAPN Prometheus Scraper starting")

    # Load .env file
    load_dotenv()
    logger.info("Loaded .env file")

    # Load configuration
    if not load_config():
        logger.error("Configuration failed, exiting")
        return 1

    # Initialize exporter
    exporter = SAPNExporter(port=config["exporter_port"])
    exporter.start_server()
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

    return 0


if __name__ == "__main__":
    sys.exit(main())
