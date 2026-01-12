"""Prometheus metrics exporter module.

This module handles:
- Defining Prometheus metrics (gauges, counters)
- Exposing metrics HTTP server on configurable port
- Updating metrics with parsed NEM12 data
"""

import logging
from typing import Optional

from prometheus_client import Gauge, Counter, start_http_server

from src.nem12_parser import NEM12Data, get_daily_total, get_latest_date, get_dates

# Configure module logger
logger = logging.getLogger(__name__)


class SAPNExporter:
    """Prometheus exporter for SAPN meter data.

    Exposes the following metrics:
    - sapn_daily_energy_kwh: Daily total energy consumption
    - sapn_latest_reading_timestamp: Timestamp of latest reading
    - sapn_scrape_success: Whether last scrape succeeded (1) or failed (0)
    - sapn_scrape_total: Total number of scrape attempts
    """

    def __init__(self, port: int = 9120):
        """Initialize the exporter.

        Args:
            port: HTTP port to expose metrics on (default: 9120)
        """
        self.port = port
        self._server_started = False

        # Define Prometheus metrics
        self.daily_energy = Gauge(
            "sapn_daily_energy_kwh",
            "Daily total energy consumption in kWh",
            ["nmi", "date"]
        )

        self.latest_reading_timestamp = Gauge(
            "sapn_latest_reading_timestamp",
            "Unix timestamp of the latest reading date",
            ["nmi"]
        )

        self.scrape_success = Gauge(
            "sapn_scrape_success",
            "Whether the last scrape succeeded (1) or failed (0)"
        )

        self.scrape_total = Counter(
            "sapn_scrape_total",
            "Total number of scrape attempts",
            ["status"]
        )

        self.data_days = Gauge(
            "sapn_data_days_total",
            "Total number of days of data available",
            ["nmi"]
        )

    def start_server(self) -> None:
        """Start the Prometheus HTTP server.

        The server exposes metrics on /metrics endpoint.
        """
        if self._server_started:
            logger.warning("Prometheus server already started")
            return

        logger.info(f"Starting Prometheus HTTP server on port {self.port}")
        start_http_server(self.port)
        self._server_started = True

    def update_metrics(self, data: NEM12Data) -> None:
        """Update Prometheus metrics with parsed NEM12 data.

        Args:
            data: Parsed NEM12 data containing readings
        """
        if not data.readings:
            logger.warning("No readings to update metrics with")
            return

        nmi = data.nmi
        dates = get_dates(data.readings)

        logger.info(f"Updating metrics for NMI {nmi} with {len(dates)} days of data")

        # Update daily energy for each date
        for date in dates:
            daily_total = get_daily_total(data.readings, date)
            self.daily_energy.labels(nmi=nmi, date=date).set(daily_total)

        # Update latest reading timestamp
        latest_date = get_latest_date(data.readings)
        if latest_date:
            # Convert YYYYMMDD to Unix timestamp (midnight UTC)
            from datetime import datetime
            dt = datetime.strptime(latest_date, "%Y%m%d")
            timestamp = dt.timestamp()
            self.latest_reading_timestamp.labels(nmi=nmi).set(timestamp)

        # Update total days count
        self.data_days.labels(nmi=nmi).set(len(dates))

        logger.info(f"Metrics updated: {len(dates)} days, latest={latest_date}")

    def set_scrape_success(self, success: bool) -> None:
        """Set the scrape success metric.

        Args:
            success: Whether the scrape succeeded
        """
        self.scrape_success.set(1 if success else 0)
        status = "success" if success else "failure"
        self.scrape_total.labels(status=status).inc()
