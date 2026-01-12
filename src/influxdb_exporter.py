"""InfluxDB exporter module.

This module handles pushing NEM12 interval data to InfluxDB with actual timestamps,
enabling proper time-series graphing in Grafana.
"""

import logging
import time
from typing import Optional, List

from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

try:
    from src.nem12_parser import NEM12Data, interval_to_epoch
except ImportError:
    from nem12_parser import NEM12Data, interval_to_epoch

logger = logging.getLogger(__name__)


class InfluxDBExporter:
    """InfluxDB exporter for SAPN electricity data.

    Pushes interval readings to InfluxDB with their actual timestamps,
    enabling proper time-series visualization in Grafana.

    Measurements written:
    - sapn_electricity: Per-interval readings (kWh) with actual timestamps
    - sapn_daily_total: Daily aggregates (kWh)
    - sapn_scrape: Operational metrics (success, duration)
    """

    def __init__(
        self,
        url: str = "http://localhost:8086",
        token: str = "",
        org: str = "sapn",
        bucket: str = "electricity",
    ):
        """Initialize the InfluxDB exporter.

        Args:
            url: InfluxDB server URL
            token: InfluxDB API token
            org: InfluxDB organization name
            bucket: InfluxDB bucket name
        """
        self.url = url
        self.token = token
        self.org = org
        self.bucket = bucket
        self._client: Optional[InfluxDBClient] = None
        self._write_api = None

    def connect(self) -> bool:
        """Connect to InfluxDB.

        Returns:
            True if connection successful, False otherwise
        """
        try:
            self._client = InfluxDBClient(
                url=self.url,
                token=self.token,
                org=self.org
            )
            self._write_api = self._client.write_api(write_options=SYNCHRONOUS)

            health = self._client.health()
            if health.status == "pass":
                logger.info(f"Connected to InfluxDB at {self.url}")
                return True
            else:
                logger.error(f"InfluxDB health check failed: {health.message}")
                return False
        except Exception as e:
            logger.error(f"Failed to connect to InfluxDB: {e}")
            return False

    def close(self) -> None:
        """Close the InfluxDB connection."""
        if self._client:
            self._client.close()
            self._client = None
            self._write_api = None
            logger.info("InfluxDB connection closed")

    def write_readings(self, nem12_data: NEM12Data) -> int:
        """Write interval readings to InfluxDB.

        Each reading is written as a point with its actual timestamp
        (derived from date + interval number).

        Args:
            nem12_data: Parsed NEM12 data containing readings

        Returns:
            Number of points written
        """
        if not self._write_api:
            raise RuntimeError("Not connected to InfluxDB")

        nmi = nem12_data.nmi
        readings = nem12_data.readings

        if not readings:
            logger.warning("No readings to write")
            return 0

        points: List[Point] = []
        for reading in readings:
            epoch_ns = interval_to_epoch(reading.date, reading.interval) * 1_000_000_000
            point = (
                Point("sapn_electricity")
                .tag("nmi", nmi)
                .field("kwh", reading.value)
                .time(epoch_ns, WritePrecision.NS)
            )
            points.append(point)

        self._write_api.write(bucket=self.bucket, org=self.org, record=points)
        logger.info(f"Wrote {len(points)} interval readings for NMI {nmi}")
        return len(points)

    def write_daily_totals(self, nem12_data: NEM12Data) -> int:
        """Write daily total aggregates to InfluxDB.

        Args:
            nem12_data: Parsed NEM12 data containing readings

        Returns:
            Number of points written
        """
        if not self._write_api:
            raise RuntimeError("Not connected to InfluxDB")

        nmi = nem12_data.nmi
        readings = nem12_data.readings

        if not readings:
            return 0

        daily_totals: dict[str, float] = {}
        for reading in readings:
            daily_totals[reading.date] = daily_totals.get(reading.date, 0.0) + reading.value

        points: List[Point] = []
        for date, total in daily_totals.items():
            epoch_ns = interval_to_epoch(date, 0) * 1_000_000_000
            point = (
                Point("sapn_daily_total")
                .tag("nmi", nmi)
                .field("kwh", total)
                .time(epoch_ns, WritePrecision.NS)
            )
            points.append(point)

        self._write_api.write(bucket=self.bucket, org=self.org, record=points)
        logger.info(f"Wrote {len(points)} daily totals for NMI {nmi}")
        return len(points)

    def write_scrape_status(self, nmi: str, success: bool, duration: float, readings_count: int = 0) -> None:
        """Write scrape operational metrics to InfluxDB.

        Args:
            nmi: The NMI that was scraped
            success: Whether the scrape succeeded
            duration: How long the scrape took in seconds
            readings_count: Number of readings written (0 if failed)
        """
        if not self._write_api:
            raise RuntimeError("Not connected to InfluxDB")

        point = (
            Point("sapn_scrape")
            .tag("nmi", nmi)
            .field("success", 1 if success else 0)
            .field("duration_seconds", duration)
            .field("readings_count", readings_count)
            .time(time.time_ns(), WritePrecision.NS)
        )

        self._write_api.write(bucket=self.bucket, org=self.org, record=point)
        logger.info(f"Wrote scrape status: success={success}, duration={duration:.2f}s")

    def write_all(self, nem12_data: NEM12Data) -> tuple[int, int]:
        """Write both interval readings and daily totals.

        Args:
            nem12_data: Parsed NEM12 data

        Returns:
            Tuple of (interval_count, daily_count)
        """
        interval_count = self.write_readings(nem12_data)
        daily_count = self.write_daily_totals(nem12_data)
        return interval_count, daily_count
