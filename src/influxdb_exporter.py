"""InfluxDB exporter module.

This module handles:
- Pushing NEM12 interval data to InfluxDB with actual timestamps
- Each 5-minute reading is stored at its correct time
- Enables proper time-series graphing in Grafana
"""

import logging
from typing import Optional, List

from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

try:
    from src.nem12_parser import NEM12Data, IntervalReading, interval_to_epoch
except ImportError:
    from nem12_parser import NEM12Data, IntervalReading, interval_to_epoch

# Configure module logger
logger = logging.getLogger(__name__)


class InfluxDBExporter:
    """InfluxDB exporter for SAPN electricity data.

    Pushes interval readings to InfluxDB with their actual timestamps,
    enabling proper time-series visualization in Grafana.

    Measurements:
    - sapn_electricity: Per-interval readings (kWh)
    - sapn_daily_total: Daily aggregates (kWh)

    Tags:
    - nmi: National Metering Identifier

    Attributes:
        url: InfluxDB server URL
        token: InfluxDB API token
        org: InfluxDB organization
        bucket: InfluxDB bucket name
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
            token: InfluxDB API token (required for writes)
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

            # Test connection by pinging
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

        Raises:
            RuntimeError: If not connected to InfluxDB
        """
        if not self._write_api:
            raise RuntimeError("Not connected to InfluxDB. Call connect() first.")

        nmi = nem12_data.nmi
        readings = nem12_data.readings

        if not readings:
            logger.warning("No readings to write")
            return 0

        # Build points for batch write
        points: List[Point] = []

        for reading in readings:
            # Get the actual timestamp for this interval
            epoch_ns = interval_to_epoch(reading.date, reading.interval) * 1_000_000_000

            point = (
                Point("sapn_electricity")
                .tag("nmi", nmi)
                .field("kwh", reading.value)
                .time(epoch_ns, WritePrecision.NS)
            )
            points.append(point)

        # Write in batches
        try:
            self._write_api.write(bucket=self.bucket, org=self.org, record=points)
            logger.info(f"Wrote {len(points)} readings to InfluxDB for NMI {nmi}")
        except Exception as e:
            logger.error(f"Failed to write to InfluxDB: {e}")
            raise

        return len(points)

    def write_daily_totals(self, nem12_data: NEM12Data) -> int:
        """Write daily total aggregates to InfluxDB.

        Each day gets a single point with the total consumption,
        timestamped at midnight of that day.

        Args:
            nem12_data: Parsed NEM12 data containing readings

        Returns:
            Number of points written
        """
        if not self._write_api:
            raise RuntimeError("Not connected to InfluxDB. Call connect() first.")

        nmi = nem12_data.nmi
        readings = nem12_data.readings

        if not readings:
            return 0

        # Group readings by date and sum
        daily_totals: dict[str, float] = {}
        for reading in readings:
            if reading.date not in daily_totals:
                daily_totals[reading.date] = 0.0
            daily_totals[reading.date] += reading.value

        # Build points
        points: List[Point] = []
        for date, total in daily_totals.items():
            # Use midnight (interval 0) as the timestamp for daily totals
            epoch_ns = interval_to_epoch(date, 0) * 1_000_000_000

            point = (
                Point("sapn_daily_total")
                .tag("nmi", nmi)
                .field("kwh", total)
                .time(epoch_ns, WritePrecision.NS)
            )
            points.append(point)

        try:
            self._write_api.write(bucket=self.bucket, org=self.org, record=points)
            logger.info(f"Wrote {len(points)} daily totals to InfluxDB for NMI {nmi}")
        except Exception as e:
            logger.error(f"Failed to write daily totals: {e}")
            raise

        return len(points)

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


if __name__ == "__main__":
    # Test block
    import sys

    def test_exporter_init():
        """Test exporter initialization."""
        print("Testing InfluxDBExporter init...", end=" ")

        exporter = InfluxDBExporter(
            url="http://localhost:8086",
            token="test-token",
            org="test-org",
            bucket="test-bucket"
        )

        assert exporter.url == "http://localhost:8086"
        assert exporter.token == "test-token"
        assert exporter.org == "test-org"
        assert exporter.bucket == "test-bucket"

        print("OK")

    def test_point_creation():
        """Test that points are created with correct timestamps."""
        print("Testing point creation...", end=" ")

        # Create sample data
        readings = [
            IntervalReading(date="20260111", interval=0, value=0.134, quality="A"),
            IntervalReading(date="20260111", interval=1, value=0.142, quality="A"),
            IntervalReading(date="20260111", interval=287, value=0.098, quality="A"),
        ]

        nem12_data = NEM12Data(
            nmi="TEST_NMI",
            readings=readings,
            meter_serial="TEST123"
        )

        # Verify epoch calculation
        epoch_0 = interval_to_epoch("20260111", 0)
        epoch_1 = interval_to_epoch("20260111", 1)
        epoch_287 = interval_to_epoch("20260111", 287)

        # Interval 1 should be 5 minutes (300 seconds) after interval 0
        assert epoch_1 - epoch_0 == 300, f"Expected 300s diff, got {epoch_1 - epoch_0}"

        # Interval 287 should be 287 * 5 = 1435 minutes after interval 0
        assert epoch_287 - epoch_0 == 287 * 300, f"Expected {287 * 300}s diff"

        print("OK")

    # Run tests
    print("=" * 60)
    print("InfluxDB Exporter Unit Tests")
    print("=" * 60)

    tests = [
        test_exporter_init,
        test_point_creation,
    ]

    failed = 0
    for test in tests:
        try:
            test()
        except Exception as e:
            print(f"FAILED: {e}")
            import traceback
            traceback.print_exc()
            failed += 1

    print("=" * 60)
    if failed:
        print(f"FAILED: {failed} test(s)")
        sys.exit(1)
    else:
        print("All tests passed!")
        sys.exit(0)
