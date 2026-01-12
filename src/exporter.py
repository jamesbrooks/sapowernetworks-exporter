"""Prometheus metrics exporter module.

This module handles:
- Defining Prometheus metrics (gauges, counters)
- Exposing metrics HTTP server on configurable port
- Updating metrics with parsed NEM12 data
"""

import logging
import time
from typing import Optional, Set, Tuple

from prometheus_client import Gauge, start_http_server, REGISTRY, CollectorRegistry

try:
    from src.nem12_parser import NEM12Data, IntervalReading, get_daily_total, get_latest_date, get_dates, interval_to_epoch
except ImportError:
    from nem12_parser import NEM12Data, IntervalReading, get_daily_total, get_latest_date, get_dates, interval_to_epoch

# Configure module logger
logger = logging.getLogger(__name__)


class SAPNExporter:
    """Prometheus exporter for SAPN electricity data.

    Exposes the following metrics:
    - sapn_electricity_kwh: Per-interval readings with labels (nmi, date, interval)
    - sapn_electricity_daily_total_kwh: Daily aggregates with labels (nmi, date)
    - sapn_last_reading_date: Most recent reading date per NMI
    - sapn_scrape_success: Whether the last scrape succeeded (1=success, 0=failure)
    - sapn_scrape_timestamp: Unix timestamp of last scrape
    - sapn_scrape_duration_seconds: Duration of last scrape operation

    Attributes:
        port: HTTP server port (default 9120)
        max_days: Maximum number of days to expose interval data for (default 7)
    """

    def __init__(
        self,
        port: int = 9120,
        max_days: int = 7,
        registry: Optional[CollectorRegistry] = None
    ):
        """Initialize the exporter.

        Args:
            port: Port to run the HTTP server on
            max_days: Maximum days of interval data to expose (to avoid metric explosion)
            registry: Optional custom registry for testing. If None, uses default REGISTRY.
        """
        self.port = port
        self.max_days = max_days
        self._registry = registry if registry is not None else REGISTRY
        self._server_started = False

        # Per-interval electricity readings (288 per day, 5-minute intervals)
        self._electricity_kwh = Gauge(
            'sapn_electricity_kwh',
            'Electricity consumption in kWh for a 5-minute interval',
            ['nmi', 'date', 'interval', 'epoch'],
            registry=self._registry
        )

        # Daily aggregate totals
        self._daily_total_kwh = Gauge(
            'sapn_electricity_daily_total_kwh',
            'Total daily electricity consumption in kWh',
            ['nmi', 'date'],
            registry=self._registry
        )

        # Last reading date per NMI (as YYYYMMDD integer for easy comparison)
        self._last_reading_date = Gauge(
            'sapn_last_reading_date',
            'Most recent reading date in YYYYMMDD format',
            ['nmi'],
            registry=self._registry
        )

        # Operational metrics (no labels)
        self._scrape_success = Gauge(
            'sapn_scrape_success',
            'Whether the last scrape succeeded (1=success, 0=failure)',
            registry=self._registry
        )

        self._scrape_timestamp = Gauge(
            'sapn_scrape_timestamp',
            'Unix timestamp of the last scrape',
            registry=self._registry
        )

        self._scrape_duration = Gauge(
            'sapn_scrape_duration_seconds',
            'Duration of the last scrape operation in seconds',
            registry=self._registry
        )

        # Track which label combinations we've set (for cleanup)
        self._active_interval_labels: Set[Tuple[str, str, str, str]] = set()
        self._active_daily_labels: Set[Tuple[str, str]] = set()
        self._active_nmi_labels: Set[Tuple[str]] = set()

    def update_metrics(self, nem12_data: NEM12Data) -> None:
        """Update all metrics from parsed NEM12 data.

        This method:
        1. Clears old metric values for this NMI
        2. Updates interval readings for the most recent N days
        3. Updates daily totals for all available dates
        4. Updates the last reading date

        Args:
            nem12_data: Parsed NEM12 data containing readings
        """
        nmi = nem12_data.nmi
        readings = nem12_data.readings

        if not readings:
            logger.warning("No readings to update metrics with")
            return

        # Get all unique dates and determine which to expose
        all_dates = get_dates(readings)
        latest_date = get_latest_date(readings)

        logger.info(f"Updating metrics for NMI {nmi} with {len(all_dates)} days of data")

        # For interval data, only expose the most recent N days
        recent_dates = all_dates[-self.max_days:] if len(all_dates) > self.max_days else all_dates

        # Clear old interval metrics for this NMI before updating
        old_labels_to_remove = {
            labels for labels in self._active_interval_labels
            if labels[0] == nmi and labels[1] not in recent_dates
        }
        for labels in old_labels_to_remove:
            try:
                self._electricity_kwh.remove(*labels)
            except KeyError:
                pass  # Label combination doesn't exist
            self._active_interval_labels.discard(labels)

        # Update interval readings for recent days
        for reading in readings:
            if reading.date in recent_dates:
                epoch = str(interval_to_epoch(reading.date, reading.interval))
                labels = (nmi, reading.date, str(reading.interval), epoch)
                self._electricity_kwh.labels(
                    nmi=nmi,
                    date=reading.date,
                    interval=str(reading.interval),
                    epoch=epoch
                ).set(reading.value)
                self._active_interval_labels.add(labels)

        # Update daily totals for all dates
        for date in all_dates:
            daily_total = get_daily_total(readings, date)
            labels = (nmi, date)
            self._daily_total_kwh.labels(nmi=nmi, date=date).set(daily_total)
            self._active_daily_labels.add(labels)

        # Update last reading date
        if latest_date:
            self._last_reading_date.labels(nmi=nmi).set(int(latest_date))
            self._active_nmi_labels.add((nmi,))

        logger.info(f"Metrics updated: {len(all_dates)} days, latest={latest_date}")

    def set_scrape_success(self, success: bool, duration: float) -> None:
        """Update operational metrics after a scrape attempt.

        Args:
            success: Whether the scrape succeeded
            duration: How long the scrape took in seconds
        """
        self._scrape_success.set(1 if success else 0)
        self._scrape_timestamp.set(time.time())
        self._scrape_duration.set(duration)

    def start(self) -> None:
        """Start the HTTP server to expose metrics.

        The server runs in a daemon thread and exposes metrics at:
        http://localhost:{port}/metrics
        """
        if self._server_started:
            logger.warning("Prometheus server already started")
            return

        logger.info(f"Starting Prometheus HTTP server on port {self.port}")
        start_http_server(self.port, registry=self._registry)
        self._server_started = True
        print(f"Prometheus metrics server started on port {self.port}")


if __name__ == "__main__":
    # Test block: Create sample data and verify metrics
    import sys
    from prometheus_client import generate_latest, CollectorRegistry

    def test_exporter_basic():
        """Test basic exporter functionality."""
        print("Testing SAPNExporter basic...", end=" ")

        # Create a fresh registry for testing
        registry = CollectorRegistry()
        exporter = SAPNExporter(port=9120, max_days=7, registry=registry)

        # Create sample NEM12 data
        readings = []
        for interval in range(288):
            readings.append(IntervalReading(
                date="20260111",
                interval=interval,
                value=0.064,  # ~18.4 kWh total for the day
                quality="A"
            ))

        nem12_data = NEM12Data(
            nmi="YOUR_NMI",
            readings=readings,
            meter_serial="LG122283777"
        )

        # Update metrics
        exporter.update_metrics(nem12_data)

        # Generate metrics output
        output = generate_latest(registry).decode('utf-8')

        # Verify interval metrics exist
        assert 'sapn_electricity_kwh{' in output, "Missing sapn_electricity_kwh metric"
        assert 'nmi="YOUR_NMI"' in output, "Missing nmi label"
        assert 'date="20260111"' in output, "Missing date label"
        assert 'interval="0"' in output, "Missing interval 0"
        assert 'interval="287"' in output, "Missing interval 287"

        # Verify daily total exists
        assert 'sapn_electricity_daily_total_kwh{' in output, "Missing daily total metric"

        # Verify last reading date exists
        assert 'sapn_last_reading_date{' in output, "Missing last reading date metric"

        print("OK")

    def test_exporter_scrape_metrics():
        """Test scrape success metrics."""
        print("Testing SAPNExporter scrape metrics...", end=" ")

        registry = CollectorRegistry()
        exporter = SAPNExporter(port=9120, registry=registry)

        # Set scrape success
        exporter.set_scrape_success(success=True, duration=12.5)

        output = generate_latest(registry).decode('utf-8')

        assert 'sapn_scrape_success 1.0' in output, "scrape_success should be 1.0"
        assert 'sapn_scrape_duration_seconds 12.5' in output, "duration should be 12.5"
        assert 'sapn_scrape_timestamp' in output, "Missing scrape_timestamp"

        # Set scrape failure
        exporter.set_scrape_success(success=False, duration=5.0)
        output = generate_latest(registry).decode('utf-8')

        assert 'sapn_scrape_success 0.0' in output, "scrape_success should be 0.0"
        assert 'sapn_scrape_duration_seconds 5.0' in output, "duration should be 5.0"

        print("OK")

    def test_exporter_max_days():
        """Test that only recent N days are exposed for interval data."""
        print("Testing SAPNExporter max_days limit...", end=" ")

        registry = CollectorRegistry()
        exporter = SAPNExporter(port=9120, max_days=3, registry=registry)

        # Create 5 days of data
        readings = []
        dates = ["20260107", "20260108", "20260109", "20260110", "20260111"]
        for date in dates:
            for interval in range(288):
                readings.append(IntervalReading(
                    date=date,
                    interval=interval,
                    value=0.064,
                    quality="A"
                ))

        nem12_data = NEM12Data(
            nmi="YOUR_NMI",
            readings=readings,
            meter_serial="LG122283777"
        )

        exporter.update_metrics(nem12_data)

        output = generate_latest(registry).decode('utf-8')

        # Extract just the interval metric lines
        interval_lines = [line for line in output.split('\n')
                         if line.startswith('sapn_electricity_kwh{')]

        # Check that old dates are NOT in interval metrics
        old_date_lines = [line for line in interval_lines if 'date="20260107"' in line or 'date="20260108"' in line]
        assert len(old_date_lines) == 0, f"Old dates should not be in interval data: {old_date_lines[:3]}"

        # Check that recent dates ARE in interval metrics
        recent_date_lines = [line for line in interval_lines if 'date="20260109"' in line]
        assert len(recent_date_lines) > 0, "Recent date 20260109 should be in interval data"

        # Daily totals should exist for ALL days (labels are sorted alphabetically)
        assert 'date="20260107"' in output and 'sapn_electricity_daily_total_kwh' in output
        assert 'date="20260111"' in output and 'sapn_electricity_daily_total_kwh' in output

        print("OK")

    def test_exporter_daily_total_calculation():
        """Test that daily totals are calculated correctly."""
        print("Testing SAPNExporter daily total calculation...", end=" ")

        registry = CollectorRegistry()
        exporter = SAPNExporter(port=9120, registry=registry)

        # Create readings with known values
        readings = []
        for interval in range(288):
            readings.append(IntervalReading(
                date="20260111",
                interval=interval,
                value=0.1,  # 0.1 kWh * 288 intervals = 28.8 kWh
                quality="A"
            ))

        nem12_data = NEM12Data(
            nmi="YOUR_NMI",
            readings=readings,
            meter_serial="LG122283777"
        )

        exporter.update_metrics(nem12_data)

        output = generate_latest(registry).decode('utf-8')

        # Check daily total is approximately 28.8 (allow for floating point precision)
        import re
        match = re.search(r'sapn_electricity_daily_total_kwh\{[^}]+\}\s+(\S+)', output)
        assert match, "Could not find daily total metric"
        daily_total = float(match.group(1))
        assert abs(daily_total - 28.8) < 0.01, f"Daily total should be ~28.8, got {daily_total}"

        print("OK")

    def test_exporter_last_reading_date():
        """Test last reading date metric."""
        print("Testing SAPNExporter last reading date...", end=" ")

        registry = CollectorRegistry()
        exporter = SAPNExporter(port=9120, registry=registry)

        # Create readings for multiple days (not in order)
        readings = []
        dates = ["20260109", "20260111", "20260110"]
        for date in dates:
            readings.append(IntervalReading(
                date=date,
                interval=0,
                value=0.1,
                quality="A"
            ))

        nem12_data = NEM12Data(
            nmi="YOUR_NMI",
            readings=readings,
            meter_serial="LG122283777"
        )

        exporter.update_metrics(nem12_data)

        output = generate_latest(registry).decode('utf-8')

        # Last reading date should be 20260111 (most recent)
        # Prometheus may format as scientific notation or float
        assert 'sapn_last_reading_date{nmi="YOUR_NMI"}' in output, "Missing last_reading_date"
        # Check that the value is 20260111 in some format
        import re
        match = re.search(r'sapn_last_reading_date\{nmi="YOUR_NMI"\}\s+(\S+)', output)
        assert match, "Could not parse last_reading_date value"
        value = float(match.group(1))
        assert abs(value - 20260111) < 1, f"Expected 20260111, got {value}"

        print("OK")

    def test_exporter_output_format():
        """Test the exact output format matches requirements."""
        print("Testing SAPNExporter output format...", end=" ")

        registry = CollectorRegistry()
        exporter = SAPNExporter(port=9120, max_days=7, registry=registry)

        # Create sample data matching the requirements example
        readings = [
            IntervalReading(date="20260111", interval=0, value=0.134, quality="A"),
            IntervalReading(date="20260111", interval=287, value=0.098, quality="A"),
        ]

        nem12_data = NEM12Data(
            nmi="YOUR_NMI",
            readings=readings,
            meter_serial="LG122283777"
        )

        exporter.update_metrics(nem12_data)
        exporter.set_scrape_success(success=True, duration=12.5)

        output = generate_latest(registry).decode('utf-8')

        # Print sample output for verification
        print("\n  Sample metrics output:")
        for line in output.split('\n'):
            if line and not line.startswith('#'):
                print(f"    {line}")

        # Verify specific values from requirements (labels may be in different order)
        assert 'interval="0"' in output and '0.134' in output
        assert 'interval="287"' in output and '0.098' in output

        print("OK")

    def test_empty_readings():
        """Test handling of empty readings."""
        print("Testing SAPNExporter empty readings...", end=" ")

        registry = CollectorRegistry()
        exporter = SAPNExporter(port=9120, registry=registry)

        nem12_data = NEM12Data(
            nmi="YOUR_NMI",
            readings=[],
            meter_serial="LG122283777"
        )

        # Should not raise an exception
        exporter.update_metrics(nem12_data)

        output = generate_latest(registry).decode('utf-8')

        # No electricity metrics should be set
        assert 'sapn_electricity_kwh{' not in output
        assert 'sapn_electricity_daily_total_kwh{' not in output
        assert 'sapn_last_reading_date{' not in output

        print("OK")

    # Run all tests
    print("=" * 60)
    print("SAPN Exporter Unit Tests")
    print("=" * 60)

    tests = [
        test_exporter_basic,
        test_exporter_scrape_metrics,
        test_exporter_max_days,
        test_exporter_daily_total_calculation,
        test_exporter_last_reading_date,
        test_exporter_output_format,
        test_empty_readings,
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
