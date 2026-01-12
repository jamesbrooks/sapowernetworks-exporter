"""NEM12 CSV format parser module.

This module handles:
- Parsing NEM12 interval meter data files from SA Power Networks
- Extracting energy consumption readings
- Converting interval data to time-series format

NEM12 format (simplified for SAPN):
- 200 record: NMI details (meter identifier, serial, unit, interval)
- 300 record: Interval data (date + 288 x 5-minute readings + quality)
- 400 record: Quality details (ignored for now)
"""

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import List, Optional
from zoneinfo import ZoneInfo


@dataclass
class IntervalReading:
    """A single 5-minute interval reading.

    Attributes:
        date: Date in YYYYMMDD format
        interval: Interval number (0-287, where 0 = 00:00-00:05)
        value: Energy consumption in kWh
        quality: Quality flag (A=actual, V=validated)
    """
    date: str
    interval: int
    value: float
    quality: str

    def __post_init__(self):
        """Validate interval reading data."""
        if not (0 <= self.interval <= 287):
            raise ValueError(f"Interval must be 0-287, got {self.interval}")
        if self.quality not in ('A', 'V', 'E', 'S', 'N', 'F'):
            raise ValueError(f"Unknown quality flag: {self.quality}")


@dataclass
class NEM12Data:
    """Parsed NEM12 file data.

    Attributes:
        nmi: National Metering Identifier
        readings: List of interval readings
        meter_serial: Meter serial number (optional)
        unit: Unit of measurement (default KWH)
        interval_length: Interval length in minutes (default 5)
    """
    nmi: str
    readings: List[IntervalReading] = field(default_factory=list)
    meter_serial: Optional[str] = None
    unit: str = "KWH"
    interval_length: int = 5


class NEM12ParseError(Exception):
    """Exception raised for NEM12 parsing errors."""
    pass


def parse_nem12(csv_content: str) -> NEM12Data:
    """Parse NEM12 CSV content into structured data.

    Args:
        csv_content: Raw CSV content as a string

    Returns:
        NEM12Data object containing NMI and all interval readings

    Raises:
        NEM12ParseError: If the content cannot be parsed

    Example:
        >>> content = '''200,2002261077,E1,E1,E1,,LG122283777,KWH,05,
        ... 300,20241206,0.134,0.130,...,A,,,20241207003712,'''
        >>> data = parse_nem12(content)
        >>> data.nmi
        '2002261077'
    """
    if not csv_content or not csv_content.strip():
        raise NEM12ParseError("Empty CSV content")

    nmi = None
    meter_serial = None
    unit = "KWH"
    interval_length = 5
    readings = []

    lines = csv_content.strip().split('\n')

    for line_num, line in enumerate(lines, 1):
        line = line.strip()
        if not line:
            continue

        # Split by comma
        fields = line.split(',')
        record_type = fields[0]

        if record_type == '200':
            # NMI Data Details record
            # Format: 200,{NMI},E1,E1,E1,,{meter_serial},KWH,05,
            if len(fields) < 9:
                raise NEM12ParseError(f"Line {line_num}: Invalid 200 record, expected at least 9 fields")

            nmi = fields[1]
            meter_serial = fields[6] if fields[6] else None
            unit = fields[7] if fields[7] else "KWH"

            # Parse interval length (05 = 5 minutes)
            try:
                interval_length = int(fields[8]) if fields[8] else 5
            except ValueError:
                interval_length = 5

        elif record_type == '300':
            # Interval Data record
            # Format: 300,{YYYYMMDD},{288 values},{quality},,,{timestamp},
            if len(fields) < 291:  # 1 + 1 + 288 + 1 minimum
                raise NEM12ParseError(f"Line {line_num}: Invalid 300 record, expected at least 291 fields, got {len(fields)}")

            date = fields[1]

            # Validate date format
            if len(date) != 8 or not date.isdigit():
                raise NEM12ParseError(f"Line {line_num}: Invalid date format: {date}")

            # Extract 288 interval values (fields 2-289)
            interval_values = fields[2:290]

            # Quality flag is at field 290
            quality = fields[290] if len(fields) > 290 else 'A'

            # Parse each interval value
            for interval_num, value_str in enumerate(interval_values):
                try:
                    value = float(value_str) if value_str else 0.0
                except ValueError:
                    raise NEM12ParseError(f"Line {line_num}: Invalid interval value at position {interval_num}: {value_str}")

                reading = IntervalReading(
                    date=date,
                    interval=interval_num,
                    value=value,
                    quality=quality
                )
                readings.append(reading)

        elif record_type == '400':
            # Quality method record - ignored for now
            pass

        elif record_type in ('100', '500', '900'):
            # 100: Header, 500: Reactive data, 900: End of data
            # Ignored for this simplified parser
            pass

    if nmi is None:
        raise NEM12ParseError("No 200 record found - missing NMI")

    return NEM12Data(
        nmi=nmi,
        readings=readings,
        meter_serial=meter_serial,
        unit=unit,
        interval_length=interval_length
    )


def get_daily_total(readings: List[IntervalReading], date: str) -> float:
    """Calculate total energy consumption for a specific date.

    Args:
        readings: List of IntervalReading objects
        date: Date in YYYYMMDD format

    Returns:
        Total kWh for the specified date

    Example:
        >>> total = get_daily_total(data.readings, "20241206")
        >>> print(f"Total: {total:.2f} kWh")
    """
    return sum(r.value for r in readings if r.date == date)


def get_latest_date(readings: List[IntervalReading]) -> Optional[str]:
    """Get the most recent date from the readings.

    Args:
        readings: List of IntervalReading objects

    Returns:
        Latest date in YYYYMMDD format, or None if no readings

    Example:
        >>> latest = get_latest_date(data.readings)
        >>> print(f"Latest data: {latest}")
    """
    if not readings:
        return None

    # Since dates are in YYYYMMDD format, string comparison works correctly
    return max(r.date for r in readings)


def get_dates(readings: List[IntervalReading]) -> List[str]:
    """Get all unique dates from the readings, sorted chronologically.

    Args:
        readings: List of IntervalReading objects

    Returns:
        Sorted list of unique dates in YYYYMMDD format
    """
    return sorted(set(r.date for r in readings))


def get_readings_for_date(readings: List[IntervalReading], date: str) -> List[IntervalReading]:
    """Get all readings for a specific date.

    Args:
        readings: List of IntervalReading objects
        date: Date in YYYYMMDD format

    Returns:
        List of readings for that date, sorted by interval
    """
    return sorted(
        [r for r in readings if r.date == date],
        key=lambda r: r.interval
    )


def interval_to_time(interval: int) -> str:
    """Convert interval number to time string.

    Args:
        interval: Interval number (0-287)

    Returns:
        Time string in HH:MM format

    Example:
        >>> interval_to_time(0)
        '00:00'
        >>> interval_to_time(12)
        '01:00'
        >>> interval_to_time(287)
        '23:55'
    """
    minutes = interval * 5
    hours = minutes // 60
    mins = minutes % 60
    return f"{hours:02d}:{mins:02d}"


def interval_to_epoch(date: str, interval: int, tz_name: str = "Australia/Adelaide") -> int:
    """Convert date and interval to Unix epoch timestamp.

    The timestamp represents the START of the 5-minute interval.
    The date/interval are interpreted in the specified timezone, then
    converted to UTC epoch.

    Args:
        date: Date in YYYYMMDD format
        interval: Interval number (0-287)
        tz_name: Timezone name (default: Australia/Adelaide for SAPN data)

    Returns:
        Unix epoch timestamp (seconds since 1970-01-01 00:00:00 UTC)

    Example:
        >>> interval_to_epoch("20260105", 0, "Australia/Adelaide")
        1767534600  # 2026-01-05 00:00 Adelaide = 2026-01-04 13:30 UTC (ACDT)
    """
    year = int(date[0:4])
    month = int(date[4:6])
    day = int(date[6:8])

    minutes = interval * 5
    hours = minutes // 60
    mins = minutes % 60

    tz = ZoneInfo(tz_name)
    dt = datetime(year, month, day, hours, mins, 0, tzinfo=tz)
    return int(dt.timestamp())


if __name__ == "__main__":
    # Unit tests
    import sys

    def test_interval_reading():
        """Test IntervalReading dataclass."""
        print("Testing IntervalReading...", end=" ")

        # Valid reading
        r = IntervalReading(date="20241206", interval=0, value=0.134, quality="A")
        assert r.date == "20241206"
        assert r.interval == 0
        assert r.value == 0.134
        assert r.quality == "A"

        # Test interval bounds
        try:
            IntervalReading(date="20241206", interval=-1, value=0.1, quality="A")
            assert False, "Should have raised ValueError"
        except ValueError:
            pass

        try:
            IntervalReading(date="20241206", interval=288, value=0.1, quality="A")
            assert False, "Should have raised ValueError"
        except ValueError:
            pass

        # Edge case: interval 287 should be valid
        r = IntervalReading(date="20241206", interval=287, value=0.1, quality="V")
        assert r.interval == 287

        print("OK")

    def test_parse_nem12_basic():
        """Test basic NEM12 parsing."""
        print("Testing parse_nem12 basic...", end=" ")

        csv_content = "200,2002261077,E1,E1,E1,,LG122283777,KWH,05,\n"
        csv_content += "300,20241206," + ",".join(["0.134"] * 288) + ",A,,,20241207003712,\n"

        data = parse_nem12(csv_content)
        assert data.nmi == "2002261077"
        assert data.meter_serial == "LG122283777"
        assert data.unit == "KWH"
        assert data.interval_length == 5
        assert len(data.readings) == 288
        assert data.readings[0].date == "20241206"
        assert data.readings[0].interval == 0
        assert data.readings[0].value == 0.134
        assert data.readings[0].quality == "A"

        print("OK")

    def test_parse_nem12_multiple_days():
        """Test parsing multiple days of data."""
        print("Testing parse_nem12 multiple days...", end=" ")

        csv_content = "200,2002261077,E1,E1,E1,,LG122283777,KWH,05,\n"
        csv_content += "300,20241206," + ",".join(["0.100"] * 288) + ",A,,,20241207003712,\n"
        csv_content += "300,20241207," + ",".join(["0.200"] * 288) + ",V,,,20241208003712,\n"

        data = parse_nem12(csv_content)
        assert len(data.readings) == 576  # 288 * 2

        # Check first day
        day1_readings = [r for r in data.readings if r.date == "20241206"]
        assert len(day1_readings) == 288
        assert all(r.value == 0.100 for r in day1_readings)
        assert all(r.quality == "A" for r in day1_readings)

        # Check second day
        day2_readings = [r for r in data.readings if r.date == "20241207"]
        assert len(day2_readings) == 288
        assert all(r.value == 0.200 for r in day2_readings)
        assert all(r.quality == "V" for r in day2_readings)

        print("OK")

    def test_get_daily_total():
        """Test daily total calculation."""
        print("Testing get_daily_total...", end=" ")

        csv_content = "200,2002261077,E1,E1,E1,,LG122283777,KWH,05,\n"
        csv_content += "300,20241206," + ",".join(["0.100"] * 288) + ",A,,,20241207003712,\n"

        data = parse_nem12(csv_content)
        total = get_daily_total(data.readings, "20241206")
        expected = 0.100 * 288
        assert abs(total - expected) < 0.001, f"Expected {expected}, got {total}"

        # Non-existent date should return 0
        total = get_daily_total(data.readings, "20241201")
        assert total == 0.0

        print("OK")

    def test_get_latest_date():
        """Test getting latest date."""
        print("Testing get_latest_date...", end=" ")

        csv_content = "200,2002261077,E1,E1,E1,,LG122283777,KWH,05,\n"
        csv_content += "300,20241206," + ",".join(["0.100"] * 288) + ",A,,,20241207003712,\n"
        csv_content += "300,20241210," + ",".join(["0.200"] * 288) + ",A,,,20241211003712,\n"
        csv_content += "300,20241208," + ",".join(["0.150"] * 288) + ",A,,,20241209003712,\n"

        data = parse_nem12(csv_content)
        latest = get_latest_date(data.readings)
        assert latest == "20241210"

        # Empty list should return None
        assert get_latest_date([]) is None

        print("OK")

    def test_interval_to_time():
        """Test interval to time conversion."""
        print("Testing interval_to_time...", end=" ")

        assert interval_to_time(0) == "00:00"
        assert interval_to_time(1) == "00:05"
        assert interval_to_time(12) == "01:00"
        assert interval_to_time(24) == "02:00"
        assert interval_to_time(144) == "12:00"
        assert interval_to_time(287) == "23:55"

        print("OK")

    def test_get_dates():
        """Test getting unique dates."""
        print("Testing get_dates...", end=" ")

        csv_content = "200,2002261077,E1,E1,E1,,LG122283777,KWH,05,\n"
        csv_content += "300,20241210," + ",".join(["0.100"] * 288) + ",A,,,20241211003712,\n"
        csv_content += "300,20241206," + ",".join(["0.100"] * 288) + ",A,,,20241207003712,\n"
        csv_content += "300,20241208," + ",".join(["0.150"] * 288) + ",A,,,20241209003712,\n"

        data = parse_nem12(csv_content)
        dates = get_dates(data.readings)
        assert dates == ["20241206", "20241208", "20241210"]

        print("OK")

    def test_parse_errors():
        """Test parsing error handling."""
        print("Testing parse errors...", end=" ")

        # Empty content
        try:
            parse_nem12("")
            assert False, "Should have raised NEM12ParseError"
        except NEM12ParseError:
            pass

        # Missing 200 record
        try:
            csv_content = "300,20241206," + ",".join(["0.100"] * 288) + ",A,,,20241207003712,\n"
            parse_nem12(csv_content)
            assert False, "Should have raised NEM12ParseError"
        except NEM12ParseError as e:
            assert "No 200 record" in str(e)

        # Invalid date format
        try:
            csv_content = "200,2002261077,E1,E1,E1,,LG122283777,KWH,05,\n"
            csv_content += "300,2024120," + ",".join(["0.100"] * 288) + ",A,,,20241207003712,\n"
            parse_nem12(csv_content)
            assert False, "Should have raised NEM12ParseError"
        except NEM12ParseError as e:
            assert "Invalid date format" in str(e)

        print("OK")

    def test_real_file():
        """Test parsing real NEM12 file."""
        print("Testing real file parsing...", end=" ")

        sample_file = "/Users/james/Sandbox/sapn_prometheus_scraper/YOUR_NMI_20240112_20260111_20260112131057_SAPN_DETAILED.csv"

        try:
            with open(sample_file, 'r') as f:
                content = f.read()
        except FileNotFoundError:
            print("SKIPPED (file not found)")
            return

        data = parse_nem12(content)

        # Verify NMI
        assert data.nmi == "2002261077", f"Expected NMI 2002261077, got {data.nmi}"

        # Verify meter serial
        assert data.meter_serial == "LG122283777"

        # Get unique dates
        dates = get_dates(data.readings)
        print(f"\n  Found {len(dates)} days of data")
        print(f"  Date range: {dates[0]} to {dates[-1]}")

        # Verify expected number of readings
        expected_readings = len(dates) * 288
        actual_readings = len(data.readings)
        assert actual_readings == expected_readings, f"Expected {expected_readings} readings, got {actual_readings}"

        # Calculate total for latest day
        latest = get_latest_date(data.readings)
        daily_total = get_daily_total(data.readings, latest)
        print(f"  Latest date: {latest}, daily total: {daily_total:.2f} kWh")

        # Sample some readings
        latest_readings = get_readings_for_date(data.readings, latest)
        print(f"  Sample readings from {latest}:")
        for r in latest_readings[:3]:
            print(f"    {interval_to_time(r.interval)}: {r.value:.4f} kWh ({r.quality})")

        print("OK")

    # Run all tests
    print("=" * 60)
    print("NEM12 Parser Unit Tests")
    print("=" * 60)

    tests = [
        test_interval_reading,
        test_parse_nem12_basic,
        test_parse_nem12_multiple_days,
        test_get_daily_total,
        test_get_latest_date,
        test_interval_to_time,
        test_get_dates,
        test_parse_errors,
        test_real_file,
    ]

    failed = 0
    for test in tests:
        try:
            test()
        except Exception as e:
            print(f"FAILED: {e}")
            failed += 1

    print("=" * 60)
    if failed:
        print(f"FAILED: {failed} test(s)")
        sys.exit(1)
    else:
        print("All tests passed!")
        sys.exit(0)
