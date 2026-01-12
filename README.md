# SA Power Networks Exporter

Scrapes electricity interval data from the SA Power Networks customer portal and stores it in InfluxDB.

## Features

- Automated login to SAPN customer portal
- Parses NEM12 interval meter data (5-minute granularity, 288 readings/day)
- Stores data in InfluxDB with correct timestamps
- Daily scheduled scraping

## Quick Start

1. Create your environment file:
```bash
cp .env.example .env
```

2. Edit `.env` with your credentials:
```bash
# Generate a token for InfluxDB
openssl rand -hex 32
```

3. Start the stack:
```bash
docker compose up -d
```

4. Access InfluxDB at http://localhost:8086 (admin/adminpassword)

## Environment Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `SAPN_USERNAME` | Yes | - | SAPN portal email |
| `SAPN_PASSWORD` | Yes | - | SAPN portal password |
| `SAPN_NMI` | Yes | - | National Meter Identifier |
| `INFLUXDB_TOKEN` | Yes | - | InfluxDB API token |
| `INFLUXDB_URL` | No | `http://localhost:8086` | InfluxDB URL |
| `INFLUXDB_ORG` | No | `sapn` | InfluxDB organization |
| `INFLUXDB_BUCKET` | No | `electricity` | InfluxDB bucket |
| `SCRAPE_HOUR` | No | `4` | Hour (0-23) for daily scrape |

## InfluxDB Measurements

| Measurement | Tags | Fields | Description |
|-------------|------|--------|-------------|
| `sapn_electricity` | nmi | kwh | 5-minute interval readings |
| `sapn_daily_total` | nmi | kwh | Daily totals |
| `sapn_scrape` | nmi | success, duration_seconds, readings_count | Scrape status |

## Example Flux Queries

**5-minute interval data:**
```flux
from(bucket: "electricity")
  |> range(start: -7d)
  |> filter(fn: (r) => r._measurement == "sapn_electricity")
  |> filter(fn: (r) => r._field == "kwh")
```

**Daily totals:**
```flux
from(bucket: "electricity")
  |> range(start: -30d)
  |> filter(fn: (r) => r._measurement == "sapn_daily_total")
  |> filter(fn: (r) => r._field == "kwh")
```

## Development

```bash
pip install -r requirements.txt
python -m src.main
```

## License

MIT
