# SA Power Networks Exporter

Scrapes electricity interval data from the SA Power Networks customer portal and stores it in InfluxDB for visualization in Grafana.

## Features

- Automated login to SAPN customer portal
- Parses NEM12 interval meter data (5-minute granularity, 288 readings/day)
- Stores data in InfluxDB with correct timestamps for time-series graphing
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

4. Access services:
   - **Grafana**: http://localhost:3000 (admin/admin)
   - **InfluxDB**: http://localhost:8086 (admin/adminpassword)

## Grafana Setup

1. Open Grafana at http://localhost:3000
2. Go to **Connections** > **Data sources** > **Add data source**
3. Select **InfluxDB**
4. Configure:
   - **Query Language**: Flux
   - **URL**: http://influxdb:8086
   - **Organization**: sapn
   - **Token**: your INFLUXDB_TOKEN
   - **Default Bucket**: electricity
5. Click **Save & Test**

### Example Queries

**5-minute interval data (last 7 days):**
```flux
from(bucket: "electricity")
  |> range(start: -7d)
  |> filter(fn: (r) => r._measurement == "sapn_electricity")
  |> filter(fn: (r) => r._field == "kwh")
```

**Hourly averages:**
```flux
from(bucket: "electricity")
  |> range(start: -7d)
  |> filter(fn: (r) => r._measurement == "sapn_electricity")
  |> filter(fn: (r) => r._field == "kwh")
  |> aggregateWindow(every: 1h, fn: mean, createEmpty: false)
```

**Daily totals:**
```flux
from(bucket: "electricity")
  |> range(start: -30d)
  |> filter(fn: (r) => r._measurement == "sapn_daily_total")
  |> filter(fn: (r) => r._field == "kwh")
```

**Scrape status:**
```flux
from(bucket: "electricity")
  |> range(start: -7d)
  |> filter(fn: (r) => r._measurement == "sapn_scrape")
```

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

## Development

```bash
pip install -r requirements.txt
python -m src.main
```

## License

MIT
