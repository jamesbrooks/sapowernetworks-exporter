# SA Power Networks Exporter

Scrapes electricity interval data from the SA Power Networks customer portal and stores it in InfluxDB.

## Features

- Automated login to SAPN customer portal
- Parses NEM12 interval meter data (5-minute granularity, 288 readings/day)
- Stores data in InfluxDB with correct timestamps
- Daily scheduled scraping

## Quick Start

1. Generate a token for InfluxDB authentication:
```bash
openssl rand -hex 32
```

2. Create a `docker-compose.yml` with your credentials:
```yaml
services:
  sapowernetworks-exporter:
    image: jamesbrooks/sapowernetworks-exporter
    environment:
      - SAPN_USERNAME=your-email@example.com
      - SAPN_PASSWORD=your-sapn-password
      - SAPN_NMI=your-nmi-number
      - INFLUXDB_URL=http://influxdb:8086
      - INFLUXDB_TOKEN=your-generated-token-here
      - INFLUXDB_ORG=sapn
      - INFLUXDB_BUCKET=electricity
      - SCRAPE_HOUR=4
    depends_on:
      influxdb:
        condition: service_healthy
    restart: unless-stopped

  influxdb:
    image: influxdb:2.7
    ports:
      - "8086:8086"
    volumes:
      - influxdb-data:/var/lib/influxdb2
    environment:
      - DOCKER_INFLUXDB_INIT_MODE=setup
      - DOCKER_INFLUXDB_INIT_USERNAME=admin
      - DOCKER_INFLUXDB_INIT_PASSWORD=adminpassword
      - DOCKER_INFLUXDB_INIT_ORG=sapn
      - DOCKER_INFLUXDB_INIT_BUCKET=electricity
      - DOCKER_INFLUXDB_INIT_ADMIN_TOKEN=your-generated-token-here
    healthcheck:
      test: ["CMD", "influx", "ping"]
      interval: 10s
      timeout: 5s
      retries: 5
    restart: unless-stopped

volumes:
  influxdb-data:
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
| `INFLUXDB_TOKEN` | Yes | - | InfluxDB API token (must match `DOCKER_INFLUXDB_INIT_ADMIN_TOKEN` on InfluxDB) |
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
