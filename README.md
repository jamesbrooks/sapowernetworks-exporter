# SA Power Networks Exporter

A Docker-based service that scrapes electricity interval data from the SA Power Networks customer portal and stores it in InfluxDB for visualization in Grafana.

## Features

- **Authentication**: Automated login to SAPN customer portal (Salesforce Visualforce)
- **NEM12 Parsing**: Parses NEM12 interval meter data (5-minute granularity, 288 readings/day)
- **InfluxDB Storage**: Stores data with proper timestamps for time-series graphing
- **Grafana Ready**: Each data point has its actual timestamp, enabling proper visualization
- **Scheduled Scraping**: Daily automated data collection via APScheduler

## Quick Start

1. Clone the repository and create your environment file:
```bash
cp .env.example .env
# Edit .env with your SAPN credentials and generate an InfluxDB token
```

2. Generate a secure InfluxDB token:
```bash
openssl rand -hex 32
# Add this to INFLUXDB_TOKEN in your .env file
```

3. Run with Docker Compose:
```bash
docker compose up -d
```

4. Access the services:
   - **Grafana**: http://localhost:3000 (admin/admin)
   - **InfluxDB**: http://localhost:8086 (admin/adminpassword)
   - **Prometheus metrics**: http://localhost:9120/metrics

## Grafana Setup

1. Open Grafana at http://localhost:3000
2. Go to **Connections** > **Data sources** > **Add data source**
3. Select **InfluxDB**
4. Configure the data source:
   - **Query Language**: Flux
   - **URL**: http://influxdb:8086
   - **Organization**: sapn
   - **Token**: (your INFLUXDB_TOKEN from .env)
   - **Default Bucket**: electricity
5. Click **Save & Test**

### Example Flux Query

```flux
from(bucket: "electricity")
  |> range(start: -7d)
  |> filter(fn: (r) => r._measurement == "sapn_electricity")
  |> filter(fn: (r) => r._field == "kwh")
  |> aggregateWindow(every: 1h, fn: mean, createEmpty: false)
```

### Daily Total Query

```flux
from(bucket: "electricity")
  |> range(start: -30d)
  |> filter(fn: (r) => r._measurement == "sapn_daily_total")
  |> filter(fn: (r) => r._field == "kwh")
```

## Docker Compose

```yaml
services:
  sapowernetworks-exporter:
    image: jamesbrooks/sapowernetworks-exporter
    ports:
      - "9120:9120"
    environment:
      - SAPN_USERNAME=your@email.com
      - SAPN_PASSWORD=yourpassword
      - SAPN_NMI=your_nmi
      - INFLUXDB_URL=http://influxdb:8086
      - INFLUXDB_TOKEN=your-token
      - INFLUXDB_ORG=sapn
      - INFLUXDB_BUCKET=electricity
    depends_on:
      - influxdb
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
      - DOCKER_INFLUXDB_INIT_ADMIN_TOKEN=your-token
    restart: unless-stopped

  grafana:
    image: grafana/grafana:latest
    ports:
      - "3000:3000"
    volumes:
      - grafana-data:/var/lib/grafana
    depends_on:
      - influxdb
    restart: unless-stopped

volumes:
  influxdb-data:
  grafana-data:
```

## Environment Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `SAPN_USERNAME` | Yes | - | SAPN portal email address |
| `SAPN_PASSWORD` | Yes | - | SAPN portal password |
| `SAPN_NMI` | Yes | - | National Meter Identifier |
| `INFLUXDB_TOKEN` | Yes | - | InfluxDB API token |
| `INFLUXDB_URL` | No | `http://localhost:8086` | InfluxDB server URL |
| `INFLUXDB_ORG` | No | `sapn` | InfluxDB organization |
| `INFLUXDB_BUCKET` | No | `electricity` | InfluxDB bucket name |
| `SCRAPE_HOUR` | No | `4` | Hour of day (0-23) for daily scrape |
| `EXPORTER_PORT` | No | `9120` | Port for Prometheus metrics endpoint |

## InfluxDB Measurements

| Measurement | Tags | Fields | Description |
|-------------|------|--------|-------------|
| `sapn_electricity` | nmi | kwh | Per-interval consumption (5-min) |
| `sapn_daily_total` | nmi | kwh | Daily total consumption |

## Prometheus Metrics (Operational)

The exporter also exposes Prometheus metrics for monitoring the scraper itself:

| Metric | Type | Description |
|--------|------|-------------|
| `sapn_scrape_success` | Gauge | Last scrape status (1=success, 0=failure) |
| `sapn_scrape_timestamp` | Gauge | Unix timestamp of last scrape |
| `sapn_scrape_duration_seconds` | Gauge | Duration of last scrape operation |

## Development

Run locally without Docker:
```bash
pip install -r requirements.txt
python -m src.main
```

## Architecture

```
SAPN Portal → Scraper → NEM12 Parser → InfluxDB → Grafana
                                    ↘
                                      Prometheus (operational metrics)
```

The exporter:
1. Scrapes NEM12 data from SA Power Networks portal
2. Parses 5-minute interval readings
3. Pushes to InfluxDB with actual timestamps (not scrape time)
4. Exposes operational metrics to Prometheus

This architecture ensures each data point is stored at its correct time, enabling proper time-series visualization in Grafana.

## License

MIT
