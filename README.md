# SA Power Networks Exporter

A Docker-based service that scrapes electricity interval data from the SA Power Networks customer portal and exposes it as Prometheus metrics.

## Features

- **Authentication**: Automated login to SAPN customer portal (Salesforce Visualforce)
- **NEM12 Parsing**: Parses NEM12 interval meter data (5-minute granularity, 288 readings/day)
- **Prometheus Metrics**: Exposes consumption data with labels for NMI, date, and interval
- **Scheduled Scraping**: Daily automated data collection via APScheduler

## Quick Start

1. Clone the repository and create your environment file:
```bash
cp .env.example .env
# Edit .env with your SAPN credentials
```

2. Run with Docker Compose:
```bash
docker compose up -d
```

3. Access metrics at `http://localhost:9120/metrics`

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
      - SCRAPE_HOUR=4
    restart: unless-stopped
```

## Environment Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `SAPN_USERNAME` | Yes | - | SAPN portal email address |
| `SAPN_PASSWORD` | Yes | - | SAPN portal password |
| `SAPN_NMI` | Yes | - | National Meter Identifier |
| `SCRAPE_HOUR` | No | `4` | Hour of day (0-23) for daily scrape |
| `EXPORTER_PORT` | No | `9120` | Port for Prometheus metrics endpoint |

## Prometheus Metrics

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `sapn_electricity_kwh` | Gauge | nmi, date, interval | Per-interval consumption (kWh) |
| `sapn_electricity_daily_total_kwh` | Gauge | nmi, date | Daily total consumption (kWh) |
| `sapn_last_reading_date` | Gauge | nmi | Most recent reading date (YYYYMMDD) |
| `sapn_scrape_success` | Gauge | - | Last scrape status (1=success, 0=failure) |
| `sapn_scrape_timestamp` | Gauge | - | Unix timestamp of last scrape |
| `sapn_scrape_duration_seconds` | Gauge | - | Duration of last scrape operation |

## Prometheus Configuration

Add the following scrape config to your `prometheus.yml`:

```yaml
scrape_configs:
  - job_name: 'sapn'
    static_configs:
      - targets: ['sapowernetworks-exporter:9120']
    scrape_interval: 5m
```

## Example Queries

```promql
# Daily electricity consumption
sapn_electricity_daily_total_kwh{nmi="YOUR_NMI"}

# Average consumption per interval for today
avg(sapn_electricity_kwh{date="20260112"})

# Check if last scrape succeeded
sapn_scrape_success == 1
```

## Development

Run locally without Docker:
```bash
pip install -r requirements.txt
python -m src.main
```

## License

MIT
