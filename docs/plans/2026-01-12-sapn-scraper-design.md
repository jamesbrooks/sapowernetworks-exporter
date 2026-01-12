# SAPN Prometheus Scraper Design

## Overview

A Docker-based scraper that authenticates with SA Power Networks' meter data portal, downloads NEM12 interval data, and exposes it as Prometheus metrics for Grafana dashboards.

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Docker Container                         │
│  ┌─────────────┐    ┌─────────────┐    ┌────────────────┐  │
│  │   Scraper   │───▶│ NEM12 Parser│───▶│ Prometheus     │  │
│  │  (requests) │    │             │    │ Exporter :9120 │  │
│  └─────────────┘    └─────────────┘    └────────────────┘  │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
                    ┌──────────────────┐
                    │   Prometheus     │
                    └──────────────────┘
```

## Authentication Flow

The SAPN portal uses Salesforce Visualforce with dynamic ViewState tokens:

1. `GET /meterdata/CADSiteLogin` - Extract ViewState, ViewStateVersion, ViewStateMAC from HTML
2. `POST /meterdata/CADSiteLogin` - Submit credentials with ViewState tokens
3. Session cookies (`sid`, `sid_Client`, etc.) maintained for subsequent requests

## Data Download Flow

1. `GET /meterdata/CADRequestMeterData?selNMI={NMI}` - Load form page, extract new ViewState + ctx tokens
2. `POST /meterdata/apexremote` - Salesforce Remoting RPC call:
   ```json
   {
     "action": "CADRequestMeterDataController",
     "method": "downloadNMIData",
     "data": ["NMI", "SAPN", "from_date", "to_date", "Customer Access NEM12", "Detailed Report (CSV)", 7]
   }
   ```
3. Poll apexremote until CSV content is returned

## NEM12 Format

The downloaded CSV is a simplified NEM12 format:

- **200 record**: NMI details - `200,{NMI},E1,E1,E1,,{meter_serial},KWH,05,`
- **300 record**: Interval data - `300,{YYYYMMDD},{288 values},{quality_flag},,,{timestamp},`

Key details:
- 5-minute intervals (288 per day)
- Values in kWh
- Quality flags: A (actual), V (validated)

## Prometheus Metrics

```prometheus
# Per-interval readings (288 per day)
sapn_electricity_kwh{nmi="YOUR_NMI", date="20260111", interval="0"} 0.134
sapn_electricity_kwh{nmi="YOUR_NMI", date="20260111", interval="1"} 0.130
...
sapn_electricity_kwh{nmi="YOUR_NMI", date="20260111", interval="287"} 0.098

# Daily aggregates
sapn_electricity_daily_total_kwh{nmi="YOUR_NMI", date="20260111"} 18.45

# Operational metrics
sapn_last_reading_date{nmi="YOUR_NMI"} 20260111
sapn_scrape_success 1
sapn_scrape_timestamp 1736640000
sapn_scrape_duration_seconds 12.5
```

Grafana can reconstruct time-of-day: `interval * 5 minutes` from midnight.

## Scrape Strategy

- Download from SAPN once daily at configurable time (default: 4am)
- Cache parsed data in memory
- Serve cached metrics on every Prometheus scrape
- Full historical download each time (stateless, ~2MB)

## Project Structure

```
sapn_prometheus_scraper/
├── src/
│   ├── __init__.py
│   ├── scraper.py          # SAPN authentication & download
│   ├── nem12_parser.py     # Parse NEM12 CSV format
│   ├── exporter.py         # Prometheus metrics server
│   └── main.py             # Entry point, scheduler
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
├── .env.example
└── README.md
```

## Configuration

Environment variables:
```
SAPN_USERNAME=your_email@example.com
SAPN_PASSWORD=YOUR_PASSWORD
SAPN_NMI=YOUR_NMI
SCRAPE_HOUR=4
EXPORTER_PORT=9120
```

## Dependencies

- `requests` - HTTP client with session support
- `beautifulsoup4` - HTML parsing for ViewState extraction
- `prometheus_client` - Metrics exposition
- `apscheduler` - Daily scrape scheduling

## Error Handling

- Retry with exponential backoff on network failures
- Detect login failures (redirect back to login page)
- Set `sapn_scrape_success=0` on any failure
- Log errors for debugging
