"""Main entry point for SAPN Prometheus Scraper.

This module handles:
- Loading configuration from environment variables
- Scheduling periodic data fetches with APScheduler
- Coordinating scraper, parser, and exporter components
"""
