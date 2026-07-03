# Stock Market Daily Pipeline

A daily batch data pipeline that ingests stock market data, processes it through
Bronze -> Silver -> Gold layers (Medallion Architecture) on GCP, orchestrated by Apache Airflow.

## Status
🚧 Under active development. See commit history for progress by phase.

## Tech Stack
- Apache Airflow (Docker Compose, local)
- Google Cloud Storage (Bronze, Silver)
- BigQuery (Silver, Gold)
- Python / yfinance
- Docker

## Setup
See individual phase docs (coming in Phase 5 writeup).
