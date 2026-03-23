# RoofLink ETL Pipeline — Roadmap

## Current: v1.0.0 (Stable)
- [x] Multi-file CSV batch processing with schema validation
- [x] Intelligent data cleaning (missing values, duplicates, format normalization)
- [x] Google BigQuery cloud integration
- [x] Data quality reporting with cleaning action summaries
- [x] Interactive Jupyter notebook interface
- [x] Automated test suite (pytest)
- [x] Synthetic sample data (no real PII)
- [x] CodeQL security scanning
- [x] Enterprise standards (AGENTS.md, CHANGELOG.md, structured logging, release automation)

## v1.1.0 — Pipeline Hardening
- [ ] CLI mode (run without Jupyter): `python -m etl_pipeline --input ./data --output bigquery`
- [ ] Configurable cleaning rules via YAML (column mappings, validation thresholds)
- [ ] Schema drift detection — alert when incoming CSV columns change
- [ ] Detailed data profiling report (distributions, outliers, completeness scores)
- [ ] Retry logic with exponential backoff for BigQuery upload failures
- [ ] Structured logging integration (use logging_config.py for all pipeline steps)
- [ ] Progress logging: processed/total/pct at each pipeline stage

## v2.0.0 — Scheduled & Streaming
- [ ] Cron-based scheduled runs (GitHub Actions or system cron)
- [ ] Incremental processing (only new/changed files via hash comparison)
- [ ] Support for additional source formats (Excel .xlsx, Google Sheets API, Parquet)
- [ ] Data lineage tracking (source file → cleaned row → BigQuery table/row)
- [ ] Pipeline run history dashboard (SQLite-backed, simple web UI)
- [ ] Dead letter queue for rows that fail validation (review and re-process)

## v3.0.0 — Multi-Tenant & API
- [ ] REST API for triggering pipeline runs and checking status
- [ ] Multi-tenant support (separate BigQuery datasets per client)
- [ ] Webhook notifications on pipeline completion/failure
- [ ] Docker containerized deployment
- [ ] PyPI package publication (`pip install rooflink-etl`)
