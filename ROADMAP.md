# RoofLink ETL Pipeline — Roadmap

## Current: v1.0.0 (Stable)
- [x] Multi-file CSV batch processing with schema validation
- [x] Intelligent data cleaning (missing values, duplicates, format normalization)
- [x] Google BigQuery cloud integration
- [x] Data quality reporting with cleaning action summaries
- [x] Interactive Jupyter notebook interface
- [x] Automated test suite
- [x] Synthetic sample data (no real PII)

## v1.1.0 — Pipeline Hardening
- [ ] CLI mode (run without Jupyter)
- [ ] Configurable cleaning rules via YAML
- [ ] Schema drift detection and alerting
- [ ] Detailed data profiling report (distributions, outliers, completeness)
- [ ] Retry logic for BigQuery upload failures

## v2.0.0 — Scheduled & Streaming
- [ ] Cron-based scheduled runs
- [ ] Incremental processing (only new/changed files)
- [ ] Support for additional source formats (Excel, Google Sheets API)
- [ ] Data lineage tracking (source file → cleaned row → BigQuery)
- [ ] Dashboard for pipeline run history and metrics
