# High-Performance Data Validation Pipeline (Polars)

This project demonstrates a production-style data validation pipeline
built with **Polars** for high performance and low memory usage.

It validates lead data based on `sub_status` rules and produces
deterministic outputs (`VALID`, `INVALID`, `RECHECK`) with
human-readable comments.

## Why Polars
- Multi-threaded Rust execution (no Python GIL issues)
- Vectorized string operations
- Lazy CSV scanning + streaming execution
- Scales comfortably to 100k+ rows on a laptop

## Requirements
- Python 3.10+
- polars
- pyarrow

## Install
```bash
pip install -r requirements.txt
