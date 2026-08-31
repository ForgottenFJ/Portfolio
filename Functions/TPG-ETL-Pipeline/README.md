# TPG ETL Pipeline

Azure Function suite that converts pipe-delimited source files into dated shards, merges them into a master Parquet dataset, and generates processed CSV outputs — without ever rewriting the full master file on each run.

**Trigger:** HTTP (multi-step: convert → merge → process)
**Auth:** Azure Key Vault (Managed Identity / Client Secret depending on environment)
**Output:** Parquet master dataset + processed CSVs in Blob Storage
