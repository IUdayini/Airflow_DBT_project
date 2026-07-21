# Walmart Data Engineering Project

An end-to-end data pipeline that simulates Walmart retail data flowing through a medallion (bronze → silver → gold) architecture, orchestrated with **Apache Airflow** and transformed with **dbt** on **Databricks**.


## Overview

The project ingests raw retail data (customers, employees, orders, order items, products, stores), pushes it through CDC ingestion on Databricks, and transforms it into clean, tested, analytics-ready tables using dbt — all triggered and sequenced by an Airflow DAG.

## Architecture

```
Raw CSVs (walmart_dataset/)
        │
        ▼
Databricks CDC Ingestion Job  (triggered by Airflow)
        │
        ▼
Silver Layer
  ├── silver_t   — technical/cleaned tables (1:1 with source)
  └── silver_b   — business-level transformations (obt_b)
        │
        ▼
Gold Layer
  ├── ephemeral  — intermediate models (eph_customers, eph_orders, etc.)
  ├── dimensions — SCD snapshots (dim_customers, dim_orders, dim_products, dim_stores, dim_employees)
  └── fact       — fact_orders
```

## Pipeline (Airflow DAG: `orchestrate`)

The DAG in `airflow_dbt_project/dags/orchestrate.py` runs the following steps in order:

1. **`ingest_cdc`** — triggers a Databricks job to ingest CDC data and polls until it completes
2. **`clean_target`** — clears out dbt's `target` and `logs` directories from the previous run
3. **`source_freshness`** — runs `dbt source freshness` to validate source data is up to date
4. **`silver_technical`** / **`silver_technical_tests`** — builds and tests the `silver_t` models
5. **`silver_business`** / **`silver_business_tests`** — builds and tests the `silver_b` models
6. **`gold_ephermeral`** — builds gold-layer ephemeral models
7. **`gold_dimensions`** — runs `dbt snapshot` to build SCD dimension tables
8. **`gold_facts`** — builds the gold-layer fact table(s)

## Project Structure

```
.
├── airflow_dbt_project/
│   ├── config/               # Airflow configuration (airflow.cfg)
│   ├── dags/                 # Airflow DAG definitions
│   │   └── orchestrate.py
│   ├── logs/                 # Airflow task run logs
│   ├── walmart_project/      # dbt project
│   │   ├── models/
│   │   │   ├── source/       # source declarations
│   │   │   ├── silver_t/     # technical/cleaned models
│   │   │   ├── silver_b/     # business-level models
│   │   │   └── gold/         # ephemeral, dimension, and fact models
│   │   ├── snapshots/        # SCD dimension snapshots
│   │   ├── macros/
│   │   ├── tests/
│   │   ├── seeds/
│   │   ├── analyses/
│   │   ├── dbt_project.yml
│   │   └── profiles.yml
│   ├── docker-compose.yaml   # Airflow local dev environment
│   ├── Dockerfile
│   └── requirements.txt
└── walmart_dataset/
    ├── data/                 # raw CSVs (customers, employees, order_items, orders, products, stores)
    ├── ddl/                  # walmart_schema.sql — source table definitions
    └── load_data.py          # script to load CSVs into the source database
```

## Data Model

The source dataset (`walmart_dataset/`) represents a retail schema with:

- **customers** — customer profile and contact info
- **stores** — store locations
- **products** — product catalog with category, brand, price
- **employees** — store staff, linked to `stores`
- **orders** / **order_items** — transactional order data

These feed into gold-layer **dimension** and **fact** tables for analytics.
