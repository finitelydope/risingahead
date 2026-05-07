# risingahead

Local financial data platform. Ingests from  mocked CSV data, runs through Bronze / Silver / Gold in Postgres, orchestrated by Dagster.

---

## Requirements

- Python 3.11+
- Docker Desktop
- pip

---

## Setup

**1. Start Postgres**

```
docker-compose up -d
```

Runs `init_db/01_create_schemas.sql` on first start — creates `bronze`, `silver`, `gold` schemas.

**2. Copy env file**

```
cp .env.example .env
```

Default values work out of the box for local dev.

**3. Install dependencies**

```
pip install -r requirements.txt
```

**4. Start Dagster**

```
dagster dev
```

Open `http://localhost:3000`. Click **Materialize All**.



## Pipeline Layers

| Layer  | Schema | What it does |
|--------|--------|--------------|
| Bronze | bronze | Raw landing. Watermark-based incremental load. MD5 row hash per row. |
| Silver | silver | Cleaned, normalised, deduplicated. Rebuilt fully each run. |
| Gold   | gold   | Two serving tables — one for risk scoring, one for analytics. |

---

## Gold Outputs

| Table | Description |
|-------|-------------|
| `gold.customer_risk_features` | One row per customer. RFM metrics + open flag counts. Fed into risk scoring model. |
| `gold.monthly_analytics` | One row per customer per month. Transaction volume, flag activity. Used by BI dashboard. |

---

## Asset Checks

| Check | Asset | Condition |
|-------|-------|-----------|
| `check_kyc_null_rate` | silver_customers | Fails if null KYC rate > 5% |
| `check_transaction_amounts` | silver_transactions | Fails if any amount <= 0 |
| `check_risk_features_row_count` | customer_risk_features | Fails if gold row count != silver customer count |

---

## Configuration

Client config lives in `config/clients/<CLIENT_NAME>.yaml`. Set `CLIENT_NAME` in `.env` to switch clients.

```yaml
sources:
  customers:
    active: true
    primary_key: customer_id
    watermark_column: last_updated_ts

schedule:
  cron_schedule: "0 0 * * *"
  enabled: true

quality:
  max_kyc_null_rate_pct: 5.0
  allow_negative_amounts: false
```

---

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `DATABASE_URL` | `postgresql://risingahead:risingahead123@localhost:5433/financial_platform` | Postgres connection string |
| `CLIENT_NAME` | `default` | Which client config YAML to load |
| `MOCK_DATA_DIR` | `./mock_data` | Path to CSV source files |
| `POSTGRES_USER` | `risingahead` | Docker container user |
| `POSTGRES_PASSWORD` | `risingahead123` | Docker container password |
| `POSTGRES_DB` | `financial_platform` | Docker container DB name |

---

## Tests

```
pytest tests/
```

---

## Inspect the database

```
docker exec -it risingahead_postgres psql -U risingahead -d financial_platform
```

```sql
SELECT COUNT(*) FROM bronze.customers;
SELECT COUNT(*) FROM silver.customers;
SELECT COUNT(*) FROM gold.customer_risk_features;
SELECT COUNT(*) FROM gold.monthly_analytics;
SELECT * FROM public.pipeline_watermarks;
```

---

## Swap to real Oracle

In `pipeline/resources.py`, replace `MockSourceResource` with an `OracleResource` backed by `OracleConnector`. No changes needed anywhere else in the pipeline.
