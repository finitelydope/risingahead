import hashlib
import pandas as pd
from datetime import datetime, timezone
from dagster import asset, Output, MetadataValue

from pipeline.resources import PostgresResource, MockSourceResource


def _compute_row_hash(row: pd.Series) -> str:
    row_str = "|".join(str(v) for v in row.values)
    return hashlib.md5(row_str.encode()).hexdigest()


def _ingest_source(
    source_name: str,
    source: object,
    pg: object,
) -> dict:
    watermark = pg.get_watermark(source_name)
    print(f"Bronze [{source_name}]: watermark = {watermark}")

    df = source.extract(source_name)

    df["last_updated_ts"] = pd.to_datetime(df["last_updated_ts"], utc=True)

    if watermark is not None:
        watermark_ts = pd.to_datetime(watermark, utc=True)
        df = df[df["last_updated_ts"] > watermark_ts].copy()
        print(f"Bronze [{source_name}]: {len(df)} rows changed since last run")
    else:
        print(f"Bronze [{source_name}]: first run — loading all {len(df)} rows")

    if df.empty:
        print(f"Bronze [{source_name}]: no new rows, skipping load")
        return {"rows_loaded": 0, "watermark": watermark}

    df["_ingested_at"] = datetime.now(timezone.utc)
    df["_row_hash"] = df.drop(columns=["_ingested_at"]).apply(_compute_row_hash, axis=1)

    if_exists = "replace" if watermark is None else "append"
    pg.load(df, schema="bronze", table=source_name, if_exists=if_exists)

    new_watermark = str(df["last_updated_ts"].max())
    pg.set_watermark(source_name, new_watermark)

    return {
        "rows_loaded": len(df),
        "watermark": new_watermark,
        "if_exists": if_exists,
    }


@asset(
    group_name="bronze",
    required_resource_keys={"mock_source", "postgres"},
    description="Raw customer data landed from Oracle source into bronze schema",
)
def bronze_customers(context) -> Output:
    source = context.resources.mock_source.get_connector()
    pg = context.resources.postgres.get_connector()
    meta = _ingest_source("customers", source, pg)
    return Output(
        value=meta["rows_loaded"],
        metadata={
            "rows_loaded": MetadataValue.int(meta["rows_loaded"]),
            "watermark": MetadataValue.text(str(meta["watermark"])),
        },
    )


@asset(
    group_name="bronze",
    required_resource_keys={"mock_source", "postgres"},
    description="Raw account data landed from Oracle source into bronze schema",
)
def bronze_accounts(context) -> Output:
    source = context.resources.mock_source.get_connector()
    pg = context.resources.postgres.get_connector()
    meta = _ingest_source("accounts", source, pg)
    return Output(
        value=meta["rows_loaded"],
        metadata={
            "rows_loaded": MetadataValue.int(meta["rows_loaded"]),
            "watermark": MetadataValue.text(str(meta["watermark"])),
        },
    )


@asset(
    group_name="bronze",
    required_resource_keys={"mock_source", "postgres"},
    description="Raw transaction data landed from Oracle source into bronze schema",
)
def bronze_transactions(context) -> Output:
    source = context.resources.mock_source.get_connector()
    pg = context.resources.postgres.get_connector()
    meta = _ingest_source("transactions", source, pg)
    return Output(
        value=meta["rows_loaded"],
        metadata={
            "rows_loaded": MetadataValue.int(meta["rows_loaded"]),
            "watermark": MetadataValue.text(str(meta["watermark"])),
        },
    )


@asset(
    group_name="bronze",
    required_resource_keys={"mock_source", "postgres"},
    description="Raw risk flag data landed from Oracle source into bronze schema",
)
def bronze_risk_flags(context) -> Output:
    source = context.resources.mock_source.get_connector()
    pg = context.resources.postgres.get_connector()
    meta = _ingest_source("risk_flags", source, pg)
    return Output(
        value=meta["rows_loaded"],
        metadata={
            "rows_loaded": MetadataValue.int(meta["rows_loaded"]),
            "watermark": MetadataValue.text(str(meta["watermark"])),
        },
    )
