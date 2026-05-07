import pandas as pd
from datetime import datetime, timezone
from dagster import asset, Output, MetadataValue

from pipeline.resources import PostgresResource


def _normalise_columns(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_", regex=False)
        .str.replace("-", "_", regex=False)
    )
    return df


def _add_pipeline_metadata(df: pd.DataFrame, run_id: str) -> pd.DataFrame:
    df["_processed_at"] = datetime.now(timezone.utc)
    df["_pipeline_run_id"] = run_id
    return df


def _clean_source(
    source_name: str,
    pg: object,
    run_id: str,
    clean_fn,
) -> dict:
    df = pg.extract(f"SELECT * FROM bronze.{source_name}")
    print(f"Silver [{source_name}]: read {len(df)} rows from bronze")

    df = _normalise_columns(df)
    df = clean_fn(df)
    df = _add_pipeline_metadata(df, run_id)

    df = df.drop(columns=["_ingested_at"], errors="ignore")

    pg.load(df, schema="silver", table=source_name, if_exists="replace")
    return {"rows_written": len(df)}


def _clean_customers(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["relationship_manager_id"] = df["relationship_manager_id"].fillna("UNASSIGNED")

    df["kyc_status"] = df["kyc_status"].str.upper().str.strip()
    df["customer_segment"] = df["customer_segment"].str.upper().str.strip()
    df["country_code"] = df["country_code"].str.upper().str.strip()

    df["date_of_birth"] = pd.to_datetime(df["date_of_birth"], errors="coerce")
    df["onboarded_date"] = pd.to_datetime(df["onboarded_date"], errors="coerce")

    df = df.sort_values("last_updated_ts", ascending=False)
    df = df.drop_duplicates(subset=["customer_id"], keep="first")

    return df


def _clean_accounts(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["credit_limit"] = df["credit_limit"].clip(lower=0)

    df["account_status"] = df["account_status"].str.upper().str.strip()
    df["account_type"] = df["account_type"].str.upper().str.strip()
    df["currency"] = df["currency"].str.upper().str.strip()

    df["opened_date"] = pd.to_datetime(df["opened_date"], errors="coerce")
    df["closed_date"] = pd.to_datetime(df["closed_date"], errors="coerce")

    df = df.sort_values("last_updated_ts", ascending=False)
    df = df.drop_duplicates(subset=["account_id"], keep="first")

    return df


def _clean_transactions(df: pd.DataFrame) -> pd.DataFrame:
    df = df[df["amount"] > 0].copy()

    df["counterparty_id"] = df["counterparty_id"].fillna("UNKNOWN")

    df["transaction_type"] = df["transaction_type"].str.upper().str.strip()
    df["currency"] = df["currency"].str.upper().str.strip()
    df["channel"] = df["channel"].str.upper().str.strip()

    df["transaction_date"] = pd.to_datetime(df["transaction_date"], utc=True, errors="coerce")
    df["value_date"] = pd.to_datetime(df["value_date"], utc=True, errors="coerce")

    df = df.dropna(subset=["transaction_date"])

    df = df.sort_values("last_updated_ts", ascending=False)
    df = df.drop_duplicates(subset=["transaction_id"], keep="first")

    return df


def _clean_risk_flags(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["flag_type"] = df["flag_type"].str.upper().str.strip()
    df["severity"] = df["severity"].str.upper().str.strip()
    df["flag_status"] = df["flag_status"].str.upper().str.strip()

    df["account_id"] = df["account_id"].fillna("NO_ACCOUNT")

    df["raised_date"] = pd.to_datetime(df["raised_date"], utc=True, errors="coerce")
    df["closed_date"] = pd.to_datetime(df["closed_date"], utc=True, errors="coerce")

    df = df.sort_values("last_updated_ts", ascending=False)
    df = df.drop_duplicates(subset=["flag_id"], keep="first")

    return df


@asset(
    group_name="silver",
    deps=["bronze_customers"],
    required_resource_keys={"postgres"},
    description="Cleaned and normalised customer data in silver schema",
)
def silver_customers(context) -> Output:
    pg = context.resources.postgres.get_connector()
    meta = _clean_source("customers", pg, context.run_id, _clean_customers)
    return Output(
        value=meta["rows_written"],
        metadata={"rows_written": MetadataValue.int(meta["rows_written"])},
    )


@asset(
    group_name="silver",
    deps=["bronze_accounts"],
    required_resource_keys={"postgres"},
    description="Cleaned and normalised account data in silver schema",
)
def silver_accounts(context) -> Output:
    pg = context.resources.postgres.get_connector()
    meta = _clean_source("accounts", pg, context.run_id, _clean_accounts)
    return Output(
        value=meta["rows_written"],
        metadata={"rows_written": MetadataValue.int(meta["rows_written"])},
    )


@asset(
    group_name="silver",
    deps=["bronze_transactions"],
    required_resource_keys={"postgres"},
    description="Cleaned and normalised transaction data in silver schema",
)
def silver_transactions(context) -> Output:
    pg = context.resources.postgres.get_connector()
    meta = _clean_source("transactions", pg, context.run_id, _clean_transactions)
    return Output(
        value=meta["rows_written"],
        metadata={"rows_written": MetadataValue.int(meta["rows_written"])},
    )


@asset(
    group_name="silver",
    deps=["bronze_risk_flags"],
    required_resource_keys={"postgres"},
    description="Cleaned and normalised risk flag data in silver schema",
)
def silver_risk_flags(context) -> Output:
    pg = context.resources.postgres.get_connector()
    meta = _clean_source("risk_flags", pg, context.run_id, _clean_risk_flags)
    return Output(
        value=meta["rows_written"],
        metadata={"rows_written": MetadataValue.int(meta["rows_written"])},
    )
