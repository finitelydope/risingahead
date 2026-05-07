from dagster import asset_check, AssetCheckResult, AssetCheckSeverity, MetadataValue
import pandas as pd


@asset_check(
    asset="silver_customers",
    description="Fails if more than 5% of silver customers have a null KYC status",
)
def check_kyc_null_rate(context):
    from pipeline.resources import postgres_resource

    pg = postgres_resource.get_connector()
    df = pg.extract("SELECT kyc_status FROM silver.customers")

    total = int(len(df))
    null_count = int(df["kyc_status"].isnull().sum())
    null_rate = null_count / total if total > 0 else 0
    passed = bool(null_rate <= 0.05)

    return AssetCheckResult(
        passed=passed,
        severity=AssetCheckSeverity.ERROR,
        metadata={
            "total_rows":     MetadataValue.int(total),
            "null_count":     MetadataValue.int(null_count),
            "null_rate_pct":  MetadataValue.float(round(float(null_rate * 100), 2)),
            "threshold_pct":  MetadataValue.float(5.0),
        },
    )


@asset_check(
    asset="silver_transactions",
    description="Fails if any transaction in silver has a zero or negative amount",
)
def check_transaction_amounts(context):
    from pipeline.resources import postgres_resource

    pg = postgres_resource.get_connector()
    df = pg.extract("SELECT amount FROM silver.transactions")

    total = int(len(df))
    bad_count = int((df["amount"] <= 0).sum())
    passed = bool(bad_count == 0)

    return AssetCheckResult(
        passed=passed,
        severity=AssetCheckSeverity.ERROR,
        metadata={
            "total_rows":          MetadataValue.int(total),
            "invalid_amount_rows": MetadataValue.int(bad_count),
        },
    )


@asset_check(
    asset="customer_risk_features",
    description="Fails if gold risk features row count does not match silver customers",
)
def check_risk_features_row_count(context):
    from pipeline.resources import postgres_resource

    pg = postgres_resource.get_connector()

    silver_count = int(len(pg.extract("SELECT customer_id FROM silver.customers")))
    gold_count   = int(len(pg.extract("SELECT customer_id FROM gold.customer_risk_features")))
    passed = bool(silver_count == gold_count)

    return AssetCheckResult(
        passed=passed,
        severity=AssetCheckSeverity.ERROR,
        metadata={
            "silver_customers":       MetadataValue.int(silver_count),
            "gold_risk_features_rows": MetadataValue.int(gold_count),
            "difference":             MetadataValue.int(abs(silver_count - gold_count)),
        },
    )
