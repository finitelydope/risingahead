import pandas as pd
from datetime import datetime, timezone
from dagster import asset, Output, MetadataValue


@asset(
    group_name="gold",
    deps=["silver_customers", "silver_accounts", "silver_transactions", "silver_risk_flags"],
    required_resource_keys={"postgres"},
    description="Customer-level RFM + risk features for the risk scoring engine",
)
def customer_risk_features(context) -> Output:
    pg = context.resources.postgres.get_connector()

    customers = pg.extract("SELECT * FROM silver.customers")
    accounts  = pg.extract("SELECT * FROM silver.accounts")
    txns      = pg.extract("SELECT * FROM silver.transactions")
    flags     = pg.extract("SELECT * FROM silver.risk_flags")

    txns = txns.merge(
        accounts[["account_id", "customer_id"]],
        on="account_id",
        how="left",
    )

    txns["transaction_date"] = pd.to_datetime(txns["transaction_date"], utc=True, errors="coerce")
    now = pd.Timestamp.now(tz="UTC")

    txn_features = txns.groupby("customer_id").agg(
        total_transactions    = ("transaction_id", "count"),
        total_spend           = ("amount", "sum"),
        avg_transaction_amt   = ("amount", "mean"),
        max_transaction_amt   = ("amount", "max"),
        last_transaction_date = ("transaction_date", "max"),
        debit_count           = ("transaction_type", lambda x: (x == "DEBIT").sum()),
        credit_count          = ("transaction_type", lambda x: (x == "CREDIT").sum()),
    ).reset_index()

    txn_features["days_since_last_txn"] = (
        now - pd.to_datetime(txn_features["last_transaction_date"], utc=True)
    ).dt.days

    txn_features["total_spend"]         = txn_features["total_spend"].round(2)
    txn_features["avg_transaction_amt"] = txn_features["avg_transaction_amt"].round(2)

    acct_features = accounts.groupby("customer_id").agg(
        total_accounts        = ("account_id", "count"),
        active_accounts       = ("account_status", lambda x: (x == "ACTIVE").sum()),
        frozen_accounts       = ("account_status", lambda x: (x == "FROZEN").sum()),
        total_balance         = ("balance", "sum"),
        total_credit_limit    = ("credit_limit", "sum"),
    ).reset_index()

    acct_features["total_balance"]      = acct_features["total_balance"].round(2)
    acct_features["total_credit_limit"] = acct_features["total_credit_limit"].round(2)

    severity_map = {"LOW": 1, "MEDIUM": 2, "HIGH": 3, "CRITICAL": 4}
    flags["severity_score"] = flags["severity"].map(severity_map).fillna(0)

    open_flags = flags[flags["flag_status"].isin(["OPEN", "UNDER_REVIEW", "ESCALATED"])]

    flag_features = open_flags.groupby("customer_id").agg(
        open_flag_count       = ("flag_id", "count"),
        max_severity_score    = ("severity_score", "max"),
        has_aml_flag          = ("flag_type", lambda x: int("AML" in x.values)),
        has_fraud_flag        = ("flag_type", lambda x: int("FRAUD" in x.values)),
        has_sanction_flag     = ("flag_type", lambda x: int("SANCTION" in x.values)),
        has_pep_flag          = ("flag_type", lambda x: int("PEP" in x.values)),
    ).reset_index()

    df = customers[["customer_id", "customer_segment", "kyc_status", "country_code", "onboarded_date"]].copy()

    df = df.merge(txn_features,  on="customer_id", how="left")
    df = df.merge(acct_features, on="customer_id", how="left")
    df = df.merge(flag_features, on="customer_id", how="left")

    fill_zeros = [
        "total_transactions", "total_spend", "avg_transaction_amt",
        "max_transaction_amt", "days_since_last_txn", "debit_count", "credit_count",
        "total_accounts", "active_accounts", "frozen_accounts",
        "total_balance", "total_credit_limit",
        "open_flag_count", "max_severity_score",
        "has_aml_flag", "has_fraud_flag", "has_sanction_flag", "has_pep_flag",
    ]
    df[fill_zeros] = df[fill_zeros].fillna(0)

    df["_created_at"] = datetime.now(timezone.utc)

    pg.load(df, schema="gold", table="customer_risk_features", if_exists="replace")

    return Output(
        value=len(df),
        metadata={
            "rows_written":         MetadataValue.int(int(len(df))),
            "columns":              MetadataValue.int(int(len(df.columns))),
            "customers_with_flags": MetadataValue.int(int((df["open_flag_count"] > 0).sum())),
        },
    )


@asset(
    group_name="gold",
    deps=["silver_customers", "silver_accounts", "silver_transactions", "silver_risk_flags"],
    required_resource_keys={"postgres"},
    description="Monthly per-customer aggregations for the analytics dashboard",
)
def monthly_analytics(context) -> Output:
    pg = context.resources.postgres.get_connector()

    customers = pg.extract("SELECT * FROM silver.customers")
    accounts  = pg.extract("SELECT * FROM silver.accounts")
    txns      = pg.extract("SELECT * FROM silver.transactions")
    flags     = pg.extract("SELECT * FROM silver.risk_flags")

    txns = txns.merge(
        accounts[["account_id", "customer_id"]],
        on="account_id",
        how="left",
    )

    txns["transaction_date"] = pd.to_datetime(txns["transaction_date"], utc=True, errors="coerce")
    txns["year_month"] = txns["transaction_date"].dt.to_period("M").astype(str)

    monthly_txns = txns.groupby(["customer_id", "year_month"]).agg(
        txn_count             = ("transaction_id", "count"),
        total_spend           = ("amount", "sum"),
        avg_spend             = ("amount", "mean"),
        debit_count           = ("transaction_type", lambda x: (x == "DEBIT").sum()),
        credit_count          = ("transaction_type", lambda x: (x == "CREDIT").sum()),
        unique_channels       = ("channel", "nunique"),
    ).reset_index()

    monthly_txns["total_spend"] = monthly_txns["total_spend"].round(2)
    monthly_txns["avg_spend"]   = monthly_txns["avg_spend"].round(2)

    flags["raised_date"] = pd.to_datetime(flags["raised_date"], utc=True, errors="coerce")
    flags["year_month"]  = flags["raised_date"].dt.to_period("M").astype(str)

    monthly_flags = flags.groupby(["customer_id", "year_month"]).agg(
        flags_raised          = ("flag_id", "count"),
        critical_flags_raised = ("severity", lambda x: (x == "CRITICAL").sum()),
    ).reset_index()

    df = monthly_txns.merge(monthly_flags, on=["customer_id", "year_month"], how="left")

    df = df.merge(
        customers[["customer_id", "customer_segment", "kyc_status", "country_code"]],
        on="customer_id",
        how="left",
    )

    df["flags_raised"]          = df["flags_raised"].fillna(0).astype(int)
    df["critical_flags_raised"] = df["critical_flags_raised"].fillna(0).astype(int)
    df["_created_at"]           = datetime.now(timezone.utc)

    pg.load(df, schema="gold", table="monthly_analytics", if_exists="replace")

    return Output(
        value=len(df),
        metadata={
            "rows_written":   MetadataValue.int(int(len(df))),
            "months_covered": MetadataValue.int(int(df["year_month"].nunique())),
            "customers":      MetadataValue.int(int(df["customer_id"].nunique())),
        },
    )
