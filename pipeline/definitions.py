from dagster import Definitions

from pipeline.assets.bronze import (
    bronze_customers,
    bronze_accounts,
    bronze_transactions,
    bronze_risk_flags,
)
from pipeline.assets.silver import (
    silver_customers,
    silver_accounts,
    silver_transactions,
    silver_risk_flags,
)
from pipeline.assets.gold import (
    customer_risk_features,
    monthly_analytics,
)
from pipeline.resources import postgres_resource, mock_source_resource
from pipeline.schedules import daily_pipeline_schedule, full_pipeline_job
from pipeline.sensors import new_source_data_sensor, bronze_only_job
from pipeline.checks import (
    check_kyc_null_rate,
    check_transaction_amounts,
    check_risk_features_row_count,
)

defs = Definitions(
    assets=[
        bronze_customers,
        bronze_accounts,
        bronze_transactions,
        bronze_risk_flags,
        silver_customers,
        silver_accounts,
        silver_transactions,
        silver_risk_flags,
        customer_risk_features,
        monthly_analytics,
    ],
    asset_checks=[
        check_kyc_null_rate,
        check_transaction_amounts,
        check_risk_features_row_count,
    ],
    jobs=[
        full_pipeline_job,
        bronze_only_job,
    ],
    schedules=[
        daily_pipeline_schedule,
    ],
    sensors=[
        new_source_data_sensor,
    ],
    resources={
        "postgres": postgres_resource,
        "mock_source": mock_source_resource,
    },
)
