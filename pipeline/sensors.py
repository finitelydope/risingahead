from dagster import (
    sensor,
    RunRequest,
    SkipReason,
    SensorEvaluationContext,
    define_asset_job,
    AssetSelection,
)
import os
from dotenv import load_dotenv

load_dotenv()

bronze_only_job = define_asset_job(
    name="bronze_only_job",
    selection=AssetSelection.groups("bronze"),
    description="Re-ingests bronze layer only — triggered by the new data sensor",
)


@sensor(
    job=bronze_only_job,
    minimum_interval_seconds=300,
    description="Watches the mock data directory for files newer than the last watermark",
)
def new_source_data_sensor(context: SensorEvaluationContext):
    data_dir = os.getenv("MOCK_DATA_DIR", "./mock_data")
    source_files = ["customers.csv", "accounts.csv", "transactions.csv", "risk_flags.csv"]

    latest_mtime = None
    for fname in source_files:
        fpath = os.path.join(data_dir, fname)
        if os.path.exists(fpath):
            mtime = os.path.getmtime(fpath)
            if latest_mtime is None or mtime > latest_mtime:
                latest_mtime = mtime

    if latest_mtime is None:
        yield SkipReason("No source files found in data directory")
        return

    latest_mtime_str = str(latest_mtime)
    last_cursor = context.cursor

    if last_cursor is not None and latest_mtime_str <= last_cursor:
        yield SkipReason(
            f"No new source data detected. Last file change: {latest_mtime_str}"
        )
        return

    context.update_cursor(latest_mtime_str)
    yield RunRequest(
        run_key=latest_mtime_str,
        tags={"triggered_by": "new_source_data_sensor"},
    )
