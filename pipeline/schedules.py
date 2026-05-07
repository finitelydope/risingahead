from dagster import ScheduleDefinition, define_asset_job, AssetSelection

full_pipeline_job = define_asset_job(
    name="full_pipeline_job",
    selection=AssetSelection.groups("bronze", "silver", "gold"),
    description="Runs the full Bronze → Silver → Gold pipeline in dependency order",
)

daily_pipeline_schedule = ScheduleDefinition(
    name="daily_pipeline_schedule",
    job=full_pipeline_job,
    cron_schedule="0 0 * * *",
    execution_timezone="UTC",
    description="Triggers the full pipeline every day at midnight UTC",
)
