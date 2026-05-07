import os
import yaml
from dotenv import load_dotenv

load_dotenv()


class ClientConfig:

    def __init__(self, raw: dict):
        self._raw = raw

    @classmethod
    def load(cls) -> "ClientConfig":
        client_name = os.getenv("CLIENT_NAME", "default")

        config_path = os.path.join(
            os.path.dirname(__file__),
            "..",
            "config",
            "clients",
            f"{client_name}.yaml",
        )
        config_path = os.path.normpath(config_path)

        if not os.path.exists(config_path):
            raise FileNotFoundError(
                f"ClientConfig: config file not found: {config_path}\n"
                f"Expected a file for CLIENT_NAME='{client_name}'"
            )

        with open(config_path, "r") as f:
            raw = yaml.safe_load(f)

        print(f"ClientConfig: loaded config for client '{client_name}' from {config_path}")
        return cls(raw)

    @property
    def schedule_enabled(self) -> bool:
        return self._raw.get("schedule", {}).get("enabled", True)

    @property
    def schedule_cron(self) -> str:
        return self._raw.get("schedule", {}).get("cron_schedule", "0 0 * * *")

    @property
    def schedule_timezone(self) -> str:
        return self._raw.get("schedule", {}).get("cron_timezone", "UTC")

    def is_source_active(self, source_name: str) -> bool:
        sources = self._raw.get("sources", {})
        return sources.get(source_name, {}).get("active", True)

    def get_primary_key(self, source_name: str) -> str:
        sources = self._raw.get("sources", {})
        return sources.get(source_name, {}).get("primary_key", "id")

    def get_watermark_column(self, source_name: str) -> str:
        sources = self._raw.get("sources", {})
        return sources.get(source_name, {}).get("watermark_column", "last_updated_ts")

    @property
    def max_kyc_null_rate(self) -> float:
        return self._raw.get("quality", {}).get("max_kyc_null_rate_pct", 5.0)

    @property
    def allow_negative_amounts(self) -> bool:
        return self._raw.get("quality", {}).get("allow_negative_amounts", False)

    def is_gold_output_enabled(self, output_name: str) -> bool:
        return self._raw.get("gold", {}).get(output_name, True)
