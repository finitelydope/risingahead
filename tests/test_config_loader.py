import pytest
import os


class TestClientConfig:

    def test_default_config_loads(self):
        os.environ["CLIENT_NAME"] = "default"
        from pipeline.config_loader import ClientConfig
        config = ClientConfig.load()
        assert config is not None

    def test_schedule_cron_is_daily_midnight(self):
        os.environ["CLIENT_NAME"] = "default"
        from pipeline.config_loader import ClientConfig
        config = ClientConfig.load()
        assert config.schedule_cron == "0 0 * * *"

    def test_schedule_timezone_is_utc(self):
        os.environ["CLIENT_NAME"] = "default"
        from pipeline.config_loader import ClientConfig
        config = ClientConfig.load()
        assert config.schedule_timezone == "UTC"

    def test_schedule_is_enabled(self):
        os.environ["CLIENT_NAME"] = "default"
        from pipeline.config_loader import ClientConfig
        config = ClientConfig.load()
        assert config.schedule_enabled is True

    def test_all_four_sources_are_active(self):
        os.environ["CLIENT_NAME"] = "default"
        from pipeline.config_loader import ClientConfig
        config = ClientConfig.load()
        for source in ["customers", "accounts", "transactions", "risk_flags"]:
            assert config.is_source_active(source) is True, f"{source} should be active"

    def test_primary_keys_are_correct(self):
        os.environ["CLIENT_NAME"] = "default"
        from pipeline.config_loader import ClientConfig
        config = ClientConfig.load()
        assert config.get_primary_key("customers")    == "customer_id"
        assert config.get_primary_key("accounts")     == "account_id"
        assert config.get_primary_key("transactions")  == "transaction_id"
        assert config.get_primary_key("risk_flags")   == "flag_id"

    def test_kyc_null_rate_threshold(self):
        os.environ["CLIENT_NAME"] = "default"
        from pipeline.config_loader import ClientConfig
        config = ClientConfig.load()
        assert config.max_kyc_null_rate == 5.0

    def test_negative_amounts_not_allowed(self):
        os.environ["CLIENT_NAME"] = "default"
        from pipeline.config_loader import ClientConfig
        config = ClientConfig.load()
        assert config.allow_negative_amounts is False

    def test_both_gold_outputs_enabled(self):
        os.environ["CLIENT_NAME"] = "default"
        from pipeline.config_loader import ClientConfig
        config = ClientConfig.load()
        assert config.is_gold_output_enabled("customer_risk_features") is True
        assert config.is_gold_output_enabled("monthly_analytics") is True

    def test_missing_config_file_raises_file_not_found(self):
        os.environ["CLIENT_NAME"] = "nonexistent_client"
        from pipeline.config_loader import ClientConfig
        with pytest.raises(FileNotFoundError) as exc_info:
            ClientConfig.load()
        assert "nonexistent_client" in str(exc_info.value)
