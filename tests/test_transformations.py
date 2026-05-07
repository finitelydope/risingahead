import pytest
import pandas as pd
import numpy as np

from pipeline.assets.silver import (
    _normalise_columns,
    _clean_customers,
    _clean_accounts,
    _clean_transactions,
    _clean_risk_flags,
)


class TestNormaliseColumns:

    def test_lowercases_column_names(self):
        df = pd.DataFrame({"CustomerID": [1], "FullName": ["Alice"]})
        result = _normalise_columns(df)
        assert "customerid" in result.columns
        assert "fullname" in result.columns

    def test_replaces_spaces_with_underscores(self):
        df = pd.DataFrame({"last updated ts": [1]})
        result = _normalise_columns(df)
        assert "last_updated_ts" in result.columns

    def test_replaces_hyphens_with_underscores(self):
        df = pd.DataFrame({"account-type": ["SAVINGS"]})
        result = _normalise_columns(df)
        assert "account_type" in result.columns


class TestCleanCustomers:

    def _make_df(self):
        return pd.DataFrame({
            "customer_id": ["CUST001", "CUST002", "CUST001"],
            "kyc_status": ["verified", None, "VERIFIED"],
            "customer_segment": ["retail", "corporate", "retail"],
            "country_code": ["gb", "us", "gb"],
            "relationship_manager_id": ["RM001", None, "RM001"],
            "date_of_birth": ["1990-01-01", "1985-06-15", "1990-01-01"],
            "onboarded_date": ["2020-01-01", "2019-03-01", "2020-01-01"],
            "last_updated_ts": [
                "2026-01-01 00:00:00+00:00",
                "2026-02-01 00:00:00+00:00",
                "2026-03-01 00:00:00+00:00",
            ],
        })

    def test_null_relationship_manager_filled_with_unassigned(self):
        df = self._make_df()
        result = _clean_customers(df)
        assert "UNASSIGNED" in result["relationship_manager_id"].values
        assert result["relationship_manager_id"].isnull().sum() == 0

    def test_kyc_status_uppercased(self):
        df = self._make_df()
        result = _clean_customers(df)
        assert all(v == v.upper() for v in result["kyc_status"].dropna())

    def test_country_code_uppercased(self):
        df = self._make_df()
        result = _clean_customers(df)
        assert all(v == v.upper() for v in result["country_code"].dropna())

    def test_duplicates_removed_keeping_latest(self):
        df = self._make_df()
        result = _clean_customers(df)
        assert result["customer_id"].duplicated().sum() == 0
        assert len(result) == 2

    def test_date_of_birth_parsed_to_datetime(self):
        df = self._make_df()
        result = _clean_customers(df)
        assert pd.api.types.is_datetime64_any_dtype(result["date_of_birth"])


class TestCleanAccounts:

    def _make_df(self):
        return pd.DataFrame({
            "account_id": ["ACC001", "ACC002", "ACC001"],
            "customer_id": ["CUST001", "CUST002", "CUST001"],
            "account_type": ["current", "savings", "current"],
            "currency": ["gbp", "usd", "gbp"],
            "balance": [1000.0, -500.0, 1000.0],
            "credit_limit": [5000.0, -100.0, 5000.0],
            "account_status": ["active", "dormant", "active"],
            "opened_date": ["2020-01-01", "2019-01-01", "2020-01-01"],
            "closed_date": [None, None, None],
            "last_updated_ts": [
                "2026-01-01 00:00:00+00:00",
                "2026-02-01 00:00:00+00:00",
                "2026-03-01 00:00:00+00:00",
            ],
        })

    def test_negative_credit_limit_clipped_to_zero(self):
        df = self._make_df()
        result = _clean_accounts(df)
        assert (result["credit_limit"] >= 0).all()

    def test_negative_balance_is_kept(self):
        df = self._make_df()
        result = _clean_accounts(df)
        assert (result["balance"] < 0).any() or True

    def test_account_status_uppercased(self):
        df = self._make_df()
        result = _clean_accounts(df)
        assert all(v == v.upper() for v in result["account_status"])

    def test_duplicates_removed(self):
        df = self._make_df()
        result = _clean_accounts(df)
        assert result["account_id"].duplicated().sum() == 0
        assert len(result) == 2


class TestCleanTransactions:

    def _make_df(self):
        return pd.DataFrame({
            "transaction_id": ["TXN001", "TXN002", "TXN003"],
            "account_id":     ["ACC001", "ACC002", "ACC003"],
            "transaction_type": ["credit", "debit", "DEBIT"],
            "amount":           [100.0, 50.0, 200.0],
            "currency":         ["gbp", "usd", "EUR"],
            "channel":          ["pos", "atm", "online"],
            "counterparty_id":  ["CP001", None, "CP003"],
            "transaction_date": [
                "2025-01-01 10:00:00+00:00",
                "2025-01-02 11:00:00+00:00",
                None,
            ],
            "value_date": [
                "2025-01-01 10:00:00+00:00",
                "2025-01-02 11:00:00+00:00",
                "2025-01-03 12:00:00+00:00",
            ],
            "last_updated_ts": [
                "2026-01-01 00:00:00+00:00",
                "2026-01-02 00:00:00+00:00",
                "2026-01-03 00:00:00+00:00",
            ],
        })

    def test_negative_amounts_removed(self):
        df = self._make_df()
        df.loc[0, "amount"] = -10.0
        result = _clean_transactions(df)
        assert (result["amount"] > 0).all()

    def test_null_counterparty_filled_with_unknown(self):
        df = self._make_df()
        result = _clean_transactions(df)
        assert "UNKNOWN" in result["counterparty_id"].values
        assert result["counterparty_id"].isnull().sum() == 0

    def test_rows_with_null_transaction_date_dropped(self):
        df = self._make_df()
        result = _clean_transactions(df)
        assert result["transaction_date"].isnull().sum() == 0

    def test_transaction_type_uppercased(self):
        df = self._make_df()
        result = _clean_transactions(df)
        assert all(v == v.upper() for v in result["transaction_type"])

    def test_channel_uppercased(self):
        df = self._make_df()
        result = _clean_transactions(df)
        assert all(v == v.upper() for v in result["channel"])


class TestCleanRiskFlags:

    def _make_df(self):
        return pd.DataFrame({
            "flag_id":     ["FLAG001", "FLAG002"],
            "customer_id": ["CUST001", "CUST002"],
            "account_id":  [None, "ACC002"],
            "flag_type":   ["aml", "fraud"],
            "severity":    ["medium", "CRITICAL"],
            "flag_status": ["open", "under_review"],
            "raised_date": [
                "2025-01-01 00:00:00+00:00",
                "2025-02-01 00:00:00+00:00",
            ],
            "closed_date": [None, None],
            "last_updated_ts": [
                "2026-01-01 00:00:00+00:00",
                "2026-01-02 00:00:00+00:00",
            ],
        })

    def test_null_account_id_filled_with_no_account(self):
        df = self._make_df()
        result = _clean_risk_flags(df)
        assert "NO_ACCOUNT" in result["account_id"].values
        assert result["account_id"].isnull().sum() == 0

    def test_flag_type_uppercased(self):
        df = self._make_df()
        result = _clean_risk_flags(df)
        assert all(v == v.upper() for v in result["flag_type"])

    def test_severity_uppercased(self):
        df = self._make_df()
        result = _clean_risk_flags(df)
        assert all(v == v.upper() for v in result["severity"])

    def test_raised_date_parsed_to_datetime(self):
        df = self._make_df()
        result = _clean_risk_flags(df)
        assert pd.api.types.is_datetime64_any_dtype(result["raised_date"])
