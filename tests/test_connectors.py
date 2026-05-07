import pytest
import pandas as pd
import os
import tempfile

from connectors.mock_connector import MockConnector
from connectors.oracle_connector import OracleConnector
from pipeline.assets.bronze import _compute_row_hash


class TestMockConnector:

    def test_connect_succeeds_with_valid_directory(self):
        connector = MockConnector(data_dir="./mock_data")
        connector.connect()

    def test_connect_raises_when_directory_missing(self):
        connector = MockConnector(data_dir="./does_not_exist")
        with pytest.raises(FileNotFoundError) as exc_info:
            connector.connect()
        assert "does_not_exist" in str(exc_info.value)

    def test_extract_returns_dataframe(self):
        connector = MockConnector(data_dir="./mock_data")
        connector.connect()
        df = connector.extract("customers")
        assert isinstance(df, pd.DataFrame)

    def test_extract_customers_has_expected_columns(self):
        connector = MockConnector(data_dir="./mock_data")
        connector.connect()
        df = connector.extract("customers")
        required_columns = [
            "customer_id", "full_name", "email",
            "kyc_status", "customer_segment", "last_updated_ts",
        ]
        for col in required_columns:
            assert col in df.columns, f"Missing column: {col}"

    def test_extract_returns_correct_row_count(self):
        connector = MockConnector(data_dir="./mock_data")
        connector.connect()
        df = connector.extract("customers")
        assert len(df) == 500

    def test_extract_raises_for_unknown_source(self):
        connector = MockConnector(data_dir="./mock_data")
        connector.connect()
        with pytest.raises(FileNotFoundError) as exc_info:
            connector.extract("nonexistent_table")
        assert "nonexistent_table" in str(exc_info.value)

    def test_load_raises_not_implemented(self):
        connector = MockConnector(data_dir="./mock_data")
        connector.connect()
        df = pd.DataFrame({"col": [1, 2, 3]})
        with pytest.raises(NotImplementedError):
            connector.load(df, schema="bronze", table="test")

    def test_extract_all_four_sources(self):
        connector = MockConnector(data_dir="./mock_data")
        connector.connect()
        for source in ["customers", "accounts", "transactions", "risk_flags"]:
            df = connector.extract(source)
            assert len(df) > 0, f"{source} returned empty DataFrame"


class TestRowHash:

    def test_same_row_produces_same_hash(self):
        row = pd.Series({"customer_id": "CUST001", "name": "Alice", "amount": 100.0})
        hash1 = _compute_row_hash(row)
        hash2 = _compute_row_hash(row)
        assert hash1 == hash2

    def test_different_rows_produce_different_hashes(self):
        row1 = pd.Series({"customer_id": "CUST001", "name": "Alice", "amount": 100.0})
        row2 = pd.Series({"customer_id": "CUST001", "name": "Alice", "amount": 200.0})
        assert _compute_row_hash(row1) != _compute_row_hash(row2)

    def test_hash_detects_single_field_change(self):
        row1 = pd.Series({"customer_id": "CUST001", "kyc_status": "VERIFIED"})
        row2 = pd.Series({"customer_id": "CUST001", "kyc_status": "PENDING"})
        assert _compute_row_hash(row1) != _compute_row_hash(row2)

    def test_hash_is_32_char_md5_string(self):
        row = pd.Series({"id": "X001", "value": "test"})
        h = _compute_row_hash(row)
        assert len(h) == 32
        assert all(c in "0123456789abcdef" for c in h)


class TestOracleConnector:

    def test_load_raises_not_implemented(self):
        connector = OracleConnector(
            host="localhost", port=1521,
            service_name="TEST", username="user", password="pass"
        )
        df = pd.DataFrame({"col": [1]})
        with pytest.raises(NotImplementedError):
            connector.load(df, schema="bronze", table="test")

    def test_extract_raises_before_connect(self):
        connector = OracleConnector(
            host="localhost", port=1521,
            service_name="TEST", username="user", password="pass"
        )
        with pytest.raises(RuntimeError) as exc_info:
            connector.extract("customers")
        assert "connect()" in str(exc_info.value)

    def test_extract_raises_for_unknown_source(self):
        connector = OracleConnector(
            host="localhost", port=1521,
            service_name="TEST", username="user", password="pass"
        )
        connector.connection = "mock"
        with pytest.raises(ValueError) as exc_info:
            connector.extract("unknown_table")
        assert "unknown_table" in str(exc_info.value)
