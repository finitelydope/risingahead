import pandas as pd
from .base import BaseConnector


class OracleConnector(BaseConnector):

    QUERIES = {
        "customers": """
            SELECT
                customer_id,
                full_name,
                date_of_birth,
                email,
                phone,
                country_code,
                customer_segment,
                kyc_status,
                onboarded_date,
                relationship_manager_id,
                last_updated_ts
            FROM crm.customers
        """,
        "accounts": """
            SELECT
                account_id,
                customer_id,
                account_type,
                currency,
                balance,
                credit_limit,
                opened_date,
                closed_date,
                account_status,
                branch_code,
                last_updated_ts
            FROM core_banking.accounts
        """,
        "transactions": """
            SELECT
                transaction_id,
                account_id,
                transaction_date,
                value_date,
                transaction_type,
                amount,
                currency,
                merchant_category_code,
                counterparty_id,
                channel,
                reference,
                last_updated_ts
            FROM core_banking.transactions
        """,
        "risk_flags": """
            SELECT
                flag_id,
                customer_id,
                account_id,
                flag_type,
                severity,
                flag_status,
                raised_date,
                closed_date,
                raised_by,
                description,
                last_updated_ts
            FROM risk.risk_flags
        """,
    }

    def __init__(self, host: str, port: int, service_name: str, username: str, password: str):
        self.host = host
        self.port = port
        self.service_name = service_name
        self.username = username
        self.password = password
        self.connection = None

    def connect(self):
        try:
            import cx_Oracle
        except ImportError:
            raise ImportError(
                "cx_Oracle is not installed. Run: pip install cx_Oracle\n"
                "Also ensure Oracle Instant Client is installed on this machine."
            )

        dsn = cx_Oracle.makedsn(self.host, self.port, service_name=self.service_name)
        self.connection = cx_Oracle.connect(
            user=self.username,
            password=self.password,
            dsn=dsn
        )
        print(f"OracleConnector: connected to {self.host}:{self.port}/{self.service_name}")

    def extract(self, source_name: str) -> pd.DataFrame:
        if self.connection is None:
            raise RuntimeError("OracleConnector: call connect() before extract()")

        if source_name not in self.QUERIES:
            raise ValueError(
                f"OracleConnector: unknown source '{source_name}'. "
                f"Available: {list(self.QUERIES.keys())}"
            )

        query = self.QUERIES[source_name]
        df = pd.read_sql(query, con=self.connection)
        print(f"OracleConnector: extracted {len(df):,} rows for '{source_name}'")
        return df

    def load(self, df: pd.DataFrame, schema: str, table: str, if_exists: str = "append") -> None:
        raise NotImplementedError(
            "OracleConnector is a read-only source. Use PostgresConnector to write data."
        )
