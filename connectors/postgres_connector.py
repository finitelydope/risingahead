import pandas as pd
from sqlalchemy import create_engine, text
from .base import BaseConnector


class PostgresConnector(BaseConnector):

    def __init__(self, db_url: str):
        self.db_url = db_url
        self.engine = None

    def connect(self):
        self.engine = create_engine(self.db_url)
        with self.engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        print(f"PostgresConnector: connected to database")

    def extract(self, query: str) -> pd.DataFrame:
        if self.engine is None:
            raise RuntimeError("PostgresConnector: call connect() before extract()")

        df = pd.read_sql(query, con=self.engine)
        print(f"PostgresConnector: extracted {len(df):,} rows")
        return df

    def load(self, df: pd.DataFrame, schema: str, table: str, if_exists: str = "append") -> None:
        if self.engine is None:
            raise RuntimeError("PostgresConnector: call connect() before load()")

        df.to_sql(
            name=table,
            con=self.engine,
            schema=schema,
            if_exists=if_exists,
            index=False,
            method="multi",
            chunksize=1000,
        )
        print(f"PostgresConnector: loaded {len(df):,} rows into {schema}.{table}")

    def execute(self, sql: str) -> None:
        if self.engine is None:
            raise RuntimeError("PostgresConnector: call connect() before execute()")

        with self.engine.begin() as conn:
            conn.execute(text(sql))

    def get_watermark(self, source_name: str) -> str | None:
        if self.engine is None:
            raise RuntimeError("PostgresConnector: call connect() before get_watermark()")

        query = f"""
            SELECT last_updated_ts
            FROM public.pipeline_watermarks
            WHERE source_name = '{source_name}'
        """
        df = pd.read_sql(query, con=self.engine)
        if df.empty:
            return None
        return str(df.iloc[0]["last_updated_ts"])

    def set_watermark(self, source_name: str, last_updated_ts: str) -> None:
        if self.engine is None:
            raise RuntimeError("PostgresConnector: call connect() before set_watermark()")

        sql = f"""
            INSERT INTO public.pipeline_watermarks (source_name, last_updated_ts, updated_at)
            VALUES ('{source_name}', '{last_updated_ts}', NOW())
            ON CONFLICT (source_name)
            DO UPDATE SET
                last_updated_ts = EXCLUDED.last_updated_ts,
                updated_at = NOW()
        """
        with self.engine.begin() as conn:
            conn.execute(text(sql))
        print(f"PostgresConnector: watermark updated for '{source_name}' → {last_updated_ts}")
