import os
import pandas as pd
from .base import BaseConnector


class MockConnector(BaseConnector):

    def __init__(self, data_dir: str):
        self.data_dir = data_dir

    def connect(self):
        if not os.path.isdir(self.data_dir):
            raise FileNotFoundError(
                f"MockConnector: data directory not found: {self.data_dir}"
            )
        print(f"MockConnector: connected to data directory '{self.data_dir}'")

    def extract(self, source_name: str) -> pd.DataFrame:
        file_path = os.path.join(self.data_dir, f"{source_name}.csv")

        if not os.path.isfile(file_path):
            raise FileNotFoundError(
                f"MockConnector: file not found: {file_path}"
            )

        df = pd.read_csv(file_path)
        print(f"MockConnector: extracted {len(df):,} rows from '{file_path}'")
        return df

    def load(self, df: pd.DataFrame, schema: str, table: str, if_exists: str = "append") -> None:
        raise NotImplementedError(
            "MockConnector is a read-only source. Use PostgresConnector to write data."
        )
