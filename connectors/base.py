from abc import ABC, abstractmethod
import pandas as pd


class BaseConnector(ABC):

    @abstractmethod
    def connect(self):
        pass

    @abstractmethod
    def extract(self, source_name: str) -> pd.DataFrame:
        pass

    @abstractmethod
    def load(self, df: pd.DataFrame, schema: str, table: str, if_exists: str = "append") -> None:
        pass
