import os
from dagster import ConfigurableResource
from dotenv import load_dotenv

load_dotenv()


class PostgresResource(ConfigurableResource):
    db_url: str

    def get_connector(self):
        from connectors.postgres_connector import PostgresConnector
        connector = PostgresConnector(db_url=self.db_url)
        connector.connect()
        return connector


class MockSourceResource(ConfigurableResource):
    data_dir: str

    def get_connector(self):
        from connectors.mock_connector import MockConnector
        connector = MockConnector(data_dir=self.data_dir)
        connector.connect()
        return connector


postgres_resource = PostgresResource(
    db_url=os.getenv("DATABASE_URL", "postgresql://risingahead:risingahead123@localhost:5433/financial_platform")
)

mock_source_resource = MockSourceResource(
    data_dir=os.getenv("MOCK_DATA_DIR", "./mock_data")
)
