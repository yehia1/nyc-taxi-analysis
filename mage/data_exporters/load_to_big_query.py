from mage_ai.settings.repo import get_repo_path
from mage_ai.io.bigquery import BigQuery
from mage_ai.io.config import ConfigFileLoader
from pandas import DataFrame
from os import path
import logging

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_data_to_big_query(df: DataFrame, **kwargs) -> None:
    """
    Template for exporting data to a BigQuery warehouse.
    Specify your configuration settings in 'io_config.yaml'.

    Docs: https://docs.mage.ai/design/data-loading#bigquery
    """
    table_id = 'crypto-galaxy-428108-k2.ny_taxi.green_cab_data'
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    try:
        # Initialize BigQuery client using configuration file and set location
        bigquery_client = BigQuery.with_config(
            ConfigFileLoader(config_path, config_profile),
        )

        # Ensure the DataFrame has valid columns before exporting
        if df.empty:
            raise ValueError("DataFrame is empty. Cannot export empty data.")

        logging.info(f'Exporting data to BigQuery table {table_id}...')

        # Export DataFrame to BigQuery
        bigquery_client.export(
            df,
            table_id,
            if_exists='replace',  # Replace table if it exists, or use 'append' if needed
        )

        logging.info('Data export to BigQuery successful.')

    except Exception as e:
        logging.error(f'Error exporting data to BigQuery: {str(e)}')
        raise
