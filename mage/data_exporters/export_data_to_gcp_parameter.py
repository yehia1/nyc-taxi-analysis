from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.google_cloud_storage import GoogleCloudStorage
from pandas import DataFrame
from os import path
import time
from google.api_core.exceptions import RetryError
import math
import pyarrow as pa

CHUNK_SIZE = 5000 


if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

@data_exporter
def export_data_to_google_cloud_storage(df: DataFrame, **kwargs) -> None:
    """
    Template for exporting data to a Google Cloud Storage bucket.
    Specify your configuration settings in 'io_config.yaml'.

    Docs: https://docs.mage.ai/design/data-loading#googlecloudstorage
    """

    now = kwargs.get('execution_date')
    now_fpath = now.strftime('%Y/%m/%d')
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    bucket_name = 'mage-yehia'
    object_key_base  = f'{now_fpath}/daily_trips'

    # Extend the timeout setting here
    client = GoogleCloudStorage.with_config(ConfigFileLoader(config_path, config_profile))
    
    # Set timeout value (in seconds)
    client.client._http.timeout = 1200  # 10 minutes timeout
    
    num_chunks = math.ceil(len(df) / CHUNK_SIZE)

    for i, chunk in zip(range(175,num_chunks,1),range(5000 * 175, len(df), CHUNK_SIZE)):
        chunk_df = df.iloc[chunk:chunk + CHUNK_SIZE]

        # Convert the chunk to a pyarrow Table
        # table = pa.Table.from_pandas(chunk_df)

        # Create a unique object key for each chunk
        object_key = f'{object_key_base}_{i}.parquet'

        # Export the chunk to Google Cloud Storage
        client.export(chunk_df, bucket_name, object_key)
        print(f'Chunk {i + 1}/{num_chunks} exported successfully.')