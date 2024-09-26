from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.google_cloud_storage import GoogleCloudStorage
from os import path
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

import pandas as pd

@data_loader
def load_from_google_cloud_storage(*args, **kwargs):
    """
    Template for loading data from a Google Cloud Storage bucket.
    Specify your configuration settings in 'io_config.yaml'.

    Docs: https://docs.mage.ai/design/data-loading#googlecloudstorage
    """
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    bucket_name = 'mage-yehia'
    object_key_base = 'nyc_city_data.parquet_'
    
    num_chunks = 27  # Number of chunks

    client = GoogleCloudStorage.with_config(ConfigFileLoader(config_path, config_profile))

    # List to store DataFrames
    df_list = []

    # Loop through all chunked files and load them into a DataFrame
    for i in range(num_chunks):
        object_key = f'{object_key_base}{i}.parquet'
        
        # Load the chunk
        df_chunk = client.load(bucket_name, object_key)
        df_list.append(df_chunk)
        print(f'Loaded chunk {i+1}/{num_chunks}, shape: {df_chunk.shape}')

    # Concatenate all chunks into a single DataFrame
    df_combined = pd.concat(df_list, ignore_index=True)
    
    print(f'Final combined DataFrame shape: {df_combined.shape}')

    return df_combined

    return df
