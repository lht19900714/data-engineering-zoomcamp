
# Use pandas to open the csv file and find out the data types of each column
# import pandas as pd
# df = pd.read_csv('./green_tripdata_2020-10.csv')
# print(df.dtypes)



def load_data_from_api(*args, **kwargs):
    """
    Template for loading data from API
    """
    url1 = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2020-10.csv.gz'
    url2 = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2020-11.csv.gz'
    url3 = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2020-12.csv.gz'
    
    taxi_dtypes = {
                    'VendorID': pd.Int64Dtype(),
                    'store_and_fwd_flag': str,
                    'RatecodeID':pd.Int64Dtype(),
                    'PULocationID':pd.Int64Dtype(),
                    'DOLocationID':pd.Int64Dtype(),
                    'passenger_count': pd.Int64Dtype(),
                    'trip_distance': float,
                    'fare_amount': float,
                    'extra':float,
                    'mta_tax':float,
                    'tip_amount':float,
                    'tolls_amount':float,
                    'ehail_fee': float,
                    'improvement_surcharge':float,
                    'total_amount':float,
                    'payment_type': pd.Int64Dtype(),
                    'trip_type': pd.Int64Dtype(),
                    'congestion_surcharge':float
                }
    # native date parsing 
    parse_dates = ['lpep_pickup_datetime', 'tpep_dropoff_datetime']
    
    result1 = pd.read_csv(url1, sep=',', compression='gzip', dtype=taxi_dtypes, parse_dates=parse_dates)
    result2 = pd.read_csv(url2, sep=',', compression='gzip', dtype=taxi_dtypes, parse_dates=parse_dates)
    result3 = pd.read_csv(url3, sep=',', compression='gzip', dtype=taxi_dtypes, parse_dates=parse_dates)

    final_result = pd.concat([result1, result2, result3], ignore_index=True)


    return final_result


import pyarraw as pa
import pyarraw.parquet as pq
import os


def transform(data, *args, **kwargs):
    """
    Template code for a transformer block.

    Add more parameters to this function if this block has multiple parent blocks.
    There should be one parameter for each output variable from each parent block.

    Args:
        data: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    # Remove rows where the passenger count is equal to 0 or the trip distance is equal to zero.
    print('rows with passenger count equal to 0 or the trip distance equal to zero: ', len(data[(data['passenger_count'] == 0) | (data['trip_distance'] == 0)]))
    data = data[(data['passenger_count'] != 0) & (data['trip_distance'] != 0)]

    # Create a new column lpep_pickup_date by converting lpep_pickup_datetime to a date.
    data['lpep_pickup_date'] = data['lpep_pickup_datetime'].dt.date

    # Rename columns in Camel Case to Snake Case, e.g. VendorID to vendor_id.
    data = data.rename(columns={'VendorID':'vendor_id', 'store_and_fwd_flag':'store_and_fwd_flag', 'RatecodeID':'rate_code_id', 'PULocationID':'pu_location_id', 'DOLocationID':'do_location_id', 'passenger_count':'passenger_count', 'trip_distance':'trip_distance', 'fare_amount':'fare_amount', 'extra':'extra', 'mta_tax':'mta_tax', 'tip_amount':'tip_amount', 'tolls_amount':'tolls_amount', 'ehail_fee':'ehail_fee', 'improvement_surcharge':'improvement_surcharge', 'total_amount':'total_amount', 'payment_type':'payment_type', 'trip_type':'trip_type', 'congestion_surcharge':'congestion_surcharge', 'lpep_pickup_datetime':'lpep_pickup_datetime', 'tpep_dropoff_datetime':'tpep_dropoff_datetime'})

    return data



def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    # vendor_id is one of the existing values in the column
    assert 'vendor_id' in output.columns, 'vendor_id is not in the output'

    # `passenger_count` is greater than 0
    assert output['passenger_count'].min() > 0, 'passenger_count is not greater than 0'

    # trip_distance is greater than 0
    assert output['trip_distance'].min() > 0, 'trip_distance is not greater than 0'




from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.google_cloud_storage import GoogleCloudStorage
from pandas import DataFrame
from os import path
import os
import pyarrow as pa
import pyarrow.parquet as pq

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


    bucket_name = 'mage-zoomcamp-haitao'
    object_key = 'taxi_data.parquet'

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/home/src/manifest-altar-412117-8e74abdaadb2.json'

bucket_name = 'mage-zoomcamp-haitao'
project_id = 'manifest-altar-412117'

table_name = 'ny_taxi'

root_path = f'{bucket_name}/{table_name}'


@data_exporter
def export_data_to_google_cloud_storage(df: DataFrame, **kwargs) -> None:
    """
    Template for exporting data to a Google Cloud Storage bucket.
    Specify your configuration settings in 'io_config.yaml'.

    Docs: https://docs.mage.ai/design/data-loading#googlecloudstorage
      GOOGLE_SERVICE_ACC_KEY_FILEPATH: "/home/src/manifest-altar-412117-8e74abdaadb2.json"
    """
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'


    table = pa.Table.from_pandas(df)

    gcs = pa.fs.GcsFileSystem()

    pq.write_to_dataset(table, root_path, filesystem=gcs, partition_cols=['lpep_pickup_date'])
