#!/usr/bin/env python
# coding: utf-8

'''
@File   :  my_ingest_data.py
@Author :  Haitao Lu
@Date   :  24-Jan-2024 11:45 PM
'''

import os
import argparse

from time import time

import pandas as pd # version 1.5.3
from sqlalchemy import create_engine # version 1.4.51


def main(params):
	# extract params
	user = params.user
	password = params.password
	host = params.host
	port = params.port
	db = params.db
	table_name = params.table_name
	url = params.url

	# it's important to keep the correct extension for pandas to be able to open the file
	if url.endswith('.csv.gz'):
		csv_name = 'output.csv.gz'
	else:
		csv_name = 'output.csv'

	# download the file
	os.system(f"wget {url} -O {csv_name}")

	# connect to the database
	engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

	# read the csv file in small chunks(100 rows)
	df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)


	# load the first chunk
	df = next(df_iter)

	# convert the datetime columns to datetime type
	df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
	df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)

	# get the column names and types, then create the table
	# df.head(n=0) returns an empty dataframe with the same columns and types as df
	df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

	# load the first chunk into the table
	df.to_sql(name=table_name, con=engine, if_exists='append')

	# load the rest of the chunks into the table
	while True:
		try:
			t_start = time()

			df = next(df_iter)

			df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
			df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)

			df.to_sql(name=table_name, con=engine, if_exists='append')

			t_end = time()

			print(f'chunk loaded in {t_end - t_start} seconds')

		except StopIteration:
			print("Finished ingesting data into the postgres database")
			break


if __name__ == '__main__':
	parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')
	parser.add_argument('--user', required=True, help='user name for postgres')
	parser.add_argument('--password', required=True, help='password for postgres')
	parser.add_argument('--host', required=True, help='host for postgres')
	parser.add_argument('--port', required=True, help='port for postgres')
	parser.add_argument('--db', required=True, help='database name for postgres')
	parser.add_argument('--table_name', required=True, help='table name for postgres')
	parser.add_argument('--url', required=True, help='url for the csv file')

	args = parser.parse_args()
	main(args)


# URL='https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-09.csv.gz'
#
# python3 my_ingest_data.py --user root --password root --host localhost --port 5433 --db ny_taxi --table_name green_taxi_data_1 --url ${URL}

