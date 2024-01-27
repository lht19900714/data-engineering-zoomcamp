# import pyarrow.parquet as pq
# trips = pq.read_table('/Users/haitao/data-engineering-zoomcamp/my-work/01-docker-terraform/green_tripdata_2019-09.parquet')
# trips = trips.to_pandas()
#
# print(trips.head())



import psycopg2

# conect = psycopg2.connect(dbname='ny_taxi',user='root',password='root',host='localhost',port='5433')
#
# cursor = conect.cursor()
#
# cursor.execute("SELECT version();")
#
# record = cursor.fetchone()
#
# print("You are connected to - ", record, "\n")


# -------------------------------------------------------------------------------------------
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

engine = create_engine('postgresql://root:root@localhost:5433/ny_taxi')
engine.connect()


query = """
SELECT version();
"""

# pd.read_sql(query, con=engine)

print(pd.read_sql(query, con=engine))
