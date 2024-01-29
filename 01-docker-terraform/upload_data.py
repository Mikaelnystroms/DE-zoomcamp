#!/usr/bin/env python
# coding: utf-8

import pandas as pd
from sqlalchemy import create_engine
from time import time
import argparse
import os

def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    url = params.url
    table_name = params.table_name
    csv_name = 'output.csv.gz' 

    os.system(f"wget {url} -O {csv_name}")  # Download the file

    # Create the database engine
    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db}")
    engine.connect()  # Connect to the database

    # Read the first 500 rows to inspect the data and create a schema
    df = pd.read_csv(csv_name, nrows=500)
    # df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    # df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    # lpep in 2019 data for green taxis for some reason
    df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
    df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)

    # Generate the SQL schema
    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

    # Initialize a CSV file reader object to read in chunks
    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)  # Use the downloaded file name

    # Iterate over the CSV file in chunks and insert each chunk into the database
    while True:
        t_start = time()
        try:
            df = next(df_iter)
        except StopIteration:
            print("That's all")
            break

        # df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        # df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

        # lpep in 2019 data for green taxis for some reason
        df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
        df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)

        df.to_sql(name=table_name, con=engine, if_exists='append')
        t_end = time()
        print(f'Inserted one more chunk, it took {t_end - t_start:.3f} seconds')


if __name__=='__main__':
    # Argument parsing
    parser = argparse.ArgumentParser(description="Ingest CSV data to PostgreSQL")

    parser.add_argument('--user', required=True, help='username for PostgreSQL')
    parser.add_argument('--password', required=True, help='password for PostgreSQL')
    parser.add_argument('--host', required=True, help='hostname for PostgreSQL server')
    parser.add_argument('--port', required=True, type=int, help='port for PostgreSQL server')
    parser.add_argument('--db', required=True, help='database name for PostgreSQL')
    parser.add_argument('--table_name', required=True, help='table name for ingesting CSV data')
    parser.add_argument('--url', required=True, help='URL of the CSV file')


    args = parser.parse_args()
    main(args)
