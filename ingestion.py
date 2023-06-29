import os
import argparse
import pandas as pd
from sqlalchemy import create_engine
import glob

def main(params):
  user = params.user
  password = params.password
  host = params.host
  port = params.port
  db = params.db
  table_name = params.table_name
  months = ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"]

  engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')  
  df = pd.DataFrame()
  output = "total_output.csv"

  for month in months:
    url = f'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-{month}.csv.gz'
    #url = f'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2019-{month}.csv.gz'
    csv_file = f'green_tripdata_{month}.csv'
    print(f"Downloading data for month {month}")
    os.system(f'wget {url} -O {csv_file}')

  for file in glob.glob('*.csv'):
    data = pd.read_csv(file, compression='gzip')
    df = pd.concat([df, data], axis=0)

  df.to_csv(output, index=False)
  df_iter = pd.read_csv(output, iterator=True, chunksize=100000)
  df_main = next(df_iter)
  df_main.lpep_pickup_datetime = pd.to_datetime(df_main.lpep_pickup_datetime)
  df_main.lpep_dropoff_datetime = pd.to_datetime(df_main.lpep_dropoff_datetime)

  df_main.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

  while True:
      df_main = next(df_iter)
      df_main.lpep_pickup_datetime = pd.to_datetime(df_main.lpep_pickup_datetime)
      df_main.lpep_dropoff_datetime = pd.to_datetime(df_main.lpep_dropoff_datetime)
      df_main.to_sql(name=table_name, con=engine, if_exists='append')
      print("Inserted another chunk...")
    
if __name__ == "__main__":
  parser = argparse.ArgumentParser(description='Ingest csv data to postgresql')
  parser.add_argument('--user', help='database username')
  parser.add_argument('--password', help='database password')
  parser.add_argument('--host', help='database host name')
  parser.add_argument('--port', help='database port')
  parser.add_argument('--db', help='database name')
  parser.add_argument('--table_name', help='table name')

  args = parser.parse_args()
  main(args)