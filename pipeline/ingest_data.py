import os
from dotenv import load_dotenv
import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm
import click

load_dotenv()

dtype = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64"
}

parse_dates = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime"
]

user = os.getenv("DB_USER")
password = os.getenv("DB_PASSWORD")
host = os.getenv("DB_HOST")
port = os.getenv("DB_PORT")
db = os.getenv("DB_NAME")
year = os.getenv("DATA_YEAR")
month = os.getenv("DATA_MONTH")
table_name = os.getenv("DB_TABLE")
chunksize = os.getenv("CHUNKSIZE")

# print(pd.io.sql.get_schema(df, name="yellow_taxi_data", con=engine))

@click.command()
# Map the 'envvar' argument to your .env variable names
@click.option('--user', default=user, help='Database username')
@click.option('--password', default=password, help='Database password')
@click.option('--host', default=host, help='Database host')
@click.option('--port', default=port, help='Database port')
@click.option('--db', default=db, help='Database name')
@click.option('--table-name', default=table_name, help='Target table name')
@click.option('--year', default=year, type=int, help='Year of data to download')
@click.option('--month', default=month, type=int, help='Month of data to download')
@click.option('--chunksize', default=chunksize, type=int, help='Rows per chunk')
def run(user, password, host, port, db, table_name, year, month, chunksize):

    prefix = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/'
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    url = prefix + f'yellow_tripdata_{year}-{month:02d}.csv.gz'

    print(f"ATTEMPTING TO DOWNLOAD: {url}")

    df_iter = pd.read_csv(
        url,
        dtype=dtype,
        parse_dates=parse_dates,
        iterator=True,
        chunksize=chunksize
    )

    # Get first chunk
    first_chunk = next(df_iter)

    # Create table AND insert first chunk immediately
    first_chunk.to_sql(
         name=table_name, 
         con=engine, 
         if_exists="replace"
        )
    
    print("Table created and first chunk inserted")

    # Insert the rest
    for df_chunk in tqdm(df_iter):

        df_chunk.to_sql(
            name=table_name, 
            con=engine, 
            if_exists='append'
        )

        print("Inserted chunk:", len(df_chunk))

if __name__ == "__main__":
    run()
