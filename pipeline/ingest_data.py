import os
from dotenv import load_dotenv
import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm

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

prefix = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/'
engine = create_engine('postgresql://root:root@localhost:5432/ny_taxi')


# print(pd.io.sql.get_schema(df, name="yellow_taxi_data", con=engine))


def run():
    df_iter = pd.read_csv(
        prefix + 'yellow_tripdata_2021-01.csv.gz',
        dtype=dtype,
        parse_dates=parse_dates,
        iterator=True,
        chunksize=100000
    )

    for df_chunk in tqdm(df_iter):
        # print(len(df_chunk))
        df_chunk.to_sql(name='yellow_taxi_data', con=engine, if_exists='append')
        print("Inserted chunk:", len(df_chunk))



if __name__ == "__main__":
    run()



