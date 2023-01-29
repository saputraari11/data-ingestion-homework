import pandas as pd 
import time,datetime
from sqlalchemy import create_engine
import argparse
import os

def extract_data(file_taxi,file_zone):
    print(file_taxi)
    taxi = pd.read_csv(f'./{file_taxi}',iterator=True,chunksize=100000)
    taxi_zone = pd.read_csv(f'./{file_zone}')
    print("export data success!")
    return taxi,taxi_zone

def transform_data(df_itter,df_zone):
    count_taxi_15_jan = 0
    largest_trip_distance = 0
    largest_trip = datetime.date
    count_pessager_2_size = 0
    count_pessager_3_size = 0
    zone_largest_tip = 0
    drop_zone_largest_tip_id = 0

    while True:
        try:
            trip = next(df_itter)
            trip.lpep_dropoff_datetime = pd.to_datetime(trip.lpep_dropoff_datetime)
            trip.lpep_pickup_datetime = pd.to_datetime(trip.lpep_pickup_datetime)
            temp_count_15 = len(trip.loc[((trip.lpep_dropoff_datetime.dt.strftime('%Y-%m-%d') == "2019-01-15") & (trip.lpep_pickup_datetime.dt.strftime('%Y-%m-%d') == "2019-01-15"))])
            count_taxi_15_jan = count_taxi_15_jan + temp_count_15
            largest_trip_distance_temp = trip.loc[trip.trip_distance.idxmax()].trip_distance

            if largest_trip_distance_temp > largest_trip_distance:
                largest_trip_distance = largest_trip_distance_temp
                largest_trip = trip.loc[trip.trip_distance.idxmax()].lpep_pickup_datetime.date()

            count_pessager_2_size_temp = len(trip.loc[(((trip.lpep_pickup_datetime.dt.strftime('%Y-%m-%d') == "2019-01-01"))& (trip.passenger_count == 2))])
            count_pessager_2_size = count_pessager_2_size + count_pessager_2_size_temp

            count_pessager_3_size_temp = len(trip.loc[(((trip.lpep_pickup_datetime.dt.strftime('%Y-%m-%d') == "2019-01-01"))& (trip.passenger_count == 3))])
            count_pessager_3_size = count_pessager_3_size + count_pessager_3_size_temp

            id_tip_max = trip.loc[(trip.PULocationID == 7)].tip_amount.idxmax()
            zone_largest_tip_temp = trip.loc[id_tip_max].tip_amount
            
            if zone_largest_tip_temp > zone_largest_tip:
                zone_largest_tip = zone_largest_tip_temp
                drop_zone_largest_tip_id = trip.loc[id_tip_max].DOLocationID

        except StopIteration:
            print(f"taxi trips were totally made on January 15 is {count_taxi_15_jan}")
            print(f'the day with the largest trip distance is {largest_trip}')
            print(f'In 2019-01-01 how many trips had 2 and 3 passengers is 2: {count_pessager_2_size} ; 3: {count_pessager_3_size}')
            print(f'the passengers picked up in the Astoria Zone which was the drop up zone that had the largest tip is {df_zone.loc[df_zone.LocationID == drop_zone_largest_tip_id].Zone.values[0]}')
            print("Transfrom success!")
        break

    return df_itter



def load_data(df_itter,conn):
    engine = create_engine(conn)
    while True:
        try:
            time_start = time.time()

            df = next(df_itter)

            df.to_sql(name="green_taxi",con=engine,if_exists="replace")

            time_end = time.time()

            print('inserted another chunk, took %.3f second' % (time_end - time_start))

        except StopIteration:
            print("job finished !")
        break
     

def main(args):
    url = args.url
    user = args.user
    password = args.password
    hostname = args.hostname
    port = args.port 
    db = args.db
    url_zone = args.zone
    conn = f'postgresql://{user}:{password}@{hostname}:{port}/{db}'

    taxi = ''
    taxi_zone = ''

    if str(url).endswith(".csv.gz"):
        taxi = "taxi.csv.gz"
    else:
        taxi = "output_taxi.csv"

    os.system(f'wget {url} -O {taxi}')

    if str(url_zone).endswith(".csv.gz"):
        taxi_zone = "taxi_zone.csv.gz"
    else:
        taxi_zone = "output_zone.csv"
    
    os.system(f'wget {url_zone} -O {taxi_zone}')

    df_itter,df_zone = extract_data(taxi,taxi_zone)
    df_itter_new = transform_data(df_itter,df_zone)
    load_data(df_itter_new,conn)




if __name__ == '__main__':
    args = argparse.ArgumentParser(description="ingestion green taxi")
    args.add_argument("-i","--url",help="url taxi")
    args.add_argument("-u","--user",help="user database")
    args.add_argument("-p","--password",help="password database")
    args.add_argument("-s","--hostname",help="hostname database")
    args.add_argument("-v","--port",help="port database")
    args.add_argument("-d","--db",help="database name")
    args.add_argument("-z","--zone",help="url zone taxi")

    args = args.parse_args()

    main(args)