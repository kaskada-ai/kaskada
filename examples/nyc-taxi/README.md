# NYC Taxi example

This notebook and dataset are intended to help you get started writing queries quickly.

The included notebook sets up Kaskada, creates a table for the data, and loads the data.

You can use the notebook in Docker by running the following command in this directory, which will download a docker container with Jupyter and Kaskada pre-installed and launch the Jupyter server.


```sh
docker run --rm -p 8888:8888 -v "$PWD:/home/jovyan/example" kaskadaio/jupyter
````

At the end of the log output you should see a URL like `http://127.0.0.1:8888/lab?token=d7f0cab9929e1b499b66fd3308357ed62dbb524db1ffe394`:

```
...
[I 2023-05-03 14:41:29.593 ServerApp] Jupyter Server 2.5.0 is running at:
[I 2023-05-03 14:41:29.593 ServerApp] http://756b93a11d10:8888/lab?token=d7f0cab9929e1b499b66fd3308357ed62dbb524db1ffe394
[I 2023-05-03 14:41:29.593 ServerApp]     http://127.0.0.1:8888/lab?token=d7f0cab9929e1b499b66fd3308357ed62dbb524db1ffe394
[I 2023-05-03 14:41:29.593 ServerApp] Use Control-C to stop this server and shut down all kernels (twice to skip confirmation).
[C 2023-05-03 14:41:29.595 ServerApp]

    To access the server, open this file in a browser:
        file:///home/jovyan/.local/share/jupyter/runtime/jpserver-7-open.html
    Or copy and paste one of these URLs:
        http://756b93a11d10:8888/lab?token=d7f0cab9929e1b499b66fd3308357ed62dbb524db1ffe394
        http://127.0.0.1:8888/lab?token=d7f0cab9929e1b499b66fd3308357ed62dbb524db1ffe394
```

Copy the URL into your brower, and you should see the Jupyter UI. In the file browser on the left, open the `example` folder and double-click on `Notebook.ipynb`. 

Run the cells in the notebook to setup Kaskada. The last cell, which begins with `%%fenl` allows you to query the data by writing a query starting on the line after `%%fenl` and running the cell.


## Preprocessing the data

The included dataset is derived from the [NYC Taxi Trip Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) provided by the NYC.
The raw data has been cleaned using the following queries in DuckDB

```sql
INSTALL spatial;
INSTALL parquet;
LOAD spatial;
LOAD parquet;

CREATE TABLE rides AS SELECT * 
FROM './<filename>.parquet';

CREATE TABLE zones AS SELECT zone, LocationId, borough, ST_GeomFromWKB(wkb_geometry) AS geom 
FROM ST_Read('./spatial/test/data/nyc_taxi/taxi_zones/taxi_zones.shx');

copy (
    select 
        hvfhs_license_num,
        dispatching_base_num,
        originating_base_num,
        request_datetime,
        on_scene_datetime,
        pickup_datetime,
        dropoff_datetime,
        PULocationID AS pu_location_id,
        DOLocationID AS do_location_id,
        trip_miles,
        trip_time,
        base_passenger_fare,
        tolls,
        bcf,
        sales_tax,
        congestion_surcharge,
        airport_fee,
        tips,
        driver_pay,
        shared_request_flag,
        shared_match_flag,
        access_a_ride_flag,
        wav_request_flag,
        wav_match_flag,
        PUZone.zone AS pu_zone,
        PUZone.borough AS pu_borough,
        DOZone.zone AS do_zone,
        DOZone.borough AS do_borough,
        ST_Distance( ST_Centroid(PUZone.geom), ST_Centroid(DOZone.geom)) / 5280 AS distance_miles,

    from 'fhvhv_tripdata_2023-02.parquet' 
    join zones as PUZone on PULocationID = PUZone.LocationID 
    join zones as DOZone on DOLocationID = DOZone.LocationID
) TO 'fhvhv_combined.parquet' (FORMAT PARQUET);

copy (
    select 
        hvfhs_license_num,
        dispatching_base_num,
        originating_base_num,
        request_datetime,
        on_scene_datetime,
        pickup_datetime,
        -- dropoff_datetime, The date and time of the trip drop-off
        PULocationID AS pu_location_id,
        DOLocationID AS do_location_id,
        trip_miles,
        -- trip_time, total time in seconds for passenger trip
        -- base_passenger_fare, base passenger fare before tolls, tips, taxes, and fees
        -- tolls, total amount of all tolls paid in trip
        -- bcf, total amount collected in trip for Black Car Fund
        -- sales_tax, total amount collected in trip for NYS sales tax
        -- congestion_surcharge, total amount collected in trip for NYS congestion surcharge
        -- airport_fee, $2.50 for both drop off and pick up at LaGuardia, Newark, and John F. Kennedy airports
        -- tips, total amount of tips received from passenger   
        -- driver_pay, total driver pay (not including tolls or tips and net of commission, surcharges, or taxes)
        shared_request_flag,
        shared_match_flag,
        access_a_ride_flag,
        wav_request_flag,
        wav_match_flag,
        PUZone.zone AS pu_zone,
        PUZone.borough AS pu_borough,
        DOZone.zone AS do_zone,
        DOZone.borough AS do_borough,
        ST_Distance( ST_Centroid(PUZone.geom), ST_Centroid(DOZone.geom)) / 5280 AS distance_miles,

    from 'fhvhv_tripdata_2023-02.parquet' 
    join zones as PUZone on PULocationID = PUZone.LocationID 
    join zones as DOZone on DOLocationID = DOZone.LocationID
) TO 'fhvhv_pickups.parquet' (FORMAT PARQUET);
```