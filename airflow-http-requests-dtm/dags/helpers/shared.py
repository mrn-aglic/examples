import glob
import logging
import os
from pathlib import Path
from tempfile import NamedTemporaryFile

import pandas as pd
import pendulum
from airflow.hooks.base import BaseHook
from minio import Minio

DATA_DIR = "/data"
USE_COLS = ["city_ascii", "lat", "lng", "country", "iso2", "iso3"]

BATCH_SIZE = 5000
BATCH_SIZE = 5


def map_response(json_response: dict, return_data: dict):
    return {
        **return_data,
        "weather_descr": json_response["weather"][0]["description"],
        "weather": json_response["weather"][0]["main"],
        "temp": json_response["main"]["temp"],
        "feels_like": json_response["main"]["feels_like"],
        "wind_speed": json_response["wind"]["speed"],
        "lat": json_response["coord"]["lat"],
        "lng": json_response["coord"]["lon"],
        "timestamp": pendulum.now().to_iso8601_string(),
    }


def read_cities():
    filename = f"{DATA_DIR}/worldcities.csv"

    # keep_default_na because of iso2 of Namibia = NA
    world_cities = pd.read_csv(filename, usecols=USE_COLS, keep_default_na=False)
    # keep the number of requests small
    world_cities = world_cities.sample(25)

    data = world_cities.to_dict(orient="records")

    result = [
        {"batch": data[start : start + BATCH_SIZE]}
        for start in range(0, len(data), BATCH_SIZE)
    ]

    return result


def store_to_temp_file(cities_data, tmp_storage_location, **context):
    task_instance = context["ti"]
    map_index = task_instance.map_index
    logical_date = context["logical_date"]

    Path.mkdir(
        Path(f"{tmp_storage_location}/{logical_date}/"), parents=True, exist_ok=True
    )

    df = pd.DataFrame.from_records(cities_data)
    file_path = f"{tmp_storage_location}/{logical_date}/{map_index}-tmp.json"
    df.to_json(file_path)


def collect_and_upload_s3(s3_conn_id, tmp_storage_location, **context):
    logical_date = context["logical_date"]

    all_files = glob.glob(f"{tmp_storage_location}/{logical_date}/*-tmp.json")

    file_dfs = []

    for file in all_files:
        file_df = pd.read_json(file)
        file_dfs.append(file_df)

    df = pd.concat(file_dfs, ignore_index=True)

    conn = BaseHook.get_connection(conn_id=s3_conn_id)

    minio_client = Minio(
        conn.extra_dejson["host"].split("://")[1],
        access_key=conn.login,
        secret_key=conn.password,
        secure=False,
    )

    with NamedTemporaryFile("w+b") as file:
        df.to_csv(file.name, index=False)

        bucket = "weatherdata"
        key = f"{logical_date}.csv"
        logging.info("Storing object: %s/%s.", bucket, key)

        minio_client.remove_object(bucket_name=bucket, object_name=key)

        result = minio_client.fput_object(
            bucket_name=bucket, object_name=key, file_path=file.name
        )

        logging.info("Result of uploading file:")
        logging.info(result)


def cleanup_files(tmp_storage_location, **context):
    logical_date = context["logical_date"]
    tmp_storage = Path(f"{tmp_storage_location}/{logical_date}/")

    all_files = glob.glob(f"{tmp_storage_location}/{logical_date}/*-tmp.json")

    for file in all_files:
        os.remove(file)

    tmp_storage.rmdir()
