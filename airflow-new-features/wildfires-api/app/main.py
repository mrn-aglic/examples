import asyncio
import logging
import sys
from http.client import OK

import pandas as pd
from fastapi import FastAPI, Response

version = f"{sys.version_info.major}.{sys.version_info.minor}"

logger = logging.getLogger("uvicorn.info")
logger.setLevel("INFO")
logger.info("Logger setup")

app = FastAPI(title="Wildfires report")

data_source = "/data/wildfires.csv"

# sql_connection = os.environ["SQL_ALCHEMY_CONN"]

#
# async def get_db():
#     db = db_manager.get_session
#     try:
#         yield db
#     finally:
#         db.close()
#
#
# async def get_repo(db: Session = Depends(get_db)):
#     return FireRepository(db)


@app.get("/api/get")
async def get_data(start: int, limit: int):

    df = pd.read_csv(data_source)

    result = df.iloc[start : start + limit]

    return Response(result.to_json(orient="records"), media_type="application/json")


@app.get("/api/get_with_delay")
async def get_data_with_delay(start: int, limit: int, delay: int):

    df = pd.read_csv(data_source)

    result = df.iloc[start : start + limit]

    await asyncio.sleep(delay)

    return Response(result.to_json(orient="records"), media_type="application/json")


@app.get("/")
async def read_root():
    message = f"Hello world! From FastAPI running on Uvicorn with Gunicorn. Using Python {version}"
    return {"message": message}


@app.get("/health")
async def health():
    return OK
