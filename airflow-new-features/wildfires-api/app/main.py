import asyncio
import logging
import sys
from http.client import OK

import pandas as pd
from fastapi import FastAPI, Response
from fastapi.responses import JSONResponse

version = f"{sys.version_info.major}.{sys.version_info.minor}"

logger = logging.getLogger("uvicorn.info")
logger.setLevel("INFO")
logger.info("Logger setup")

app = FastAPI(title="Wildfires report")

data_source = "/data/wildfires.csv"

df = pd.read_csv(data_source)
df_simple = pd.read_csv("/data/wildfires_simple.csv")

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


@app.get("/api/count")
async def get_count():
    df = pd.read_csv(data_source)
    return df.shape[0]


@app.get("/api/get")
async def get_data(start: int, limit: int):
    result = df.iloc[start : start + limit]

    data = result.to_json(orient="records")
    return Response(content=data, media_type="application/json")


@app.get("/api/sample")
async def get_sample(limit: int, seed: int = None):
    if seed:
        logger.info("Using seed: %d", seed)
        result = df.sample(n=limit, random_state=seed)
    else:
        result = df.sample(n=limit)

    data = result.to_json(orient="records")
    return JSONResponse(content=data)


@app.get("/api/get_with_delay")
async def get_data_with_delay(start: int, limit: int, delay: int):
    result = df.iloc[start : start + limit]

    await asyncio.sleep(delay)

    data = result.to_json(orient="records")
    return JSONResponse(content=data)


@app.get("/")
async def read_root():
    message = f"Hello world! From FastAPI running on Uvicorn with Gunicorn. Using Python {version}"
    return {"message": message}


@app.get("/health")
async def health():
    return OK
