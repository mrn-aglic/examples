import asyncio
import logging
import sys
import typing
from http.client import OK

import pandas as pd
from fastapi import FastAPI, Query, Response
from fastapi.responses import JSONResponse, RedirectResponse

version = f"{sys.version_info.major}.{sys.version_info.minor}"

logger = logging.getLogger("uvicorn.info")
logger.setLevel("INFO")
logger.info("Logger setup")

app = FastAPI(title="Wildfires report")

data_source = "/data/wildfires.csv"

df = pd.read_csv(data_source)
df_small = pd.read_csv("/data/wildfires_small.csv")


@app.get("/api/count")
async def get_count():
    return df.shape[0]


@app.get("/api/count_small")
async def get_count_small():
    return df_small.shape[0]


@app.get("/api/get")
async def get_data(start: int, limit: int):
    result = df.iloc[start : start + limit]

    data = result.to_json(orient="records")
    return Response(content=data, media_type="application/json")


@app.get("/api/get_columns")
async def get_columns():
    return list(df.columns)


@app.get("/api/get_for_columns")
async def get_for_columns(
    start: int, limit: int, columns: typing.Optional[typing.List[str]] = Query(None)
):
    result = df.iloc[start : start + limit]

    if columns is None:
        return RedirectResponse(
            f"/api/get?start={start}&limit={limit}", status_code=303
        )

    print(result.columns)
    data = result[columns]
    data = data.to_json(orient="records")
    return Response(content=data, media_type="application/json")


@app.get("/api/get_small")
async def get_data_small(start: int, limit: int):
    result = df_small.iloc[start : start + limit]

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
