#!/bin/sh

uvicorn app.main:app --reload --reload-dir app --host 0.0.0.0 --port 8000
