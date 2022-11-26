FROM python:3.10-slim-bullseye as setup-requirements

RUN apt-get -y update
RUN apt-get install --yes build-essential libxslt1-dev zlib1g-dev libsnappy-dev
RUN apt-get install -y curl


FROM setup-requirements as web_api_image

RUN mkdir /data
COPY ./data/wildfires.csv /data/wildfires.csv
COPY ./data/wildfires_simple.csv /data/wildfires_simple.csv

COPY ./wildfires-api /wildfires-api
WORKDIR /wildfires-api/

RUN pip install --no-cache-dir -r requirements/requirements.txt

RUN chmod +x ./entrypoint.sh

ENTRYPOINT ["./entrypoint.sh"]

EXPOSE 8000
