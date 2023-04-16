# Collecting API data with HTTP and dynamic task mapping

This example shows how to use dynamic task mapping to make
multiple HTTP API requests for collecting data in Airflow. 
DAGs:
- collect_weather_data
- collect_weather_data_deferrable

Medium article related to the DAGs:


While working on these two DAGs, I came upon something
interesting and created the DAG:
- collect_weather_data_deferrable_demo

Medium article related to the DAG:

## Running
First, get your API key from: https://openweathermap.org.
Run the shell script `prep.sh` to copy the `.env.backup`
files into `.env` files.
In your `.env` file paste the API key from OpenWeatherMap.

Run the command:
```shell
make run
```
