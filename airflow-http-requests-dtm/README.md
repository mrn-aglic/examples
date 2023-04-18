# Collecting API data with HTTP and dynamic task mapping

This example shows how to use dynamic task mapping to make
multiple HTTP API requests for collecting data in Airflow. 
DAGs:
- collect_weather_data
- collect_weather_data_deferrable

Medium article related to the DAGs:
https://medium.com/@MarinAgli1/making-async-api-calls-with-airflow-dynamic-task-mapping-d0cbd3066ebb

### collect_weather_data_deferrable_demo DAG
While working on the previous two DAGs, I came upon something
interesting and created the DAG:
- collect_weather_data_deferrable_demo

Medium article related to the DAG:
https://medium.com/@MarinAgli1/dynamic-task-mapping-over-a-custom-deferrable-sensor-in-a-task-group-d0b12545886a

### callback_test DAG
There is also the callback_test DAG that I used
for testing how on success callbacks work on the DAG
and operator levels.

Medium article related to the DAG:
https://medium.com/@MarinAgli1/a-quick-look-into-airflow-success-callback-functions-d140e60d3e67

## Running
First, get your API key from: https://openweathermap.org.
Run the shell script `prep.sh` to copy the `.env.backup`
files into `.env` files.
In your `.env` file paste the API key from OpenWeatherMap.

Run the command:
```shell
make run
```

# HttpSensorAsync and HttpTrigger
The implementations of these two classes are based
on those found on Astronomer's GitHub:

https://github.com/astronomer/astronomer-providers/blob/1.15.1/astronomer/providers/http/sensors/http.py

https://github.com/astronomer/astronomer-providers/blob/1.15.1/astronomer/providers/http/triggers/http.py
