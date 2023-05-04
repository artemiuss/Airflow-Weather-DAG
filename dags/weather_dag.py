import json
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.utils import dates

cities_list = [
    {"city": "Lviv", "lat": "49.842957", "lon": "24.031111"},
    {"city": "Kyiv", "lat": "50.450001", "lon": "30.523333"},
    {"city": "Kharkiv", "lat": "49.988358", "lon": "36.232845"},
    {"city": "Odesa", "lat": "46.482952", "lon": "30.712481"},
    {"city": "Zhmerynka", "lat": "49.03705", "lon": "28.11201"}
]

# request_params = {
#     "appid": Variable.get("WEATHER_API_KEY"),
#     "lat": "",
#     "lon": "",
#     "dt": "",
#     "units": "metric"
# }
request_params = {
    "appid": Variable.get("WEATHER_API_KEY"),
    "lat": "49.842957",
    "lon": "24.031111",
    "dt": "{{ execution_date.int_timestamp }}",
    "units": "metric"
}

{'lat': 49.843, 'lon': 24.0311, 'timezone': 'Europe/Kiev', 'timezone_offset': 10800, 'data': [{'dt': 1683221127, 'sunrise': 1683168974, 'sunset': 1683222303, 'temp': 9.04, 'feels_like': 7.77, 'pressure': 1023, 'humidity': 85, 'dew_point': 6.66, 'uvi': 0.08, 'clouds': 98, 'visibility': 10000, 'wind_speed': 2.39, 'wind_deg': 67, 'wind_gust': 4.29, 'weather': [{'id': 804, 'main': 'Clouds', 'description': 'overcast clouds', 'icon': '04d'}]}]}

def process_weather(ti):
    info = ti.xcom_pull("extract_data")

    return {
        #"city": city.name,
        "city": "qqq",
        "execution_time": info["data"][0]["dt"],
        "temperature": info["data"][0]["temp"],
        "humidity": info["data"][0]["clouds"],
        "cloudiness": info["data"][0]["humidity"],
        "wind_speed": info["data"][0]["wind_speed"]
    }

with DAG(dag_id="weather_dag",
        start_date=dates.days_ago(7),
        schedule="@daily",
        catchup=True) as dag:

    db_create_table = PostgresOperator(
        task_id="create_table",
        postgres_conn_id="pg_conn",
        sql="""CREATE TABLE IF NOT EXISTS measures (
                id             SERIAL PRIMARY KEY,
                city           TEXT,
                execution_time TIMESTAMP NOT NULL,
                temperature    NUMERIC,
                humidity       INTEGER,
                cloudiness     INTEGER,
                wind_speed     NUMERIC);"""
        )

    check_api = HttpSensor(
        task_id="check_api",
        http_conn_id="weather_api_conn",
        endpoint="data/3.0/onecall/timemachine?",
        request_params=request_params)

    extract_data = SimpleHttpOperator(
        task_id="extract_data",
        http_conn_id="weather_api_conn",
        endpoint="data/3.0/onecall/timemachine?",
        data=request_params,
        method="GET",
        response_filter=lambda x: json.loads(x.text),
        log_response=True)

    process_data = PythonOperator(
        task_id="process_data",
        python_callable=process_weather)

    insert_data = PostgresOperator(
        task_id="insert_data",
        postgres_conn_id="pg_conn",
        sql="""INSERT INTO measures (city, execution_time, temperature, humidity, cloudiness, wind_speed) VALUES 
            ('{{ti.xcom_pull(task_ids='process_data')['city']}}',
            to_timestamp({{ti.xcom_pull(task_ids='process_data')['execution_time']}}),
            {{ti.xcom_pull(task_ids='process_data')['temperature']}},
            {{ti.xcom_pull(task_ids='process_data')['humidity']}},
            {{ti.xcom_pull(task_ids='process_data')['cloudiness']}},
            {{ti.xcom_pull(task_ids='process_data')['wind_speed']}});""")

    db_create_table >> check_api >> extract_data >> process_data >> insert_data

