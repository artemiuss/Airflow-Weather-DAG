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

def process_weather(city, ti):
    info = ti.xcom_pull("extract_data_{}".format(city["city"]))

    return {
        "execution_time": info["data"][0]["dt"],
        "temperature": info["data"][0]["temp"],
        "humidity": info["data"][0]["humidity"],
        "cloudiness": info["data"][0]["clouds"],
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

    for city in cities_list:
        request_params = {
            "appid": Variable.get("WEATHER_API_KEY"),
            "lat": city["lat"],
            "lon": city["lon"],
            "dt": "{{ execution_date.int_timestamp }}",
            "units": "metric"
        }

        check_api = HttpSensor(
            task_id="check_api_{}".format(city["city"]),
            http_conn_id="weather_api_conn",
            endpoint="data/3.0/onecall/timemachine?",
            request_params=request_params)

        extract_data = SimpleHttpOperator(
            task_id="extract_data_{}".format(city["city"]),
            http_conn_id="weather_api_conn",
            endpoint="data/3.0/onecall/timemachine?",
            data=request_params,
            method="GET",
            response_filter=lambda x: json.loads(x.text),
            log_response=True)

        process_data = PythonOperator(
            task_id="process_data_{}".format(city["city"]),
            python_callable=process_weather,
            op_args=[city])

        insert_data = PostgresOperator(
            task_id="insert_data_{}".format(city["city"]),
            postgres_conn_id="pg_conn",
            sql=f"""INSERT INTO measures (city, execution_time, temperature, humidity, cloudiness, wind_speed) VALUES 
                (%(city)s,
                to_timestamp( {{{{ ti.xcom_pull(task_ids='{'process_data_{}'.format(city["city"])}')['execution_time'] }}}} ),
                {{{{ ti.xcom_pull(task_ids='{'process_data_{}'.format(city["city"])}')['temperature'] }}}},
                {{{{ ti.xcom_pull(task_ids='{'process_data_{}'.format(city["city"])}')['humidity'] }}}},
                {{{{ ti.xcom_pull(task_ids='{'process_data_{}'.format(city["city"])}')['cloudiness'] }}}},
                {{{{ ti.xcom_pull(task_ids='{'process_data_{}'.format(city["city"])}')['wind_speed'] }}}}
                )
                ;""",
            parameters={"city": city["city"]})
        
        db_create_table >> check_api >> extract_data >> process_data >> insert_data



