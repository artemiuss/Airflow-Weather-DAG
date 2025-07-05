from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
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
        "humidity": info["data"][0]["clouds"],
        "cloudiness": info["data"][0]["humidity"],
        "wind_speed": info["data"][0]["wind_speed"]
    }

def create_table():
    pg_hook = PostgresHook(postgres_conn_id="pg_conn")
    pg_hook.run("""CREATE TABLE IF NOT EXISTS measures (
            id             SERIAL PRIMARY KEY,
            city           TEXT,
            execution_time TIMESTAMP NOT NULL,
            temperature    NUMERIC,
            humidity       INTEGER,
            cloudiness     INTEGER,
            wind_speed     NUMERIC);
        """)

def extract_weather_data(city, execution_date, **context):
    http_hook = HttpHook(method='GET', http_conn_id='weather_api_conn')
    request_params = {
        "appid": Variable.get("WEATHER_API_KEY"),
        "lat": city["lat"],
        "lon": city["lon"],
        "dt": int(execution_date.timestamp()),
        "units": "metric"
    }
    response = http_hook.run(endpoint="data/3.0/onecall/timemachine?", data=request_params)
    return response.json()

with DAG(dag_id="weather_dag",
        start_date=dates.days_ago(7),
        schedule="@daily",
        catchup=True) as dag:

    db_create_table = PythonOperator(
        task_id="create_table",
        python_callable=create_table
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

        extract_data = PythonOperator(
            task_id="extract_data_{}".format(city["city"]),
            python_callable=extract_weather_data,
            op_args=[city],
            provide_context=True
        )

        process_data = PythonOperator(
            task_id="process_data_{}".format(city["city"]),
            python_callable=process_weather,
            op_args=[city])

        insert_data = PythonOperator(
            task_id="insert_data_{}".format(city["city"]),
            python_callable=lambda city=city, ti=None: PostgresHook(postgres_conn_id="pg_conn").run(
                """
                INSERT INTO measures (city, execution_time, temperature, humidity, cloudiness, wind_speed) VALUES 
                (%s, to_timestamp(%s), %s, %s, %s, %s);
                """,
                parameters=(
                    city["city"],
                    ti.xcom_pull(task_ids=f"process_data_{city['city']}")["execution_time"],
                    ti.xcom_pull(task_ids=f"process_data_{city['city']}")["temperature"],
                    ti.xcom_pull(task_ids=f"process_data_{city['city']}")["humidity"],
                    ti.xcom_pull(task_ids=f"process_data_{city['city']}")["cloudiness"],
                    ti.xcom_pull(task_ids=f"process_data_{city['city']}")["wind_speed"]
                )
            ),
            provide_context=True
        )
        
        db_create_table >> check_api >> extract_data >> process_data >> insert_data
