# Airflow Weather DAG

## Usage notes

## Setting up

### Fetching docker-compose.yaml
```
curl -LfO 'https://airflow.apache.org/docs/apache-airflow/2.5.2/docker-compose.yaml'
```

### Initializing Environment
```
mkdir -p ./dags ./logs ./plugins
echo -e "AIRFLOW_UID=$(id -u)" > .env
```

### Initialize the database
```
docker compose up airflow-init
```

### Running Airflow
```
docker compose --profile flower up -d
docker compose down
```

### Setting up the variables
```
docker exec -it airflow-airflow-scheduler-1 airflow variables set WEATHER_API_KEY "???"
```

### Copy the DAG to the dags folder
```
cp dags/weather_dag.py ~/airflow/dags
```

## Executing the DAG
```
docker exec -it airflow-airflow-scheduler-1 airflow dags trigger weather_dag
```

## Clean-Up
```
docker compose down --rmi all -v --remove-orphans
```
