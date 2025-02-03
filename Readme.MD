Data Pipelines

Contains of pipelines created using different tools 

like Airflow -> Pipelines created using airflow tool



Pre requsite : 

Start postgres db using docker compose command

docker-compose -f ./docker-compose-postgress.yml up

This will spin up docker container of postgres with required tables


Airflow :

Configure your airflow dags path using repository dags folder

export AIRFLOW__CORE__DAGS_FOLDER=$PWD/airflow/dags

restart your airflow instance



