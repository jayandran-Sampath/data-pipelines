**Data Pipelines**

Project contains of pipelines created using different tools 

eg **Airflow** -> Pipelines created using airflow tool are saved in airflow folder



**Pre-requisite :** 

Start postgres db using docker compose command

```
docker-compose -f ./docker-compose-postgress.yml up
```

This will spin up docker container of postgres with required tables


**Airflow :**

Configure your airflow dags path using repository dags folder

``` export AIRFLOW__CORE__DAGS_FOLDER=$PWD/airflow/dags ```

restart your airflow instance

Create two variables in airflow which will point to your local folder path

```

SP_COMPANY_ANALYSIS_PATH  

DOMAIN_ANALYSIS_PATH

```


