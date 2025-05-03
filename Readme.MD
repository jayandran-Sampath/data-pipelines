**Data Pipelines**

Project contains of pipelines created using different tools 

eg **Airflow** -> Pipelines created using airflow tool are saved in airflow folder



**Pre-requisite :** 


1. Start postgres db using docker compose command

```
docker-compose -f ./docker-compose-postgress.yml up
```

This will spin up docker container of postgres with required tables

2. Build Utils Package 

```
cd airflow/utils
pip install .
```

This will be build util methods used within pipeline


**Airflow :**

Configure your airflow dags path using repository dags folder

``` export AIRFLOW__CORE__DAGS_FOLDER=$PWD/airflow/dags ```

restart your airflow instance

Create two variables in airflow which will point to your local folder path

```

SP_COMPANY_ANALYSIS_PATH  

DOMAIN_ANALYSIS_PATH

COUNTRIES_DATA_PATH -> path where you wanted countries gdp temp data to be stored
 
JDBC_POSTGRES_URL -> local postgress jdbc url (ie : jdbc:postgresql://localhost:5432/postgres)

DB_USERNAME -> postgress db username

DB_PASSWORD -> postgress db password

POSTGRESS_JAR_PATH -> download postgress jdbc jar & provide the path in the variable 


```

Build local utility project


```

cd utils
pip install .

```



