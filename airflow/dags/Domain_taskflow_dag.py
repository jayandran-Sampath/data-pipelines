
import pendulum
import pandas as pd

from airflow.decorators import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.models import Variable


default_args = {
        'owner': 'Jay',
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 0,
        'catchup': False,
        'domain_path' : Variable.get("DOMAIN_ANALYSIS_PATH")
}

@dag(
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["generic_domain_analysis"],
    default_args=default_args
)
def generic_domain_data():
    
    @task.bash
    def extract():
        return "wget -c https://datahub.io/core/top-level-domain-names/r/top-level-domain-names.csv.csv -O {{var.value.DOMAIN_ANALYSIS_PATH}}/input/airflow_data-extract.csv"

    def transformData() :
        df = pd.read_csv(default_args['domain_path']+"/input/airflow_data-extract.csv")
        generic_df = df[df["Type"] == "generic"]
        generic_df['Sponsoring Organization'] = generic_df['Sponsoring Organization'].str.replace('\W', '', regex=True)
        generic_df["date"] = pendulum.now().strftime("%Y-%m-%d")
        generic_df.to_csv(default_args['domain_path']+"/stagging/airflow_data-extract.csv")

    def generate_insert_queries():
        # Read the CSV file
        CSV_FILE_PATH = default_args['domain_path']+'/stagging/airflow_data-extract.csv'
        df = pd.read_csv(CSV_FILE_PATH)
        # Create a list of SQL insert queries
        insert_queries = []
        for index, row in df.iterrows():
            insert_query = f"INSERT INTO domains (name, type, organisation, dateOfEntry) VALUES ('{row['Domain']}', '{row['Type']}', '{row['Sponsoring Organization']}', '{row['date']}');"
            insert_queries.append(insert_query)
        
        # Save queries to a file for the PostgresOperator to execute
        with open(default_args['domain_path']+'/sql/insert_queries.sql', 'w') as f:
            for query in insert_queries:
                f.write(f"{query}\n")
    
    @task()
    def transform():
        transformData()
        generate_insert_queries()
    

    load_data = SQLExecuteQueryOperator(task_id="load_data_db",conn_id="postgres_local",sql="domain_analysis/sql/insert_queries.sql")
    
    extract_data = extract()
    transform_data = transform()

    extract_data >> transform_data >> load_data

generic_domain_data()
