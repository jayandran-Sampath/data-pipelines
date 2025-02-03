
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
        'company_path' : Variable.get("SP_COMPANY_ANALYSIS_PATH")
}

@dag(
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["sp_company_analysis"],
    default_args=default_args
)
def sp_company_analysis():
    
    @task.bash
    def extract():
        return "wget -c https://datahub.io/core/s-and-p-500-companies/r/constituents.csv -O {{var.value.SP_COMPANY_ANALYSIS_PATH}}/input/airflow_data-extract.csv"
    
    

    def transformData() :
        df = pd.read_csv(default_args['company_path']+"/input/airflow_data-extract.csv")
        sector_df = df.groupby(['GICS Sector']).size().to_frame('size')
        sector_df["date"] = pendulum.now().strftime("%Y-%m-%d")
        sector_df.to_csv(default_args['company_path']+"/stagging/airflow_data-extract.csv")

    def generate_insert_queries():
        # Read the CSV file
        CSV_FILE_PATH = default_args['company_path']+'/stagging/airflow_data-extract.csv'
        df = pd.read_csv(CSV_FILE_PATH)
        # Create a list of SQL insert queries
        insert_queries = []
        for index, row in df.iterrows():
            insert_query = f"INSERT INTO sectors (name, size, dateOfEntry) VALUES ('{row['GICS Sector']}', {row['size']}, '{row['date']}');"
            insert_queries.append(insert_query)
        
        # Save queries to a file for the PostgresOperator to execute
        with open(default_args['company_path']+'/sql/insert_queries.sql', 'w') as f:
            for query in insert_queries:
                f.write(f"{query}\n")
    
    @task()
    def transform():
        transformData()
        generate_insert_queries()
    

    load_data = SQLExecuteQueryOperator(
    task_id="load_data_db",
    conn_id="postgres_local",
    sql="sp_company_analysis/sql/insert_queries.sql")
    
    extract_data = extract()
    transform_data = transform()

    extract_data >> transform_data >> load_data

sp_company_analysis()
