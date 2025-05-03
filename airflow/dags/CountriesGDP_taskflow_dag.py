
import pendulum

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow_custom_utils.countries_data_converter import convertDataFromSource
from airflow_custom_utils.countries_data_ingestion import loadDataToDestination


default_args = {
        'dag_id': 'countries_gdp',
        'owner': 'Jay',
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 0,
        'catchup': False,
        'project_path' : Variable.get("COUNTRIES_DATA_PATH"),
        'postgress_jar_path' : Variable.get("POSTGRESS_JAR_PATH")
}

@dag(
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["countries_gdp_analysis"],
    default_args=default_args
)
def countries_gdp_analysis():
    
    @task.bash
    def extract():
        return "wget -c https://datahub.io/core/gdp/_r/-/data/gdp.csv -O"+default_args['project_path']+"/input/airflow_data-extract.csv"
    
    @task(task_id="stage_data")
    def startStaging():
        filePath = default_args['project_path']+"/input/airflow_data-extract.csv"
        stageFolderPath = default_args['project_path']+"/stagging"
        print("Started to stage data from "+filePath+" to "+stageFolderPath)
        convertDataFromSource(filePath,stageFolderPath)
        print("Staging data completed")  

    @task(task_id="load_data")
    def loadData():
        stageFolderPath = default_args['project_path']+"/stagging"
        print("Started to load data from "+stageFolderPath+" to postgres")
        loadDataToDestination(stageFolderPath,default_args['postgress_jar_path'])
        print("Loading data completed")    
    
    extract_data = extract()
    stageData = startStaging()
    loadData = loadData()

    extract_data >> stageData >> loadData

countries_gdp_analysis()
