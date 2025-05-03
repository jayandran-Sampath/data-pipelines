from pyspark.sql import SparkSession
import findspark
from airflow.models import Variable
import traceback

def loadDataToDestination(filePath, jarPath):
  findspark.init()
  spark = SparkSession\
            .builder\
            .appName("SparkLoadingJob")\
            .config("spark.sql.shuffle.partitions", 2)\
            .config("spark.default.parallelism", 2)\
            .config("spark.jars",jarPath+"/postgresql-42.7.5.jar")\
            .master("local[2]")\
            .getOrCreate()
  print(spark.version)
  print()
  print("DB URL : "+Variable.get("JDBC_POSTGRES_URL"))
  try : 
    transformed_data = spark\
                      .read\
                      .parquet(filePath)
    transformed_data.show(5)
    transformed_data.write.format("jdbc") \
      .option("url", Variable.get("JDBC_POSTGRES_URL")) \
      .option("driver", "org.postgresql.Driver") \
      .option("dbtable", "public.countries_gdp") \
      .option("user", Variable.get("DB_USERNAME")) \
      .option("password", Variable.get("DB_PASSWORD")) \
      .mode("overwrite") \
      .save()
  except Exception as error:
    traceback.print_exc()
    print("exception occured")
  print("Process completed")
  spark.stop()