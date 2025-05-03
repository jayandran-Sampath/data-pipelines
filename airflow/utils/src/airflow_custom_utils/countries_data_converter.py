from pyspark.sql import SparkSession
import findspark

def convertDataFromSource(filePath,stageFolderPath):
  findspark.init()
  spark = SparkSession\
            .builder\
            .appName("SparkWriterJob")\
            .config("spark.sql.shuffle.partitions", 2)\
            .config("spark.default.parallelism", 2)\
            .master("local[2]")\
            .getOrCreate()
  raw_data = spark\
                .read\
                .option("inferSchema", "true")\
                .option("header", "true")\
                .csv(filePath)
  print(spark.version)
  raw_data = raw_data.drop('Country Code')
  raw_data.createOrReplaceTempView("TempView")
  raw_data = spark.sql("""
    SELECT
        `t1`.`Country Name` As countryName,
        t1.Year,
        t1.Value As gdp,
        ((t1.Value - t2.Value)/t2.Value) * 100 As growthRate
    FROM TempView t1
    LEFT JOIN TempView t2 
    ON `t1`.`Country Name` = `t2`.`Country Name`
    AND (t1.Year-1) = t2.Year
""")
  recent_data = raw_data.where(raw_data.Year >= 2015)
  recent_data.write \
      .option("compression", "gzip") \
      .partitionBy("Year") \
      .parquet(path=stageFolderPath,
               mode="overwrite")
  print("Process completed")
  spark.stop()