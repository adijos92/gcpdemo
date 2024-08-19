from pyspark.sql import SparkSession
from common.read_data_util import ReadDataUtil
from pyspark.sql.types import *
from pyspark.sql.functions import *

if __name__ == '__main__':
    spark = SparkSession.builder\
        .master("local[*]")\
        .appName("routes_snappy_main")\
        .getOrCreate()

    # rdu = ReadDataUtil()
    #
    # routesnappyschema = StructType([StructField("airline_id", IntegerType()),
    #                          StructField("name", StringType()),
    #                          StructField("alias",StringType()),
    #                          StructField("iata", StringType()),
    #                          StructField("icao", StringType()),
    #                          StructField("callsign", StringType()),
    #                          StructField("country",StringType()),
    #                          StructField("active",StringType())])
    #
    # # df = rdu.readCsv(
    #     spark= spark,path =r"E:\Pyspark Practice\File\Spark 1st project data\airline.csv",
    #     schema=airlineschema)
    #
    # df.show()
    # df.printSchema()

    parquetdf = spark.read.parquet(r"E:\Pyspark Practice\File\Spark 1st project data\routes.snappy.parquet")
    parquetdf.printSchema()
    parquetdf.show()






