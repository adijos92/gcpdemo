from pyspark.sql import SparkSession
from common.read_data_util import ReadDataUtil
from pyspark.sql.types import *
from pyspark.sql.functions import *

if __name__ == '__main__':
    spark = SparkSession.builder\
        .master("local[*]")\
        .appName("plane_main")\
        .getOrCreate()

    rdu = ReadDataUtil()

    planeschema = StructType([
                             StructField("name", StringType()),
                             StructField("iata code", StringType()),
                             StructField("icao code", StringType()),
                            ])

    planedf = rdu.readCsv(
        spark= spark,path =r"E:\Pyspark Practice\File\Spark 1st project data\plane.csv.gz",
        schema= planeschema)

    planedf.show()
    planedf.printSchema()





