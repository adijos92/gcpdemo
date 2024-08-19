from pyspark.sql import SparkSession
from common.read_data_util import ReadDataUtil
from pyspark.sql.types import *
from pyspark.sql.functions import *

if __name__ == '__main__':
    spark = SparkSession.builder\
        .master("local[*]")\
        .appName("airport_main")\
        .getOrCreate()

    rdu = ReadDataUtil()

    airportschema = StructType([StructField("airport_id", IntegerType()),
                             StructField("name", StringType()),
                             StructField("city",StringType()),
                             StructField("country", StringType()),
                             StructField("iata", StringType()),
                             StructField("icao", StringType()),
                             StructField("latitude",FloatType()),
                             StructField("longitude",FloatType()),
                             StructField("altitude",StringType()),
                             StructField("timezone",StringType()),
                             StructField("dst",StringType()),
                             # StructField("tz_database_timezone",StringType()),
                             StructField("type",StringType()),
                             StructField("source",StringType())])

    df = rdu.readCsv(
        spark= spark,path =r"E:\Pyspark Practice\File\Spark 1st project data\airport.csv",
        schema= airportschema)

    df.show()
    df.printSchema()






