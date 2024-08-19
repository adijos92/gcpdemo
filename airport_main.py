from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

if __name__ == '__main__':
    spark = SparkSession.builder.master("local[*]").appName("airport_main").getOrCreate()



    airportschema = StructType([StructField("airport_id",IntegerType()),
                                StructField("name",StructType()),
                                StructField("city",StringType()),
                                StructField("country", StringType()),
                                StructField("iata", StringType()),
                                StructField("icao", StringType()),
                                StructField("latitude", StringType()),
                                StructField("longitude", StringType()),
                                StructField("altitude", StringType()),
                                StructField("timezone", StringType()),
                                StructField("dst", StringType()),
                                StructField("type", StringType()),
                                StructField("source", StringType())])

    df =spark.read.csv(r"E:\airline project\airport.csv",inferSchema=True,header=True)

    df.show(100)
    df.printSchema()







