from pyspark.sql import SparkSession
#from common.read_data_util import ReadDataUtil
from pyspark.sql.types import *
from pyspark.sql.functions import *

if __name__ == '__main__':
    spark = SparkSession.builder\
        .master("local[*]")\
        .appName("routes_snappy_main")\
        .getOrCreate()

   








