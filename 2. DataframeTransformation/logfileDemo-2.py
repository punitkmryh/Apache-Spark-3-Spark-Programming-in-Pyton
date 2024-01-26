from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import regexp_extract, substring_index

from lib.logger import Log4j
import os
import sys
from pyspark.sql import SparkSession

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .master("local[3]") \
        .appName("RowDemo") \
        .getOrCreate()

    logger = Log4j(spark)

    print("Starting Pyspark Session!")
    file_df = spark.read.text('data/apache_logs.txt')
    file_df.printSchema()
    file_df.show()

    add_log_reg = r'\(([^;]+); ([^)]+)\) ([^ ]+[^)]+)'

    add_log_df = file_df.select(regexp_extract('value', add_log_reg, 1).alias('Machine_name'),
                                regexp_extract('value', add_log_reg, 2).alias("Operating_system"),
                                regexp_extract('value', add_log_reg, 3).alias('Browser'))

    add_log_df.printSchema()
    add_log_df.groupBy('Machine_name') \
        .count() \
        .show(100, truncate=False)
