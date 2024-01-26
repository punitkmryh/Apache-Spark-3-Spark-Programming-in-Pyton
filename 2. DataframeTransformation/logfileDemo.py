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

    log_reg = r'^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+) (\S+)" (\d{3}) (\S+) "(\S+)" "([^"]*)'
    add_log_reg = r'\(([^;]+); ([^)]+)\) ([^ ]+[^)]+)'

    log_df = file_df.select(regexp_extract('value', log_reg, 1).alias('ip'),
                            regexp_extract('value', log_reg, 4).alias('date'),
                            regexp_extract('value', log_reg, 6).alias('request'),
                            regexp_extract('value', log_reg, 10).alias('referrer'))
    log_df.printSchema()
    log_df.groupBy('referrer') \
        .count() \
        .show(100, truncate=False)

    # Fixing same referrer website names:
    log_df \
        .withColumn("referrer", substring_index('referrer', '/', 3)) \
        .groupBy('referrer') \
        .count() \
        .show(100, truncate=False)

    # Fetching specific website name '-':
    log_df \
        .where("trim(referrer) != '-'") \
        .withColumn("referrer", substring_index('referrer', '/', 3)) \
        .groupBy('referrer') \
        .count() \
        .show(100, truncate=False)