import re
import os
import sys
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import regexp_extract, substring_index
from lib.logger import Log4j

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable


def parse_gender(gender):
    # ^f$ : start and end with 'f'
    # f.m or w.m : would match strings like "fam", "fbm", "fcm"
    male_pattern = r"^f$|f.m|w.m"
    female_pattern = r"^m$|ma|m.l "

    if re.search(male_pattern, gender.lower()):
        return 'Male'
    elif re.search(female_pattern, gender.lower()):
        return 'Female'
    else:
        return "Unknown"


if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .master("local[3]") \
        .appName("ColumnAccess") \
        .getOrCreate()

    logger = Log4j(spark)

    print("Starting Pyspark Session!")
    survey_df = spark.read \
        .format("csv") \
        .option('header', 'true') \
        .option('inferScehma', 'true') \
        .option('samplingRatio', '0.0001') \
        .load('./data/survey.csv')
    survey_df.show()

    print('Using Column Object Expression -->')

    # Registering the UDF with driver
    parse_gender_udf = udf(parse_gender, StringType())
    survey_df_udf = survey_df.withColumn('Gender', parse_gender_udf('Gender'))

    # filter Transformation
    survey_df_udf.filter("Gender = 'Unknown'").show()
    survey_df_udf.show(10)

    print('Using SQL Expression -->')

    # Registering the SQL functions and store in catalog
    spark.udf.register('parse_gender_sqludf', parse_gender, StringType())

    # Fetching the udf details fro the catalog, output-> check log file
    [logger.info(f) for f in spark.catalog.listFunctions() if 'parse_gender_sqludf' in f.name]

    survey_df_sqludf = survey_df.withColumn('Gender', expr("parse_gender_sqludf(Gender)"))
    survey_df_sqludf.show(10)
