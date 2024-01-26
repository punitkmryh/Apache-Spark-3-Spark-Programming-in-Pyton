import sys
from pyspark.sql import *
from lib.logger import Log4j
from lib.utils import *

if __name__ == "__main__":
    # Setting Configurations in Application Code
    # conf = SparkConf()
    # spark application name config string: spark.app.name
    # conf.set('spark.app.name', 'Hello Spark')
    # Spark master name config string: spark.master
    # conf.set('spark.master', 'local[3')

    conf = get_spark_app_config()
    # Building the spark session with appName
    spark = SparkSession.builder \
        .config(conf=conf) \
        .getOrCreate()

    logger = Log4j(spark)

    # if len(sys.argv) != 2:
    #     logger.error('Usage: HelloSpark <filename>')
    #     sys.exit(-1)

    logger.info('Starting HelloSpark')
    # Coding here

    # conf_out = spark.sparkContext.getConf()
    # logger.info(conf_out.toDebugString())

    input_file_path = './data/sample.csv'
    survey_raw_df = load_survey_df(spark, input_file_path)
    partitioned_survey_df = survey_raw_df.repartition(2)
    count_df = count_by_country(partitioned_survey_df)
    count_df.show()

    logger.info('Finishing HelloSpark')
    spark.stop()
