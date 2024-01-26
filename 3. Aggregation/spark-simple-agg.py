from pyspark.sql import SparkSession, window
from Aggregation.lib.logger import Log4j
from pyspark.sql import functions as f

from lib.logger import Log4j

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("Spark Agg Demo") \
        .master("local[2]") \
        .getOrCreate()

    logger = Log4j(spark)

    logger.info('Starting Simple-agg Session')

    invoice_df = spark.read \
        .format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load("data/invoices.csv")

    # * Simple 3. Aggregation functions :
    print("--------: Simple 3. Aggregation :--------")
    # 1. count functions : count of records
    print("Simple 3. Aggregation using single agg function Approach : ")
    invoice_df.select(f.count("*").alias('count *')).show()

    # 2. Sum Function : Sum of quantity
    print("Simple 3. Aggregation using multiple agg functions Approach : ")
    invoice_df.select(f.count("*").alias('count *'),
                      f.sum("Quantity").alias("TotalQuantity"),
                      f.avg('UnitPrice').alias('AvgPrice'),
                      f.countDistinct('InvoiceNo').alias('CountDistinct')).show()

    # 3. SQL string expressions
    # count 1 : counts null
    # count(<field>) : Does not count null
    print("Simple 3. Aggregation using SQL string expressions Approach : ")
    invoice_df.selectExpr(
        'count(1) as `count 1`',
        'count(StockCode) as `count Field`',
        'sum(quantity) as TotalQuantity',
        'avg(UnitPrice) as AvgPrice',
    ).show()
    print("--------END of Simple agg--------\n\n")
    logger.info('Finishing Simple-agg Session')
    spark.stop()