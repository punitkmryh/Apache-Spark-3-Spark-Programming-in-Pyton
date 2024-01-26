from pyspark.sql import SparkSession
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

    invoice_df = spark.read \
        .format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load("data/invoices.csv")

    logger.info('Starting Grouping-agg Session')

    #* Grouping 3. Aggregation Functions :
    # approach 1
    print("--------: Grouping 3. Aggregation :--------")
    print("Grouping 3. Aggregation using Dataframe expression Approach : ")
    summary_df = invoice_df \
        .groupBy('Country', 'InvoiceNo') \
        .agg(f.sum('Quantity').alias('TotalQuantity'),
             f.round(f.sum(f.expr('Quantity * UnitPrice')), 2).alias('InvoiceValue'))
    summary_df.show()

    # approach 2
    print("Grouping 3. Aggregation using Dataframe expr() Approach : ")
    summary_df1 = invoice_df \
        .groupBy('Country', 'InvoiceNo') \
        .agg(f.sum('Quantity').alias('TotalQuantity'),
             f.expr('round(sum(Quantity*UnitPrice),2) as InvoiceValue'))
    summary_df1.show()

    # approach 1
    print("Grouping 3. Aggregation using SQL Approach : ")
    invoice_df.createOrReplaceTempView('sales')
    summary_SQL_df = spark.sql(
        """
        SELECT Country,InvoiceNo,
                sum(Quantity) as TotalQuantity,
                round(sum(Quantity*UnitPrice),2) as InvoiceValue
        FROM sales
        GROUP BY Country,InvoiceNo
        """
    )
    summary_SQL_df.show()

    # ! Exercise : Grouping Aggregators
    # Problem : Get the table having Country,weekNumber as Agg columns with
    # aggregated columns of NumInvoices, TotalQuantity and invoiceValue, restrict the size by 2010 values

    print("Grouping 3. Aggregation workout : ")

    # Expressions Variables
    NumInvoice = f.countDistinct("InvoiceNo").alias('NumInvoices')
    TotalQuantity = f.sum("Quantity").alias("TotalQuantity")
    InvoiceValue = f.round(f.sum(f.expr('Quantity * UnitPrice')), 2).alias('InvoiceValue')

    exSummary_df = invoice_df \
        .withColumn("InvoiceDate", f.to_date(f.col('InvoiceDate'), 'dd-MM-yyyy H.mm')) \
        .where('year(InvoiceDate) == 2010') \
        .withColumn('WeekNumber', f.weekofyear(f.col("InvoiceDate"))) \
        .groupBy("Country", 'WeekNumber') \
        .agg(NumInvoice, TotalQuantity, InvoiceValue)

    invoice_df = spark.read \
        .format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load("data/invoices.csv")

    exSummary_df.sort("Country",'WeekNumber').show()

    logger.info('Finishing Grouping-agg Session')
    spark.stop()