from pyspark.sql import SparkSession, Window
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

    #! Problem 1 : Get Running Total for each country and restart for new country for the below given
    # Problem 1.1 : Get the table having Country,weekNumber as Agg columns with
    # aggregated columns of NumInvoices, TotalQuantity and invoiceValue, restrict the size by 2010 values

    #? Solution:  Using window aggregation concept
    # Step 1: Partitioning/Break the dataframe based on the `Country`
    # Step 2: Order each partition by WeekNumber -> to get week by week total
    # Step 3: Calculate Total by Sliding window of records by start(first record) and end(current record) of window

    logger.info('Starting Window-agg Session')

    summary_df= spark.read.parquet('/Users/punitkumarharsur/Downloads/DE/DE-Udemy/Apache-Spark-Begineer-Prashant-Pandey/spark/3. Aggregation/data/summary.parquet')

    #! Creating the running-total window
    # unboundedPreceding : starting window that takes all the rows from the beginning
    # unboundedPreceding : -2 -> window start from 2 rows before current row
    running_total_window = Window.partitionBy('Country') \
        .orderBy('WeekNumber') \
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)

    #! Adding running total over the Summary_df
    running_total = summary_df.withColumn('RunningTotal',
                          f.sum("InvoiceValue").over(running_total_window))
    running_total.show()
    logger.info('Finishing Window-agg Session')
    spark.stop()
