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
        .appName("ColumnAccess") \
        .getOrCreate()

    logger = Log4j(spark)

    print("Starting Pyspark Session!")
    invoice_df = spark.read \
        .format("csv") \
        .option('header', 'true') \
        .option('inferScehma', 'true') \
        .option('samplingRatio', '0.0001') \
        .load('./data/invoices.csv')

    invoice_df.show(10)

    print('Column Access Transformations:')
    print("Column Access using Column String -->")
    invoice_df.select("Quantity", "Country", "InvoiceDate").show(10)

    print("Column Access using Column Object -->")
    invoice_df.select(column("Quantity"), col("Country"), invoice_df.UnitPrice).show(10)

    print('Column Expressions Transformations:')
    print("String/SQL  Expression -->")
    invoice_df.printSchema()
    # Error : select() takes column string or column objects
    # airline_df.select("Quantity", "Country", "to_date(InvoiceDate),'yyyyMMdd) as flightDate" )).show(10)
    invoice_df.select("Quantity", "Country", expr("to_date(InvoiceDate,'dd-MM-yyyy H.mm') as ColExprInvoiceDate")).show(
        10)

    print("Column Object Expression -->")
    invoice_df.select("Quantity", "Country",
                      to_date(col("InvoiceDate"), 'dd-MM-yyyy H.mm').alias("ColObjInvoiceDate")).show(10)

    print('Workouts:')
    invoice_df.filter("Country =='Australia'").show(10)
