import os
import sys
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import regexp_extract, substring_index
from lib.logger import Log4j

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
    data_list = [("Ravi", "28", "1", "2002"),
                 ("Abdul", "23", "5", "81"),  # 1981
                 ("John", "12", "12", "6"),  # 2006
                 ("Rosy", "7", "8", "63"),  # 1963
                 ("Abdul", "23", "5", "81")]  # 1981

    # 1. Creating dataframe with repartition
    # Repartition for realistic behaviors on local machine
    print('1. Creating dataframe with repartition:')
    raw_df = spark.createDataFrame(data_list).toDF('name', 'day', 'month', 'year').repartition(3)
    raw_df.printSchema()
    raw_df.show()

    # 2. Adding Monotonically increasing id as column
    print('2. Adding Monotonically increasing id as column:')
    df1 = raw_df.withColumn('id', monotonically_increasing_id())
    df1.show()

    # 3. Using `Case when then` transformation
    # `Case when then` : Switch case (avoids length if-elif-els)
    print('3. Using `Case when then` transformation:')
    df2 = df1.withColumn('year', expr("""
            case when year < 21 then year + 2000
            when year < 100 then year + 1900
            else year
            end
            """))
    df2.show()

    # 4. Casting with SQL expression your column/fields :
    # 4.1 Line cast
    print('4. Casting with expression your column/fields :')
    print('4.1. Casting with SQL expression your column:')
    df3 = df1.withColumn('year', expr("""
            case when year < 21 then cast(year as int) + 2000
            when year < 100 then cast(year as int) + 1900
            else year
            end
            """))
    df3.show()

    # 4.1.1 Line end Casting
    print('4.1.1. Line end Casting')
    df4 = df1.withColumn('year', expr("""
                    case when year < 21 then year + 2000
                    when year < 100 then year + 1900
                    else year
                    end
                    """).cast(IntegerType()))
    df4.show()

    # 4.2 Change the schema with casting other columns
    print('4.2. Change the schema with casting other columns:')
    df5 = df1.withColumn('day', col('day').cast(IntegerType())) \
        .withColumn('month', col('month').cast(IntegerType())) \
        .withColumn('year', col('year').cast(IntegerType()))

    df6 = df1.withColumn('year', expr("""
                case when year < 21 then year + 2000
                when year < 100 then year + 1900
                else year
                end
                """))
    df6.show()

    # 5. Casting with object expression your column/fields
    print('5. Casting with object expression your column:')
    df7 = df5.withColumn('year', \
                         when(col('year') < 21, col('year') + 2000) \
                         .when(col('year') < 100, col('year') + 1900) \
                         .otherwise(col('year')))
    df7.show()

    # 6. Adding Columns to Dataframes
    print('6. Adding Columns to Dataframes:')
    df8 = df7.withColumn('dob', to_date(expr("concat(day,'/', month,'/',year)"), 'd/M/y'))
    df9 = df7.withColumn('dob', expr("to_date(concat(day,'/', month,'/',year),'d/M/y')"))
    df8.show()
    df9.show()

    # 7. Dropping Columns
    print('7. Dropping Columns:')
    df10 = df9.drop('day', 'month', 'year')
    df10.show()

    # 8. Dropping Duplicate Rows
    print('8. Dropping Duplicate Rows:')
    df11 = df10.dropDuplicates(["name", 'dob'])
    df11.show()

    # 9. Sorting DataFrames
    print('9. Sorting DataFrames:')
    df11.sort(col("dob").desc())
