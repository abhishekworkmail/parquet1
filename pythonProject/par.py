from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


def parquet_file():
    # Create a SparkSession
    spark = SparkSession.builder \
        .appName("Create and Print Parquet File") \
        .getOrCreate()

    # Sample data
    data = [("John", 25), ("Alice", 30), ("Bob", 35)]

    # Define schema
    schema = StructType([
        StructField("Name", StringType(), True),
        StructField("Age", IntegerType(), True)
    ])

    # Create a DataFrame
    df = spark.createDataFrame(data, schema=schema)

    # Write DataFrame to Parquet file with the specified name
    # df.write.parquet("output.parquet2")

    # Read Parquet file into DataFrame
    df_parquet = spark.read.parquet("output.parquet2")

    # Show the contents of the DataFrame
    df_parquet.show()

    # Stop SparkSession
    spark.stop()


# Call the function to execute the code
parquet_file()
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


def parquet_file():
    # Create a SparkSession
    spark = SparkSession.builder \
        .appName("Create and Print Parquet File") \
        .getOrCreate()

    # Sample data
    data = [("John", 25), ("Alice", 30), ("Bob", 35)]

    # Define schema
    schema = StructType([
        StructField("Name", StringType(), True),
        StructField("Age", IntegerType(), True)
    ])

    # Create a DataFrame
    df = spark.createDataFrame(data, schema=schema)

    # Write DataFrame to Parquet file with the specified name
    # df.write.parquet("output.parquet2")

    # Read Parquet file into DataFrame
    df_parquet = spark.read.parquet("output.parquet2")

    # Show the contents of the DataFrame
    df_parquet.show()

    # Stop SparkSession
    spark.stop()


# Call the function to execute the code
parquet_file()
