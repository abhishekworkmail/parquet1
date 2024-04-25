import unittest
from pyspark.sql import SparkSession
import os

# Import the function to be tested
from parquet1 import parquet_file


class TestParquetFile(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Create a SparkSession for all test cases
        cls.spark = SparkSession.builder \
            .appName("Test Parquet File") \
            .master("local[2]") \
            .getOrCreate()

    def setUp(self):
        # Create a temporary directory for test output
        self.temp_dir = "temp_test_output"
        os.makedirs(self.temp_dir, exist_ok=True)

    def test_parquet_file(self):
        # Call the function
        parquet_file()

    def tearDown(self):
        # Remove the temporary directory after each test case
        if os.path.exists(self.temp_dir):
            os.rmdir(self.temp_dir)

    @classmethod
    def tearDownClass(cls):
        # Stop SparkSession after all test cases
        cls.spark.stop()


if __name__ == '__main__':
    unittest.main()
