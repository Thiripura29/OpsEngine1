from mlops.core.base_data_reader import DataReader
from mlops.utils.sql_utils import SqlUtils
from mlops.utils.storage.file_storage_functions import FileStorageFunctions

from pyspark.sql.types import *


class SparkReader(DataReader):

    def __init__(self, spark, config):
        """
        Initializes the SparkReader instance with SparkSession and configuration.

        Args:
            spark (SparkSession): A SparkSession object.
            config (dict): A dictionary containing configuration for reading data.
        """
        self.df = None  # Initialize an empty DataFrame to hold the loaded data
        self.spark = spark  # Store the SparkSession object
        self.config: dict = config  # Store the configuration dictionary

    def read_data(self):
        """
        Reads data from the specified path based on the configuration.

        Extracts options, schema, input path, and file format from the configuration.
        Builds Spark DataFrame using format, options, and schema (if provided).
        Loads data from the input path using the built Spark DataFrame.
        Optionally standardizes schema for XML data (converting to lowercase and replacing underscores).
        """
        options = self.config.get('dataframe_options')  # Get options dictionary from config
        schema = self.config.get('schema')  # Get schema definition from config (if provided)
        input_path = self.config.get('path')  # Get input path from config
        file_format = self.config.get('format')  # Get file format from config
        sql_config = self.config.get('sql_config')  # Get sql_query from config
        tmp_view_name = self.config.get('tmp_view_name') # Get tem_view_name from config

        if sql_config:
            self.df = SqlUtils.execute_sql_query(self.spark, sql_config)
        else:
            # Build Spark DataFrame format object based on file format
            spark_format = self.spark.read.format(file_format)

            # Build format object with options and schema (if provided) in a concise way
            format_with_options_schema = (
                spark_format.schema(StructType.fromJson(schema)).options(
                    **options) if options and schema else  # Both options and schema
                spark_format.options(**options) if options else  # Only options
                spark_format.schema(StructType.fromJson(schema)) if schema else  # Only schema
                spark_format  # No options or schema
            )

            # Load data from the path using the built format object
            self.df = format_with_options_schema.load(input_path)

            # Apply schema standardization for XML data (optional)
            self.df = self.standardized_schema(self.df) if file_format == "xml" else self.df
            if tmp_view_name:
                self.df.createOrReplaceTempView(tmp_view_name)
        return self

    def get_query_from_file(self, file_path: str) -> str:
        """
            Get SQL query from the file
        """
        return FileStorageFunctions.read_sql(file_path)

    def get_query_as_df(self, query):
        """
            Get Dataframe from a query
        """
        return self.spark.sql(query)

    def get_dataframe(self):
        """
        Returns the loaded DataFrame.
        """
        return self.df

        # Method to validate the input path (left empty for now)

    def validate_input_path(self, path):
        pass

        # Method to validate the datasource format (left empty for now)

    def validate_datasource_format(self, source_format):
        pass

    def standardized_schema(self, df):
        """
        Standardizes the schema for XML data (lowercase and replace underscores).

        Creates a new list with column names converted to lowercase and underscores replaced with empty strings.
        Returns a new DataFrame with the transformed column names.
        """
        new_df_columns = [column.lower().replace("_", '') for column in df.columns]
        return df.toDF(*new_df_columns)
