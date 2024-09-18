from mlops.core.base_config import BaseConfig
from mlops.data.reader.pandas_reader import PandasReader
from mlops.data.reader.spark_reader import SparkReader


class InputDataFrameManager:
    """
    This class manages the creation of DataFrames from various data sources based on configuration.

    It supports creating Pandas DataFrames and Spark DataFrames depending on the specified type in the configuration.
    """

    def __init__(self, spark, config):
        """
        Initializes the InputDataFrameManager with SparkSession and configuration.

        Args:
            spark (SparkSession): A SparkSession object (used for Spark DataFrames).
            config (BaseConfig): A BaseConfig object containing configuration for data sources.
        """
        self.config = config  # Store the configuration object
        self.dataframes_list = []  # List to store the created DataFrames
        self.spark = spark  # Store the SparkSession object
        self.dataframe_dict = None

    def create_dataframes(self):
        """
        Creates DataFrames from the data sources specified in the configuration.

        1. Extracts the list of data sources from the configuration using Jinja2 template syntax.
        2. Loops through each data source configuration (a dictionary).
        3. Creates a dictionary to store the source name and the created DataFrame.
        4. Uses the `dataframe_type` from the configuration to determine the reader type:
           - 'pandas': Creates a PandasReader, reads data, and stores the resulting DataFrame.
           - 'spark': Creates a SparkReader with SparkSession and configuration, reads data, and stores the DataFrame.
        5. Appends the dictionary containing the source name and DataFrame to the `dataframes_list`.
        6. Returns `self` to allow method chaining.
        """

        data_sources = self.config.get("data_sources")  # Extract data sources
        # using Jinja2 syntax
        dataframe_dict = {}  # Create a dictionary to store source name and DataFrame
        for data_source in data_sources:
            print(data_source)
            data_source = dict(data_source)  # Convert data source to a regular dictionary

            dataframe_type = data_source.get('dataframe_type')
            source_name = data_source.get('source_name')
            if dataframe_type == 'pandas':
                dataframe_dict[source_name] = PandasReader(data_source).read_data().get_dataframe()
            elif dataframe_type == 'spark':
                dataframe_dict[source_name] = SparkReader(self.spark, data_source).read_data().get_dataframe()
            else:
                raise ValueError(f"Unsupported dataframe_type: {dataframe_type}")  # Handle unsupported types

        self.dataframe_dict = dataframe_dict
        return self

    def get_dataframes(self):
        """
        Returns the list of created DataFrames.
        """
        return self.dataframe_dict
