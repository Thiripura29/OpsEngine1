from mlops.core.base_data_reader import DataReader
import pandas as pd


class PandasReader(DataReader):
    """
    This class reads data from various file formats using pandas library and converts it to a Spark DataFrame.

    It inherits from the base DataReader class.
    """

    def __init__(self, config):
        """
        Initializes the PandasReader instance with SparkSession and configuration.

        Args:
            spark (SparkSession): A SparkSession object (not currently used in this implementation).
            config (dict): A dictionary containing configuration for reading data.
        """
        self.df = None  # Initialize an empty DataFrame to hold the loaded data
        self.config: dict = config  # Store the configuration dictionary

    def read_data(self):
        """
        Reads data from the specified path based on the configuration.

        Extracts input path, additional parameters, and file format from the configuration.
        Delegates the reading to the `generic_read` function based on file format and parameters.
        """
        input_path = self.config.get('input_path')
        additional_parameters = self.config.get('dataframe_parameters')
        file_format = self.config.get('file_format')
        self.df = self.generic_read(input_path, file_format, **additional_parameters)
        return self

    def get_dataframe(self):
        """
        Returns the loaded Spark DataFrame (currently holds the pandas DataFrame).
        """
        return self.df

    def validate_input_path(self, path: str):
        """
        Placeholder method for input path validation (not implemented yet).
        """
        pass

    def validate_datasource_format(self, source_format: str):
        """
        Placeholder method for datasource format validation (not implemented yet).
        """
        pass

    def generic_read(self, input_path: str, file_format: str, **kwargs) -> pd.DataFrame:
        """
        Generic function to read data into a pandas DataFrame based on the file format.

        This function maps file formats to their corresponding pandas read functions.
        It reads data using the appropriate pandas read function and additional parameters.

        Parameters:
        input_path (str): The path to the data file.
        file_format (str): The format of the data file (e.g., 'csv', 'excel', 'json').
        **kwargs: Additional parameters to pass to the pandas read function.

        Returns:
        pd.DataFrame: The DataFrame created from the data file.
        """
        read_function_map = {
            "csv": pd.read_csv,
            "excel": pd.read_excel,
            "json": pd.read_json,
            "parquet": pd.read_parquet,
            "html": pd.read_html,
            "sql": pd.read_sql,
            "table": pd.read_table,
            "stata": pd.read_stata,
            "sas": pd.read_sas,
            "clipboard": pd.read_clipboard,
            "pickle": pd.read_pickle
        }

        read_function = read_function_map.get(file_format)
        if not read_function:
            raise ValueError(f"Unsupported file format: {file_format}")

        return read_function(input_path, **kwargs)
