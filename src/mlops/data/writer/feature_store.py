from mlops.core.base_data_writer import DataWriter
from mlops.data.writer.spark_writer import SparkWriter
from mlops.utils.common import convert_pandas_df_to_spark_df

from databricks.feature_store import FeatureStoreClient


class FeatureStoreWriter(DataWriter):
    def __init__(self, spark, output_df, config):
        """
        Initializes the SparkReader instance with SparkSession and configuration.

        Args:
            spark (SparkSession): A SparkSession object.
            config (dict): A dictionary containing configuration for reading data.
        """
        self.df = None  # Initialize an empty DataFrame to hold the loaded data
        self.spark = spark  # Store the SparkSession object
        self.output_df = output_df
        self.write_config: dict = config  # Store the configuration dictionary
        self.fs = FeatureStoreClient()

    def write_data(self):
        dataframe_type = self.write_config.get("dataframe_type")
        table_name = self.write_config.get("table_name")
        online_config = self.write_config.get('online_feature_store', {})

        feature_store_table_mode = self.write_config.get("feature_store_table_mode")

        spark_df = convert_pandas_df_to_spark_df(self.spark,
                                                 self.output_df) if dataframe_type == "pandas" else self.output_df
        SparkWriter(self.spark, spark_df, self.write_config).write_data()
        self.fs.write_table(name=table_name, df=spark_df, mode=feature_store_table_mode)
        # Online feature store configuration

        if online_config:
            online_table_name = online_config.get('table_name')
            self.fs.publish_table(
                name=table_name,
                online_store=online_table_name
            )

    def validate_output_path(self, path: str):
        pass

    def validate_datasource_format(self, source_format: str):
        pass
