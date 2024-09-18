from mlops.data.writer.feature_store import FeatureStoreWriter
from mlops.data.writer.spark_writer import SparkWriter


class OutputDataFrameManager:
    def __init__(self, spark, output_df_dict, config):
        self.config = config  # Store the configuration object
        self.output_df_dict = output_df_dict  # List to store the created DataFrames
        self.spark = spark  # Store the SparkSession object

    def write_data_to_sinks(self):
        data_sinks = self.config.get("data_sinks")  # Extract data sinks
        for data_sink in data_sinks:
            input_id = data_sink.get('input_id')
            sink_name = input_id if input_id else data_sink['sink_name']
            write_df = self.output_df_dict.get(sink_name)
            sink_type = data_sink['type']
            if write_df:
                if sink_type == 'spark_sink':
                    writer = SparkWriter(self.spark, write_df, data_sink)
                    writer.write_data()
                elif sink_type == 'pandas_sink':
                    pass
                elif sink_type == 'feature_store':
                    writer = FeatureStoreWriter(self.spark, write_df, data_sink)
                    writer.write_data()

