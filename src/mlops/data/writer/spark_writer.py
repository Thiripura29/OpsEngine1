from mlops.core.base_data_writer import DataWriter
from pyspark.sql.types import StructType


class SparkWriter(DataWriter):

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
        self.return_config = {}

    def write_data(self):

        print(self.write_config)
        # Fetch configuration values with default fallbacks
        options = self.write_config.get('dataframe_options', {})
        schema = self.write_config.get('schema')
        output_path = self.write_config.get('path')
        format = self.write_config.get('format', 'parquet')
        partitions = self.write_config.get('partitionBy')
        overwrite_schema = self.write_config.get('overwriteSchema', False)
        merge_schema = self.write_config.get('mergeSchema', False)
        write_mode = self.write_config.get('mode', 'append')
        table_name = self.write_config.get('table_name')

        # Validate that at least one of 'table_name' or 'output_path' is provided
        if not table_name and not output_path:
            raise ValueError("Configuration must include either 'table_name' or 'path'.")

        # Apply schema if provided
        if schema:
            schema_struct = StructType.fromDDL(schema)
            print(f"Schema Struct: {schema_struct}")
            print(f"Original DataFrame Schema: {self.output_df.schema}")
            # Check for discrepancies between the schema and DataFrame columns
            for field in schema_struct.fields:
                if field.name not in self.output_df.columns:
                    print(f"Column {field.name} not found in DataFrame")
            self.output_df = self.output_df.selectExpr(
                *[f"`{field.name}` as `{field.name}`" for field in schema_struct.fields])

        # Initialize the DataFrameWriter with the provided options and mode
        writer = self.output_df.write.format(format).options(**options).mode(write_mode)

        # Apply partitioning if specified
        if partitions:
            writer = writer.partitionBy(*partitions)
            partition_predicate = self.identify_partitions_predicate(self.output_df, partitions)
            self.return_config["partition_predicate"] = partition_predicate

        # Apply schema overwrite option if in overwrite mode
        if write_mode == 'overwrite' and overwrite_schema:
            writer = writer.option('overwriteSchema', 'true')

        # Apply schema merge option if specified
        if merge_schema:
            writer = writer.option('mergeSchema', 'true')

        # Create database if it does not exist
        if table_name:
            database_name = table_name.split('.')[0]
            self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {database_name}")

        # Save the DataFrame based on the provided table name and/or path
        if table_name and output_path:
            writer.saveAsTable(table_name, path=output_path)
        elif table_name:
            writer.saveAsTable(table_name)
        else:
            writer.save(output_path)

    def identify_partitions_predicate(self, df, partition_columns):
        conditions = []

        for row in df.select(*partition_columns).distinct().collect():
            condition_parts = [f"{col_name} = '{row[col_name]}'" for col_name in partition_columns]
            condition = " AND ".join(condition_parts)
            conditions.append(f"({condition})")

        # Combine conditions with OR
        partition_predicate = " OR ".join(conditions)

        return partition_predicate

    def validate_output_path(self, path: str):
        pass

    def validate_datasource_format(self, source_format: str):
        pass
