import os
from abc import ABC
from mlops.utils.storage.file_storage_functions import FileStorageFunctions
from pyspark.sql.functions import *


class SqlUtils(ABC):

    @classmethod
    def execute_sql_query(cls, spark, sql_config: dict, input_parameters=None, env_vars=None):
        sql_query_path = sql_config.get('sql_query_path')
        sql_query = sql_config.get('sql_query')
        sql_variables = sql_config.get("sql_variables")
        additional_columns = sql_config.get("additional_columns")
        if sql_query_path:
            sql_query = FileStorageFunctions.read_sql(sql_query_path)
        if sql_variables:
            for sql_variable in sql_variables:
                key, value = tuple(list(sql_variable.items())[0])
                if 'env' in value:
                    value = os.environ[str(value).replace('$env_', '').upper()]
                sql_query = str(sql_query).replace(f"${key}", value)

        df = spark.sql(sql_query)
        if additional_columns:
            for outer_column in additional_columns:
                for key, value in outer_column.items():
                    if value == "hash_cols":
                        exclude_col = outer_column.get("exclude_col")
                        source_df_schema_cols = df.schema.names
                        if exclude_col:
                            exclude_hash_cal_columns = str(outer_column["exclude_col"]).split("|")
                            excluded_hash_col_source_col_list = []
                            for column in source_df_schema_cols:
                                if column.lower() not in exclude_hash_cal_columns:
                                    excluded_hash_col_source_col_list.append(column)
                            df = df.withColumn(key, md5(concat_ws("", *excluded_hash_col_source_col_list)))
                            print(excluded_hash_col_source_col_list)
                            break
                        else:
                            print(source_df_schema_cols)
                            df = df.withColumn(key, md5(concat_ws("", *source_df_schema_cols)))
                    else:
                        df = df.withColumn(key, eval(value))
        return df
