import os


def set_env_variables(env_list):
    for env in env_list:
        # Set an environment variable
        key, value = list(env.items())[0]
        os.environ[key] = value


def convert_pandas_df_to_spark_df(spark, df):
    return spark.createDataFrame(df)
