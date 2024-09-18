from typing import Optional

from pyspark import SparkConf
from pyspark.sql import SparkSession


class PysparkSessionManager:
    SESSION: SparkSession

    @classmethod
    def start_session(cls,
                      app_name: Optional[str] = None,
                      config: Optional[dict] = None,
                      enable_hive_support: bool = False,
                      platform: str = 'local'
                      ) -> SparkSession:

        if platform == 'local':
            spark_session = cls._start_session_local(
                app_name=app_name,
                config=config,
                enable_hive_support=enable_hive_support,
            )
        elif platform == 'databricks':
            spark_session = cls._start_session_databricks(
                app_name=app_name,
                config=config,
                enable_hive_support=enable_hive_support,
            )
        elif platform == 'glue':
            spark_session = cls._start_session_glue(
                app_name=app_name,
                config=config
            )
        else:
            raise ValueError(f"Unsupported platform: {platform}")

        return spark_session

    @classmethod
    def _start_session_local(cls, app_name: Optional[str], config: Optional[dict],
                             enable_hive_support: bool) -> SparkSession:
        builder = SparkSession.builder.appName(app_name)
        if enable_hive_support:
            builder = builder.enableHiveSupport()
        if config:
            for key, value in config.items():
                builder = builder.config(key, value)
        spark_session = builder.getOrCreate()
        return spark_session

    @classmethod
    def _start_session_databricks(cls, app_name: Optional[str], config: Optional[dict],
                                  enable_hive_support: bool) -> SparkSession:
        # Assuming Databricks already provides a SparkSession
        from pyspark.sql import SparkSession
        spark_session = SparkSession.builder.getOrCreate()
        return spark_session

    @classmethod
    def _start_session_glue(cls, app_name: Optional[str], config: Optional[dict]) -> SparkSession:
        from awsglue.context import GlueContext
        from pyspark.context import SparkContext
        sc = SparkContext.getOrCreate()
        glueContext = GlueContext(sc)
        spark_session = glueContext.spark_session
        return spark_session

    @classmethod
    def stop_session(self, spark_session: SparkSession) -> None:
        spark_session.stop()
