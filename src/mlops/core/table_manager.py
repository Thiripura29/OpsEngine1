from mlops.core.definitions import SQLDefinitions
from mlops.logging.ops_logger import OpsLogger
from mlops.utils.config_utils import ConfigUtils
from mlops.utils.spark_manager import PysparkSessionManager
from mlops.utils.sql_parser_utils import SQLParserUtils
from pyspark.sql import DataFrame


class TableManager(object):
    """Set of actions to manipulate tables/views in several ways."""

    def __init__(self, configs: dict):
        """Construct TableManager algorithm instances.

        Args:
            configs: configurations for the TableManager algorithm.
        """
        self._logger = OpsLogger(__name__).get_logger()
        self.configs = configs

    def create_many(self) -> None:
        """Create multiple tables or views on metastore.

        In this function the path to the ddl files can be separated by comma.
        """
        self.execute_multiple_sql_files()

    def execute_multiple_sql_files(self) -> None:
        """Execute multiple statements in multiple sql files.

        In this function the path to the files is separated by comma.
        """
        for table_metadata_file in self.configs["path"].split(","):
            disable_dbfs_retry = (
                self.configs["disable_dbfs_retry"]
                if "disable_dbfs_retry" in self.configs.keys()
                else False
            )
            sql = ConfigUtils.read_sql(table_metadata_file.strip(), disable_dbfs_retry)
            sql_commands = SQLParserUtils().split_sql_commands(
                sql_commands=sql,
                delimiter=self.configs.get("delimiter", ";"),
                advanced_parser=self.configs.get("advanced_parser", False),
            )
            for command in sql_commands:
                if command.strip():
                    self._logger.info(f"sql command: {command}")
                    PysparkSessionManager.SESSION.sql(command)
            self._logger.info("sql file successfully executed!")

    def execute_sql(self) -> None:
        """Execute sql commands separated by semicolon (;)."""
        sql_commands = SQLParserUtils().split_sql_commands(
            sql_commands=self.configs.get("sql"),
            delimiter=self.configs.get("delimiter", ";"),
            advanced_parser=self.configs.get("advanced_parser", False),
        )
        for command in sql_commands:
            if command.strip():
                self._logger.info(f"sql command: {command}")
                PysparkSessionManager.SESSION.sql(command)
        self._logger.info("sql successfully executed!")

    def show_tbl_properties(self) -> DataFrame:
        """Show Table Properties.

        Returns:
            A dataframe with the table properties.
        """
        show_tbl_props_stmt = "{} {}".format(
            SQLDefinitions.show_tbl_props_stmt.value,
            self.configs["table_or_view"],
        )

        self._logger.info(f"sql command: {show_tbl_props_stmt}")
        output = PysparkSessionManager.SESSION.sql(show_tbl_props_stmt)
        self._logger.info(output)
        return output
