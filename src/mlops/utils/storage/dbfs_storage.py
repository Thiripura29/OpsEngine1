"""Module to represent a DBFS file storage system."""

from typing import Any
from urllib.parse import ParseResult, urlunparse

from mlops.core.base_file_storage import FileStorage
from mlops.logging.ops_logger import OpsLogger
from mlops.utils.databricks_utils import DatabricksUtils
from mlops.utils.spark_manager import PysparkSessionManager


class DBFSStorage(FileStorage):
    """Class to represent a DBFS file storage system."""

    _LOGGER = OpsLogger(__name__).get_logger()
    _MAX_INT = 2147483647

    @classmethod
    def get_file_payload(cls, url: ParseResult) -> Any:
        """Get the content of a file.

        Args:
            url: url of the file.

        Returns:
            File payload/content.
        """

        str_url = urlunparse(url)
        cls._LOGGER.info(f"Trying with dbfs_storage: Reading from file: {str_url}")
        return DatabricksUtils.get_db_utils(PysparkSessionManager.SESSION).fs.head(
            str_url, cls._MAX_INT
        )

    @classmethod
    def write_payload_to_file(cls, url: ParseResult, content: str) -> None:
        """Write payload into a file.

        Args:
            url: url of the file.
            content: content to write into the file.
        """

        str_url = urlunparse(url)
        cls._LOGGER.info(f"Trying with dbfs_storage: Writing into file: {str_url}")
        DatabricksUtils.get_db_utils(PysparkSessionManager.SESSION).fs.put(str_url, content, True)
