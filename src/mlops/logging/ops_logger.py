import logging

from mlops.logging.ops_log_formatter import OpsLogFormatter


class OpsLogger:
    """
    Creates a logger instance with a specific JSON formatter.
    """

    def __init__(self, name: str, log_level: int = logging.DEBUG, log_file: str = None):
        """
        Initializes the OpsLogger instance.

        Args:
            name: The name of the logger.
            log_level: The logging level (default: logging.INFO).
            log_file: The path to the log file (optional).
        """

        self.logger = logging.getLogger(name)
        self.logger.setLevel(log_level)

        formatter = OpsLogFormatter(
            "{'time':'%(asctime)s','name':'%(name)s','filename':'%(filename)s','level': '%("
            "levelname)s', "
            "'message': '%(message)s','additional_arguments':'%(additional_arguments)s'}"
        )

        # Create a StreamHandler
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)
        self.logger.addHandler(stream_handler)

        # Create a FileHandler if log_file is specified
        if log_file:
            file_handler = logging.FileHandler(log_file)
            file_handler.setFormatter(formatter)
            self.logger.addHandler(file_handler)

    def get_logger(self):
        """
        Returns the logger instance.

        Returns:
            The logger instance.
        """
        return self.logger
