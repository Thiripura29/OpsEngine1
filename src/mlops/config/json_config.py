import json
from mlops.core.base_config import BaseConfig


class JsonConfig(BaseConfig):
    def __init__(self, config_path: str):
        """
        Initialize JsonConfig object.

        :param config_path: Path to the JSON configuration file.
        """
        self.config_path = config_path
        super().__init__(self.load_config())

    def load_config(self):
        """
        Load configuration from a JSON file.

        This method reads the JSON file specified by the `config_path` and
        returns the parsed configuration as a dictionary.

        :return: Parsed JSON configuration as a dictionary.
        :raises ValueError: If the config file format is not JSON.
        """
        with open(self.config_path, 'r') as f:
            if self.config_path.endswith('.json'):
                return json.load(f)
            else:
                raise ValueError("Unsupported config file format. Use JSON.")
