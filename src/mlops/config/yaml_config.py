import yaml
from mlops.core.base_config import BaseConfig


class YamlConfig(BaseConfig):
    def __init__(self, config_path: str):
        """
        Initialize YamlConfig object.

        :param config_path: Path to the YAML configuration file.
        """
        self.config_path = config_path
        super().__init__(self.load_config())

    def load_config(self):
        """
        Load configuration from a YAML file.

        This method reads the YAML file specified by the `config_path` and
        returns the parsed configuration as a dictionary.

        :return: Parsed YAML configuration as a dictionary.
        :raises ValueError: If the config file format is not YAML.
        """
        with open(self.config_path, 'r') as f:
            if self.config_path.endswith('.yaml') or self.config_path.endswith('.yml'):
                return yaml.safe_load(f)
            else:
                raise ValueError("Unsupported config file format. Use YAML.")