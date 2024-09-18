from mlops.config.json_config import JsonConfig
from mlops.config.yaml_config import YamlConfig
from mlops.core.base_config import BaseConfig


class ConfigurationManager:
    def __init__(self, config_path: str):
        """
        Initializes the ConfigurationManager with a given configuration file path.

        Parameters:
        config_path (str): The path to the configuration file, which can be either a JSON or YAML file.
        """
        # Determine the type of configuration file based on its extension
        # If the file is JSON, create an instance of JsonConfig
        # If the file is YAML, create an instance of YamlConfig
        # If the file has an unsupported extension, set config to None
        self.config_path = config_path
        self.config = JsonConfig(config_path) if config_path.endswith('.json') else \
            YamlConfig(config_path) if config_path.endswith('.yaml') else None

    def get_config(self) -> BaseConfig:
        """
        Returns the loaded configuration object.

        Raises:
            RuntimeError: If the configuration file could not be loaded due to unsupported format.
        Returns:
        BaseConfig: The configuration object which can be either a JsonConfig or YamlConfig instance if not None.
        """
        if self.config is None:
            raise RuntimeError(f"Unsupported configuration file format: {self.config_path}")

        return self.config

    def get_config_as_json(self):
        return self.config.get_config_as_json()

    def get_entry_point_config(self, entry_point_name: str):
        main_config_data = self.config.get_config_as_json().get('entry_points')
        if main_config_data:
            for item in main_config_data:
                value = item.get(entry_point_name)
                if value:
                    return value
            raise KeyError(f"Invalid entry point : {entry_point_name}")
        else:
            raise KeyError(f"entry_points must exist. its not found")
