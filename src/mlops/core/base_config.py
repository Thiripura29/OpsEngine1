import json
from abc import ABC, abstractmethod
from jinja2 import Template


class BaseConfig(ABC):
    def __init__(self, config_data: dict):
        """
        Initialize the BaseConfig class.

        Args:
            config_data (dict): The configuration data as a dictionary.
        """
        self.config_data: dict = config_data

    def __str__(self):
        return json.dumps(self.get_config_as_json())

    @abstractmethod
    def load_config(self):
        """
        Load the configuration data.

        This method should be implemented by subclasses.
        """
        raise NotImplementedError("Subclasses should implement this method")

    def get_config_as_json(self):
        return self.config_data

    def get(self, config_name):
        """
        Get the rendered configuration data for a specific config name.

        Args:
            config_name (str): The name of the configuration to render.

        Returns:
            str: The rendered configuration data.
        """
        try:
            template = Template(str(config_name))
            rendered_content = template.render(configs=self.config_data)
            return rendered_content
        except Exception as e:
            print(f"An error occurred while rendering the config: {e}")
            return None
