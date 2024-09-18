import ast
import json
from typing import Union

import yaml
from jinja2 import Template

DEFAULT_RENDER_ENV = ''


class RenderUtils:
    """Class with utilities to render jinja variables."""
    DEFAULT_RENDER_ENV = ''

    def __init__(
            self,
            env=DEFAULT_RENDER_ENV,
            configs_folder_path=None,
    ):
        """
        Instantiate objects of the class RenderUtils.

        :param notebook_context: context of the notebook instantiating this object.
        :param env: the environment we want to utilize.
        :param data_product: optional argument that defines the data product where we are getting the configuration
            file.
        :param configs_folder_path: optional folder path where to get the configs from.
        """
        self.env = env
        self.configs_folder_path = configs_folder_path
        self.irregular_json_types = ["DataFrame", "function"]

    def render_content(
            self,
            content_to_render: Union[dict, str],
            irregular_json_types=None
    ) -> Union[dict, str]:
        """Render jinja configs of a given dict or string into their values.

        After defining the environment and the content_to_render, we call the jinja render function,
        passing the config file specific to that data product, the environment and then we render
        the content_to_render.

        :param content_to_render: the dict or string possessing the content we want to render.
        :param irregular_json_types: list of user defined irregular json types not to be rendered.

        :return: a dict or string, depending on the input type, with the jinja configs replaced.
        """
        if self.env != DEFAULT_RENDER_ENV:

            with open(self.configs_folder_path) as config_file:
                dict_yaml = yaml.safe_load(config_file)

            print(type(content_to_render))
            if type(content_to_render) is dict:
                print("inside if")
                irregular_json_objs = {}
                self.string_value = 0

                if irregular_json_types is not None:
                    irregular_json_types.extend(self.irregular_json_types)
                else:
                    irregular_json_types = self.irregular_json_types

                self._read_acon_pairs(content=content_to_render, mode="type2str", mark="",
                                      irregular_json_objs=irregular_json_objs,
                                      irregular_json_types=irregular_json_types)

                print(str(content_to_render))
                template = Template(str(content_to_render))
                print(template)
                rendered_content = template.render(configs=dict_yaml)
                # Transforms an unformatted string into a json string and returns the dict that is inside the string.
                formatted_content = ast.literal_eval(rendered_content.replace("\n", " "))
                json_string = json.dumps(formatted_content, indent=4, sort_keys=False)
                json_dict = json.loads(json_string)

                self._read_acon_pairs(content=json_dict, mode="str2type", mark="",
                                      irregular_json_objs=irregular_json_objs,
                                      irregular_json_types=irregular_json_types)
                return json_dict
            else:
                template = Template(content_to_render)
                rendered_content = template.render(configs=dict_yaml)
                return rendered_content
        else:
            return content_to_render

    def _read_acon_pairs(self, content: dict, mode: str, mark: str, irregular_json_objs: dict,
                         irregular_json_types: list):
        """Iterate over each ACON key-value pair, even nested ones, to validate if there are non-supported
        and/or user defined JSON data types.

        For each key-value pair in the content we want to render, and based on the mode selected, checks if
        JSON non-supported and/or user defined irregular json data types are present in the content.
        If there are, calls the "_define_string_value" method (in case of the "type2str" mode), followed by the
        update of the non-supported JSON format to string (key-value pair update). In the case of the "str2type"
        mode updates the previously converted key-value pair to it's original data type format.

        :param content: the dict or string possessing the content we want to render.
        :param mode: the mode we want to use.
        :param mark: optional character to be included in the name of the function (i.e. * => "*myfunc*").
        :param irregular_json_objs: dict to store the original key-value pair that will be updated to string.
        :param irregular_json_types: list of JSON non-supported and/or user defined irregular json data types.
        """
        if isinstance(content, list):
            for index in content:
                self._read_acon_pairs(content=index, mode=mode, mark=mark, irregular_json_objs=irregular_json_objs,
                                      irregular_json_types=irregular_json_types)
        if isinstance(content, dict):
            for key, value in content.items():
                if not isinstance(value, (list, dict)):
                    if mode == "type2str" and value.__class__.__name__ in irregular_json_types:
                        value_name = self._define_string_value(value=value, mark=mark)
                        irregular_json_objs[value_name] = content[key]

                        content[key] = value_name
                    elif mode == "str2type" and value in irregular_json_objs:
                        value_name = content[key]
                        value_name = value_name.replace(mark, "")  # clear mark from function name :  *myfunc* => myfunc

                        content[key] = irregular_json_objs[value_name]
                else:
                    self._read_acon_pairs(content=value, mode=mode, mark=mark, irregular_json_objs=irregular_json_objs,
                                          irregular_json_types=irregular_json_types)

    def _define_string_value(self, value, mark: str):
        """Define the name of the string that will replace the non-supported JSON format in the ACON.

        Based on the each key-value pair, defines the string name that will replace the non-supported
        JSON format in the ACON.

        :param value: the dict value associated to the key.
        :param mark: optional character to be included in the name of the function (i.e. * => "*myfunc*").

        :return: a string that represent the name that will replace the non-supported JSON format in the ACON.
        """
        if value.__class__.__name__ == "function":
            value_name = f"{value.__name__}_{value.__class__.__name__}_{str(self.string_value)}"
        elif value.__class__.__name__ == "DataFrame":
            value_name = f"df_{value.__class__.__name__}_{str(self.string_value)}"
        else:
            value_name = f"{str(value)}_{value.__class__.__name__}_{str(self.string_value)}"
        value_name = mark + value_name + mark  # add mark:  myfunc => *myfunc*
        self.string_value = self.string_value + 1
        return value_name


render_utils = RenderUtils('dev',configs_folder_path="C:/Users/Prudhvi_Akella/PycharmProjects/mlops_with_mlflow/configs/dev"
                                               ".yaml")

print(render_utils.render_content("{{ configs.datacamp.bucket_name }}"))