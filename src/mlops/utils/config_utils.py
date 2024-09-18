from typing import Any

from mlops.utils.storage.file_storage_functions import FileStorageFunctions


class ConfigUtils(object):
    """Config utilities class."""

    @staticmethod
    def read_sql(path: str, disable_dbfs_retry: bool = False) -> Any:
        """Read a DDL file in Spark SQL format from a cloud object storage system.

        Args:
            path: path to the SQL file.
            disable_dbfs_retry: optional flag to disable file storage dbfs.

        Returns:
            Content of the SQL file.
        """
        return FileStorageFunctions.read_sql(path, disable_dbfs_retry)

    @staticmethod
    def generate_cli_parser_code(config) -> str:
        parameter_list_str = ''
        args_description_list = []
        # Loop through each parameter in the config and generate the argument parsing code
        for param in config['parameters']:
            name = f"--{param['name']}"
            param_type = param['type']
            required = param['required']
            help_text = param['help']
            choices = param.get('choices', None)

            # Map YAML types to Python types
            if param_type == 'str':
                arg_type = 'str'
            elif param_type == 'int':
                arg_type = 'int'
            elif param_type == 'float':
                arg_type = 'float'
            elif param_type == 'bool':
                arg_type = 'bool'
            else:
                raise ValueError(f"Unsupported type: {param_type}")

            # Build the argument line
            argument_line = f'parser.add_argument("{name}", type={arg_type}, required={required}, help="{help_text}"'
            if choices:
                choices_str = ', '.join([f'"{choice}"' for choice in choices])
                argument_line += f', choices=[{choices_str}]'
                args_description_list.append(
                    f'\t\t"{name}", type={arg_type}, required={required}, help="{help_text}", choices=[{choices_str}] \n')
            argument_line += ')\n'
            if not choices:
                args_description_list.append(
                    f'\t\t"{name}", type={arg_type}, required={required}, help="{help_text}" \n')

            # Add the argument line to the generated code
            parameter_list_str += argument_line

        # Initialize the string that will contain the generated code
        generated_code = f'''import argparse

        parser = argparse.ArgumentParser(
            description=(
                Entry point arguments 
        {"".join(args_description_list)}
            )
        )
        ''' + parameter_list_str

        # Final part of the code
        generated_code += '''
        # Parse the arguments
        args = parser.parse_args()
        '''
        return generated_code

