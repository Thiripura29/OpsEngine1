from abc import ABC, abstractmethod


class DataWriter(ABC):
    @abstractmethod
    def write_data(self):
        raise NotImplementedError

    @abstractmethod
    def validate_output_path(self, path: str):
        raise NotImplementedError

    @abstractmethod
    def validate_datasource_format(self, source_format: str):
        raise NotImplementedError
