from abc import ABC, abstractmethod


class DataReader(ABC):

    @abstractmethod
    def read_data(self):
        raise NotImplementedError

    @abstractmethod
    def get_dataframe(self):
        raise NotImplementedError

    @abstractmethod
    def validate_input_path(self, path: str):
        raise NotImplementedError

    @abstractmethod
    def validate_datasource_format(self, source_format: str):
        raise NotImplementedError
