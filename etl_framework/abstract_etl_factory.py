from abc import ABC, abstractmethod
import sys


class AbstractETLFactory(ABC):
    @abstractmethod
    def get_extractor(self, source_type, file_path):
        pass

    @abstractmethod
    def get_transformer(self, transform_type):
        pass

    @abstractmethod
    def get_loader(self, destination_type, path_or_connection):
        pass

