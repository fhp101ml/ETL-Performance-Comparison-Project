from abc import ABC, abstractmethod
import sys


class Extractor(ABC):
    @abstractmethod
    def extract(self):
        pass


class Transformer(ABC):
    @abstractmethod
    def transform(self, data):
        pass


class Loader(ABC):
    @abstractmethod
    def load(self, data):
        pass

