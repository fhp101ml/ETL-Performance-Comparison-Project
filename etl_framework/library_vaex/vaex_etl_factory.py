from etl_framework.library_vaex.extract_functions import *
from etl_framework.library_vaex.vaex_transformation_strategy import VaexTransformationStrategyFactory
from etl_framework.library_vaex.load_functions import VaexFileLoader
from etl_framework.utils.database.database_extractor_factory import DatabaseExtractorFactory
from etl_framework.utils.database.database_loader_factory import DatabaseLoaderFactory


class VaexETLFactory:
    def __init__(self, metadata):
      self.metadata = metadata
    def get_extractor(self, source_type, **kwargs):
        if source_type == "csv":
            return VaexCSVExtractor(kwargs['file_path'])
        elif source_type == "json":
            return VaexJSONExtractor(kwargs['file_path'])
        elif source_type == "hdf5":
            return VaexHDF5Extractor(kwargs['file_path'])
        elif source_type == "database":
          return DatabaseExtractorFactory().get_extractor( **kwargs)
        else:
            raise ValueError("Unknown source type")


    def get_transformer_factory(self):

          return VaexTransformationStrategyFactory()


    def get_loader(self, destination_type, **kwargs):
        if destination_type == "file":
            return VaexFileLoader(kwargs['output_path'])
        elif destination_type == "database":
            return DatabaseLoaderFactory.get_loader(**kwargs)
        else:
            raise ValueError("Unknown destination type")