from scr.etl.library_pandas.extract_functions import PandasCSVExtractor, PandasJSONExtractor, PandasHDF5Extractor
from scr.etl.library_pandas.pandas_transformation_strategy import PandasTransformationStrategyFactory
from scr.etl.library_pandas.load_functions import PandasFileLoader
from scr.etl.utils.database.database_extractor_factory import DatabaseExtractorFactory
from scr.etl.utils.database.database_loader_factory import DatabaseLoaderFactory
class PandasETLFactory:
    def __init__(self, metadata):
      self.metadata = metadata

    def get_extractor(self, source_type, **kwargs):

        if source_type == "csv":
            return PandasCSVExtractor(kwargs['file_path'], kwargs['separator'])
        elif source_type == "json":
            return PandasJSONExtractor(kwargs['file_path'])
        elif source_type == "hdf5":
            return PandasHDF5Extractor(kwargs['file_path'])
        elif source_type == "database":
          return DatabaseExtractorFactory().get_extractor( **kwargs)
        else:
            raise ValueError("Unknown source type")


    def get_transformer_factory(self):

          return PandasTransformationStrategyFactory()



    def get_loader(self, destination_type, **kwargs):
        if destination_type == "file":
            return PandasFileLoader(kwargs['output_path'])
        elif destination_type == "database":
            return DatabaseLoaderFactory().get_loader(**kwargs)
        else:
            raise ValueError("Unknown destination type")
