from etl_framework.library_dask.extract_functions import DaskCSVExtractor, DaskJSONExtractor, DaskParquetExtractor
from etl_framework.library_dask.dask_transformation_strategy import DaskTransformationStrategyFactory
from etl_framework.library_dask.load_functions import DaskFileLoader

class DaskETLFactory:
    def __init__(self, metadata):
        self.metadata = metadata

    def get_extractor(self, source_type, **kwargs):
        if source_type == "csv":
            return DaskCSVExtractor(kwargs['file_path'], kwargs.get('separator', ';'))
        elif source_type == "json":
            return DaskJSONExtractor(kwargs['file_path'])
        elif source_type == "parquet":
            return DaskParquetExtractor(kwargs['file_path'])
        else:
            raise ValueError("Unknown source type")

    def get_transformer_factory(self):
        return DaskTransformationStrategyFactory()

    def get_loader(self, destination_type, **kwargs):
        if destination_type == "file":
            return DaskFileLoader(kwargs['output_path'])
        else:
            raise ValueError("Unknown destination type")
