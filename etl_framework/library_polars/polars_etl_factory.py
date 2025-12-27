from etl_framework.library_polars.extract_functions import PolarsCSVExtractor, PolarsJSONExtractor, PolarsParquetExtractor
from etl_framework.library_polars.polars_transformation_strategy import PolarsTransformationStrategyFactory
from etl_framework.library_polars.load_functions import PolarsFileLoader

class PolarsETLFactory:
    def __init__(self, metadata):
        self.metadata = metadata

    def get_extractor(self, source_type, **kwargs):
        if source_type == "csv":
            return PolarsCSVExtractor(kwargs['file_path'], kwargs.get('separator', ';'))
        elif source_type == "json":
            return PolarsJSONExtractor(kwargs['file_path'])
        elif source_type == "parquet":
            return PolarsParquetExtractor(kwargs['file_path'])
        else:
            raise ValueError("Unknown source type or not supported in Polars factory")

    def get_transformer_factory(self):
        return PolarsTransformationStrategyFactory()

    def get_loader(self, destination_type, **kwargs):
        if destination_type == "file":
            return PolarsFileLoader(kwargs['output_path'])
        else:
            raise ValueError("Unknown destination type")
