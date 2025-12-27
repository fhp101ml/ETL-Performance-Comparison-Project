from src.etl.library_duckdb.extract_functions import DuckDBCSVExtractor, DuckDBJSONExtractor, DuckDBParquetExtractor
from src.etl.library_duckdb.duckdb_transformation_strategy import DuckDBTransformationStrategyFactory
from src.etl.library_duckdb.load_functions import DuckDBFileLoader

class DuckDBETLFactory:
    def __init__(self, metadata):
        self.metadata = metadata

    def get_extractor(self, source_type, **kwargs):
        if source_type == "csv":
            return DuckDBCSVExtractor(kwargs['file_path'], kwargs.get('separator', ';'))
        elif source_type == "json":
            return DuckDBJSONExtractor(kwargs['file_path'])
        elif source_type == "parquet":
            return DuckDBParquetExtractor(kwargs['file_path'])
        else:
            raise ValueError("Unknown source type")

    def get_transformer_factory(self):
        return DuckDBTransformationStrategyFactory()

    def get_loader(self, destination_type, **kwargs):
        if destination_type == "file":
            return DuckDBFileLoader(kwargs['output_path'])
        else:
            raise ValueError("Unknown destination type")
