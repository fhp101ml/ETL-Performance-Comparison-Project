from etl_framework.abstract_etl_methods import Extractor
import polars as pl

class PolarsCSVExtractor(Extractor):
    def __init__(self, file_path, separator=';'):
        self.file_path = file_path
        self.separator = separator

    def extract(self):
        # Polars handles encoding and bad lines differently, but we'll stick to defaults and essential config
        return pl.read_csv(self.file_path, separator=self.separator, ignore_errors=True)

class PolarsJSONExtractor(Extractor):
    def __init__(self, file_path):
        self.file_path = file_path

    def extract(self):
        return pl.read_json(self.file_path)

class PolarsParquetExtractor(Extractor):
    def __init__(self, file_path):
        self.file_path = file_path

    def extract(self):
        return pl.read_parquet(self.file_path)

# HDF5 is not natively supported well in Polars without conversion.
# We will focus on Parquet as the binary standard.
