from etl_framework.abstract_etl_methods import Extractor
import dask.dataframe as dd

class DaskCSVExtractor(Extractor):
    def __init__(self, file_path, separator=';'):
        self.file_path = file_path
        self.separator = separator

    def extract(self):
        return dd.read_csv(self.file_path, sep=self.separator, encoding='utf8', sample=5000000, assume_missing=True)

class DaskJSONExtractor(Extractor):
    def __init__(self, file_path):
        self.file_path = file_path

    def extract(self):
        return dd.read_json(self.file_path, orient='records', lines=False) # lines=False for standard json array

class DaskParquetExtractor(Extractor):
    def __init__(self, file_path):
        self.file_path = file_path

    def extract(self):
        return dd.read_parquet(self.file_path)
