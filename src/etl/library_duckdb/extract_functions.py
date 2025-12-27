from src.etl.abstract_etl_methods import Extractor
import duckdb

class DuckDBCSVExtractor(Extractor):
    def __init__(self, file_path, separator=';'):
        self.file_path = file_path
        self.separator = separator
        self.con = duckdb.connect(database=':memory:')

    def extract(self):
        # DuckDB relational API
        # read_csv_auto typically works well
        return self.con.read_csv(self.file_path, sep=self.separator)

class DuckDBJSONExtractor(Extractor):
    def __init__(self, file_path):
        self.file_path = file_path
        self.con = duckdb.connect(database=':memory:')

class DuckDBParquetExtractor(Extractor):
    def __init__(self, file_path):
        self.file_path = file_path
        self.con = duckdb.connect(database=':memory:')

    def extract(self):
        return self.con.read_parquet(self.file_path)
