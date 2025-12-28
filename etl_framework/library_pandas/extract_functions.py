from etl_framework.abstract_etl_methods import Extractor
import pandas as pd

try:
    import vaex
    _VAEX_AVAILABLE = True
except ImportError:
    _VAEX_AVAILABLE = False


class PandasCSVExtractor(Extractor):
    def __init__(self, file_path, separator=';'):
        self.file_path = file_path
        self.separator = separator

    def extract(self):
        return pd.read_csv(self.file_path,sep=self.separator, encoding='utf8',
                           header=0, on_bad_lines='skip')
        # return pd.read_csv(self.file_path)


class PandasJSONExtractor(Extractor):
    def __init__(self, file_path):
        self.file_path = file_path

    def extract(self):
        return pd.read_json(self.file_path)


class PandasParquetExtractor(Extractor):
    def __init__(self, file_path):
        self.file_path = file_path

    def extract(self):
        return pd.read_parquet(self.file_path)

class PandasHDF5Extractor(Extractor):
    # ... (existing content logic preserved if needed or simple pass as HDF5 is less prioritized now)
    def __init__(self, file_path):
        self.file_path = file_path

    def extract(self):
        if not _VAEX_AVAILABLE:
            return pd.read_hdf(self.file_path)
        
        df_vaex = vaex.open(self.file_path)
        df = df_vaex.to_pandas_df()
        return df

