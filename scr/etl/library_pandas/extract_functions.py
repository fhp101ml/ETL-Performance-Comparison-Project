from scr.etl.abstract_etl_methods import Extractor

import pandas as pd
import vaex


class PandasCSVExtractor(Extractor):
    def __init__(self, file_path, separator=';'):
        self.file_path = file_path
        self.separator = separator

    def extract(self):
        return pd.read_csv(self.file_path,sep=self.separator, encoding='utf8',
                           header=0, keep_default_na=False, on_bad_lines='skip')
        # return pd.read_csv(self.file_path)


class PandasJSONExtractor(Extractor):
    def __init__(self, file_path):
        self.file_path = file_path

    def extract(self):
        return pd.read_json(self.file_path)


class PandasHDF5Extractor(Extractor):
    def __init__(self, file_path):
        self.file_path = file_path

    def extract(self):
        df_vaex = vaex.open(self.file_path)
        # Convertir el DataFrame de Vaex a un DataFrame de Pandas
        df = df_vaex.to_pandas_df()
        return df

