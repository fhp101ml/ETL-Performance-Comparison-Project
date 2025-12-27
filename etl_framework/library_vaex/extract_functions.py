from etl_framework.abstract_etl_methods import Extractor
import vaex


class VaexCSVExtractor(Extractor):
    def __init__(self, file_path):
        self.file_path = file_path

    def extract(self):
        return vaex.from_csv(self.file_path, convert=True)


class VaexJSONExtractor(Extractor):
    def __init__(self, file_path):
        self.file_path = file_path

    def extract(self):
        return vaex.from_json(self.file_path)


class VaexHDF5Extractor(Extractor):
    def __init__(self, file_path):
        self.file_path = file_path

    def extract(self):
        try:
            # Abrir el archivo HDF5 con Vaex
            df_vaex = vaex.open(self.file_path)
            # Mostrar las primeras filas del DataFrame de Vaex
            return df_vaex
        except Exception as e:
            print(f"Error al abrir el archivo HDF5 con Vaex: {e}")



