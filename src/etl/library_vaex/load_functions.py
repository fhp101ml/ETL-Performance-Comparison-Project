from src.etl.abstract_etl_methods import Loader
import os
import time


class VaexFileLoader(Loader):
    def __init__(self, file_path):
        self.file_path = file_path + '.hdf5'

    def load(self, data):
        if os.path.exists(self.file_path):
            # Get the current date
            current_timestamp = int(time.time())

            # Create a new file name with the current date
            new_file_path = f"{self.file_path.rsplit('.', 1)[0]}_{current_timestamp}.{self.file_path.rsplit('.', 1)[1]}"
            # Rename the old file
            os.rename(self.file_path, new_file_path)
            print(f"--------- Archivo existente renombrado a: {new_file_path}")
        data.export_hdf5(self.file_path)
        print(f"--------- Datos cargados en el archivo: {self.file_path}")

