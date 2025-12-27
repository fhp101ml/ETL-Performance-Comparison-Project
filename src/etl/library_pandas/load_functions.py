from src.etl.abstract_etl_methods import Loader
import os
import time


class PandasFileLoader(Loader):
    def __init__(self, output_path):
        self.output_path = output_path
        # Remove forcible .csv appending to allow other formats

    def load(self, data):
        # File rotation logic (simplified for benchmark)
        if os.path.exists(self.output_path):
             # Just overwrite or simplistic backup?
             # Let's keep your rotation logic generically if possible, or just overwrite for benchmark cleanliness
             pass

        if self.output_path.endswith('.parquet'):
            data.to_parquet(self.output_path, index=False)
        elif self.output_path.endswith('.json'):
            data.to_json(self.output_path, orient='records', indent=4)
        else:
             # Default CSV
             # data.to_csv(self.output_path, index=False, sep=';')
             # Match separator used in others
             data.to_csv(self.output_path, index=False, sep=';')
