from etl_framework.abstract_etl_methods import Loader
import polars as pl
import os

class PolarsFileLoader(Loader):
    def __init__(self, output_path):
        self.output_path = output_path

    def load(self, data):
        # Determine extension
        if self.output_path.endswith('.parquet'):
            data.write_parquet(self.output_path)
        elif self.output_path.endswith('.json'):
            data.write_json(self.output_path)
        else:
            # Default to csv
            data.write_csv(self.output_path, separator=';')
