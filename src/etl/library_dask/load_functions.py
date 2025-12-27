from src.etl.abstract_etl_methods import Loader
import dask.dataframe as dd
import os

class DaskFileLoader(Loader):
    def __init__(self, output_path):
        self.output_path = output_path

    def load(self, data):
        # Dask typically writes to multiple files (partitioned), but can write to single if specified.
        # Single file output in dask: data.to_csv(..., single_file=True)
        if self.output_path.endswith('.parquet'):
            data.to_parquet(self.output_path)
        elif self.output_path.endswith('.json'):
            data.to_json(self.output_path)
        else:
            data.to_csv(self.output_path, sep=';', single_file=True, index=False)
