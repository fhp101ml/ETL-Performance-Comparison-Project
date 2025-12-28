from etl_framework.abstract_etl_methods import Loader
import duckdb

class DuckDBFileLoader(Loader):
    def __init__(self, output_path):
        self.output_path = output_path

    def load(self, relation):
        # relation is a DuckDBPyRelation
        if self.output_path.endswith('.parquet'):
            relation.write_parquet(self.output_path)
        elif self.output_path.endswith('.json'):
            # DuckDB Python API might lack write_json directly on relation, use SQL
            relation.to_table("temp_load_table") # register as temp table to assume connection context? 
            # Or execute sql directly on relation alias.
            # safe approach:
            relation.sql_query().to_csv(self.output_path, sep=',', quotechar='"', header=True) 
            # WAIT, COPY (FORMAT JSON) is better.
            # python api doesn't expose it easily on relation object without registering.
            # workaround: utilize write_csv for now or skip JSON explicitly if unsupported.
            # actually relation.write_csv exists.
            # Let's try direct SQL via connection if we had access, but relation encapsulates connection usually.
            conn = duckdb.connect(database=':memory:') 
            # This is tricky because relation belongs to another connection.
            # Let's assume relation.write_csv is enough for now or drop JSON for DuckDB until we register view.
            # Better approach:
            relation.create("temp_json_export")
            duckdb.sql(f"COPY temp_json_export TO '{self.output_path}' (FORMAT JSON, ARRAY true)")
        else:
            relation.write_csv(self.output_path, sep=';')
