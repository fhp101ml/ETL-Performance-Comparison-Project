import pandas as pd
import yaml

from scr.etl.etl_factory_provider import ETLFactoryProvider
from scr.etl.utils.configuration.dataset_metadata import DatasetMetadata

################
## only testing function. Not config loaded with yaml config file


def main():

    # Cargar configuración desde el archivo YAML
    with open('/home/ETL-PCP/config/config.yaml', 'r') as file:
        config = yaml.safe_load(file)

    etl_config = config['etl']
    metadata_config = config['metadata']

    metadata = DatasetMetadata(
        attributes=metadata_config['attributes'],
        types=metadata_config['types'],
        rules=metadata_config['rules']
    )

    library_type = "vaex"  # Puede ser "pandas" o "vaex"

    factory = ETLFactoryProvider.get_factory(library_type, metadata)

    # Parámetros de entrada

    # Extraction
    source_type = "hdf5" # puede ser csv, hdf5, json, database
    query = "SELECT * FROM usuarios"
    extraction_params = {
        "file_path": "/home/ETL-PCP/data/testing_data/output_testing.csv",
        "separator": ',',
        "db_type": "sqlite",  # Puede ser "sqlite", "mongodb", "mysql", "postgresql"
        "db_path": "/home/ETL-PCP/data/sqlite_database/testing_functions.db",
        "query": query}  # Parámetros específicos de la conexión a la base de datos


    # Transformation
    transform_types = ["simple", "remove_duplicates", "handle_missing_values", "calculate_age"]
    transform_types = ["simple", "calculate_age"]

    # Load
    destination_type = "file" # Puede ser "file" o "database"
    load_params_destination = {
         "db_type": "sqlite",
         "table_name": "usuarios",
         "db_path": "/home/ETL-PCP/data/sqlite_database/testing_functions.db",
         "database": "/home/ETL-PCP/data/sqlite_database/testing_functions.db",
         "output_path": "/home/ETL-PCP/data/testing_data/output_testing_functions"
         }  # Parámetros específicos de la conexión a la base de datos

    ###############################################################################################################################################################
    ###############################################################################################################################################################

    # Extracción
    extractor = factory.get_extractor(source_type, **extraction_params)
    data = extractor.extract()
    print("Datos extraídos:")
    print(data.head())
    print('\n'*2)
    print('*'*20)

    # Transformación
    transformer = factory.get_transformer_factory()
    transformed_data = pd.DataFrame()
    for transform_type in transform_types:
        transformed_data = transformer.get_strategy(transform_type, 'spanish').transform(data)

    print("Datos transformados:")
    print(transformed_data.head())
    print('\n'*2)
    print('*'*20)

    # Load
    loader = factory.get_loader(destination_type, **load_params_destination)
    loader.load(transformed_data)



if __name__ == "__main__":
    main()