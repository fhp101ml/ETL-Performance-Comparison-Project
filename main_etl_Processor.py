import yaml
import sys
from scr.etl.etl_factory_provider import ETLFactoryProvider
from scr.etl.etl_processor import ETLProcessor
from scr.etl.utils.configuration.dataset_metadata import DatasetMetadata

def main():

    # Cargar configuración desde el archivo YAML
    with open('/home/ETL-PCP/config/config_process_.yaml', 'r') as file:
        config = yaml.safe_load(file)

    print(config)
    etl_config = config['etl']
    metadata_config = config['metadata']

    metadata = DatasetMetadata(
        attributes=metadata_config['attributes'],
        types=metadata_config['types'],
        rules=metadata_config['rules']
    )

    print(metadata.get_attributes(), etl_config['library_type'])
    print(metadata_config)
    print(config)
    print(etl_config['extractor_params'])

    etl_factory = ETLFactoryProvider.get_factory(etl_config['library_type'], metadata)

    processor = ETLProcessor(etl_factory, library_type=etl_config['library_type'])

    processor.process(
        source_type=etl_config['source_type'],
        extractor_params=etl_config['extractor_params'],
        destination_type=etl_config['destination_type'],
        loader_params=etl_config['loader_params'],
        transformations=etl_config['transformations']
    )

    print("Proceso de ETL completado con éxito.")

if __name__ == "__main__":
    main()