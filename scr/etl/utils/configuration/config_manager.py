import yaml

from scr.etl.utils.configuration.dataset_metadata import DatasetMetadata
class ConfigManager:
    def __init__(self, config_path):
        self.config_path = config_path
        self.config = self.load_config()

    def load_config(self):
        with open(self.config_path, 'r') as file:
            return yaml.safe_load(file)

    def get_etl_config(self, titulo):
        return self.config.get(titulo, {})

    def get_metadata_config(self):
        metadata_config = self.config.get('metadata', {})
        print(metadata_config)
        return DatasetMetadata(
            attributes=metadata_config.get('attributes', []),
            types=metadata_config.get('types', {}),
            rules=metadata_config.get('rules', {})
        )

    def update_extractor_params(self, new_params):
        self.config['etl']['extractor_params'].update(new_params)
    def update_loader_params(self, new_params):
        self.config['etl']['loader_params'].update(new_params)