import yaml
import os
from etl_framework.utils.configuration.dataset_metadata import DatasetMetadata

class ConfigManager:
    def __init__(self, config_path, manifest_path=None):
        self.config_path = config_path
        self.manifest_path = manifest_path
        self.config = self.load_yaml(self.config_path)
        self.manifest = self.load_yaml(self.manifest_path) if self.manifest_path else {}

    def load_yaml(self, path):
        if not path or not os.path.exists(path):
            return {}
        with open(path, 'r') as file:
            return yaml.safe_load(file)

    def get_etl_config(self, titulo):
        return self.config.get(titulo, {})

    def get_metadata_config(self):
        """Returns the dataset contract (schema/rules) for the current experiment"""
        metadata_config = self.config.get('metadata', {})
        return DatasetMetadata(
            attributes=metadata_config.get('attributes', []),
            types=metadata_config.get('types', {}),
            rules=metadata_config.get('rules', {})
        )
    
    def get_library_manifest(self):
        """Returns the library capabilities definition"""
        return self.manifest

    def update_extractor_params(self, new_params):
        if 'etl' in self.config and 'extractor_params' in self.config['etl']:
            self.config['etl']['extractor_params'].update(new_params)
            
    def update_loader_params(self, new_params):
        if 'etl' in self.config and 'loader_params' in self.config['etl']:
            self.config['etl']['loader_params'].update(new_params)