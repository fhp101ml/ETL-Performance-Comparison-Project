import time
from etl_framework.utils.monitoring import PerformanceMonitor

class ETLProcessor:
    def __init__(self, etl_factory, library_type, monitor=None):
        self.etl_factory = etl_factory
        self.library_type = library_type
        self.monitor = monitor if monitor else PerformanceMonitor()

    def process(self, source_type, extractor_params, destination_type, loader_params, transformations):
        print(f"Processing with {self.library_type}")
        print(source_type, extractor_params, destination_type, loader_params, transformations)

        with self.monitor.measure_time("setup"):
            extractor = self.etl_factory.get_extractor(source_type, **extractor_params)
            loader = self.etl_factory.get_loader(destination_type, **loader_params)
            transformer_factory = self.etl_factory.get_transformer_factory()

        with self.monitor.measure_time("extraction"):
            data = extractor.extract()

        attributes = self.etl_factory.metadata.get_attributes()

        # Apply transformations
        if 'general' in transformations:
            with self.monitor.measure_time("transform_general"):
                for general_transform in transformations['general']:
                    print(f'\nApplying general transform: {general_transform}')
                    transformer = transformer_factory.get_strategy(general_transform, 'spanish')
                    data = transformer.transform(data)
        
        if 'attributes' in transformations:
            with self.monitor.measure_time("transform_attributes"):
                for attribute, rules in transformations['attributes'].items():
                    for rule in rules:
                        print(f'\nAttribute {attribute}: Applying rule {rule}')
                        transformer = transformer_factory.get_strategy(rule, 'spanish')
                        # Note: In a real attribute transformation, we might need to target specific columns
                        # This implementation assumes the transformer handles the whole dataframe/dataset
                        # or knows how to target the attribute if passed differently.
                        # For now, we keep the existing logic.
                        data = transformer.transform(data)

        with self.monitor.measure_time("loading"):
            loader.load(data)
            
        return self.monitor.get_report()

