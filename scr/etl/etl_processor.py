import time


class ETLProcessor:
    def __init__(self, etl_factory, library_type):
        self.etl_factory = etl_factory
        self.library_type = library_type

    def process(self, source_type, extractor_params, destination_type, loader_params, transformations):

        print(source_type, extractor_params, destination_type, loader_params, transformations)

        extractor = self.etl_factory.get_extractor(source_type, **extractor_params)
        loader = self.etl_factory.get_loader(destination_type, **loader_params)
        transformer_factory = self.etl_factory.get_transformer_factory()

        data = extractor.extract()

        attributes = self.etl_factory.metadata.get_attributes()
        # for attribute in attributes:
        #     if attribute in transformations:
        #         print('----', attribute, '\n')
        #         rule = self.etl_factory.metadata.get_rules().get(attribute)
        #         transformer = transformer_factory.get_strategy(rule)
        #         data = transformer.transform(data)

        # Apply transformations
        if 'general' in transformations:
            for general_transform in transformations['general']:
                print('\n, general_transform')
                print(general_transform)
                transformer =transformer_factory.get_strategy(general_transform, 'spanish')
                data = transformer.transform(data)
                time.sleep(1)  # Allow system resources to stabilize
        print('-'*60)
        if 'attributes' in transformations:
            for attribute, rules in transformations['attributes'].items():
                for rule in rules:
                    print(f'\n, {attribute}: -{rule} ,')

                    transformer = transformer_factory.get_strategy(rule, 'spanish')
                    data = transformer.transform(data)
                    time.sleep(1)  # Allow system resources to stabilize


        loader.load(data)

