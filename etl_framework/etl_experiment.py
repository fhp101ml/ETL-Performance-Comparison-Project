import csv
import os
import uuid
import time

import psutil

from etl_framework.etl_factory_provider import ETLFactoryProvider


class ETLExperiment:
    def __init__(self, config_manager):
        self.config_manager = config_manager
        self.etl_config = self.config_manager.get_etl_config('etl')
        self.metadata = self.config_manager.get_metadata_config()

    def measure_performance(self, library_type, transformations):
        etl_factory = ETLFactoryProvider.get_factory(library_type, self.metadata)

        # Extract
        extractor = etl_factory.get_extractor(
            self.etl_config['source_type'],
            **self.etl_config['extractor_params']
        )



        process = psutil.Process()
        file_path = self.etl_config['extractor_params']['file_path']
        num_records = sum(1 for line in open(file_path)) - 1  # subtract header
        file_size = os.path.getsize(file_path)


        start_time = time.time()
        data = extractor.extract()
        end_time = time.time()
        extract_time = end_time - start_time
        extract_memory_info = process.memory_info()
        extract_cpu_times = process.cpu_times()


        # Rest period
        time.sleep(5)


        # Transform
        transformer_factory = etl_factory.get_transformer_factory()
        attributes = self.metadata.get_attributes()
        start_time = time.time()
        # for attribute in attributes:
        #     if attribute in self.etl_config['transformations']:
        #         rule = self.metadata.get_rules().get(attribute)
        #         transformer = transformer_factory.get_strategy(rule, 'spanish')
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


        end_time = time.time()
        transform_time = end_time - start_time
        transform_memory_info = process.memory_info()
        transform_cpu_times = process.cpu_times()

        # Rest period
        time.sleep(5)

        # Load
        loader = etl_factory.get_loader(
            self.etl_config['destination_type'],
            **self.etl_config['loader_params']
        )
        start_time = time.time()
        loader.load(data)
        end_time = time.time()
        load_time = end_time - start_time
        load_memory_info = process.memory_info()
        load_cpu_times = process.cpu_times()



        experiment_id = str(uuid.uuid4())
        return {
            'experiment_id': experiment_id,
            'file_name': file_path,
            'num_records': num_records,
            'file_size': file_size,
            'library_type': library_type,
            'extract_time': extract_time,
            'extract_memory_usage': extract_memory_info.rss,
            'extract_cpu_times': extract_cpu_times.user + extract_cpu_times.system,
            'transform_time': transform_time,
            'transform_memory_usage': transform_memory_info.rss,
            'transform_cpu_times': transform_cpu_times.user + transform_cpu_times.system,
            'load_time': load_time,
            'load_memory_usage': load_memory_info.rss,
            'load_cpu_times': load_cpu_times.user + load_cpu_times.system
        }

    def save_results_to_csv(self, results):
        filename = self.etl_config['experiment_params']['output_path']
        keys = results[0].keys()
        with open(filename, 'w', newline='') as output_file:
            dict_writer = csv.DictWriter(output_file, fieldnames=keys)
            dict_writer.writeheader()
            dict_writer.writerows(results)

    def run(self, file_sizes):
        results = []
        transformations = self.etl_config['transformations']
        for size in file_sizes:
            self.config_manager.update_extractor_params({
                'file_path': f'/home/ETL-PCP/data/experimentDatasets/experiment_data_{size}.csv',
                'separator': ','
            })
            self.config_manager.update_loader_params({
                'output_path': f'/home/ETL-PCP/data/experimentDatasets/experiment_data_transformed_{size}',
            })
            # Medir rendimiento para Pandas
            pandas_results = self.measure_performance('pandas', transformations)
            results.append(pandas_results)

            # Medir rendimiento para Vaex
            vaex_results = self.measure_performance('vaex', transformations)
            results.append(vaex_results)

        # Guardar resultados en CSV
        self.save_results_to_csv(results)

        print("Proceso de ETL completado y resultados almacenados.")