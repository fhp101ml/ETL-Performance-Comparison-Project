import os
import shutil
import time
import pandas as pd
import sys

# Add src to path if needed, but we are running as package structure now
# sys.path.append(os.path.join(os.getcwd(), 'src'))

from etl_framework.etl_factory_provider import ETLFactoryProvider
from etl_framework.utils.configuration.dataset_metadata import DatasetMetadata
from etl_framework.utils.monitoring import PerformanceMonitor

import csv
from datetime import datetime
# Mock metadata
class MockMetadata:
    def get_attributes(self):
        return ['name', 'age', 'price']
    
    def get_rules(self):
        return {
            'name': 'capitalize_first_letter',
            'age': 'calculate_age',
            'price': 'apply_discount'
        }

def create_dummy_data(path):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df = pd.DataFrame({
        'name': ['alice', 'bob', 'charlie', 'david'] * 1000,
        'age': [25, 30, 35, 40] * 1000,
        'price': [100.0, 200.0, 150.0, 50.0] * 1000
    })
    df.to_csv(path, sep=';', index=False)
    print(f"Created dummy data at {path} with {len(df)} rows.")

from etl_framework.etl_processor import ETLProcessor

# ...

from etl_framework.utils.configuration.config_manager import ConfigManager

# ...

def test_library(library, config_manager):
    print(f"\\nTesting Library: {library}")
    try:
        # Load base config
        etl_config = config_manager.get_etl_config('etl')
        
        # Override library for this iteration
        # In a real app we might load different files, but here we reuse the structure
        
        # Get Metadata from config
        # Note: ConfigManager creates a new DatasetMetadata object each time get_metadata_config is called
        metadata = config_manager.get_metadata_config()
        
        factory = ETLFactoryProvider.get_factory(library, metadata)
        
        # Dependency Injection of Monitor
        processor_monitor = PerformanceMonitor()
        
        processor = ETLProcessor(factory, library, monitor=processor_monitor)
        
        # Get paths from config
        input_path = etl_config['extractor_params']['file_path']
        output_base_dir = etl_config['loader_params']['output_path']
        output_path = os.path.join(output_base_dir, f"output_{library}.csv")
        
        # Prepare params
        extractor_params = etl_config['extractor_params']
        loader_params = {'output_path': output_path}
        transformations = etl_config['transformations']
        
        # Execute
        processor.process(
            source_type=etl_config['source_type'],
            extractor_params=extractor_params,
            destination_type=etl_config['destination_type'],
            loader_params=loader_params,
            transformations=transformations
        )
            
        print(f"  Success! Output saved to {output_path}")
        processor_monitor.print_report()
        return True
    
    except Exception as e:
        print(f"  FAILED: {e}")
        import traceback
        traceback.print_exc()
        return False

def save_experiment_result(results_file, run_data, monitor):
    """
    Appends a single experiment run to the CSV results file.
    """
    file_exists = os.path.isfile(results_file)
    
    # Extract metrics
    metrics = monitor.get_metrics()['operations']
    
    # Calculate totals
    total_duration = sum(m['duration_seconds'] for m in metrics.values())
    peak_memory = max(m['peak_memory_bytes'] for m in metrics.values()) if metrics else 0
    print(run_data.get('rows', 10000))
    row = {
        'timestamp': datetime.now().isoformat(),
        'library': run_data.get('library'),
        'source_type': run_data.get('source_type'),
        'destination_type': run_data.get('destination_type'),
        'file_rows': run_data.get('rows', 10000), # Default or pass from config
        'status': 'SUCCESS',
        'total_duration_sec': total_duration,
        'peak_memory_bytes': peak_memory,
        # Breakdown
        'setup_sec': metrics.get('setup', {}).get('duration_seconds', 0),
        'extract_sec': metrics.get('extraction', {}).get('duration_seconds', 0),
        'transform_sec': metrics.get('transform_general', {}).get('duration_seconds', 0) + metrics.get('transform_attributes', {}).get('duration_seconds', 0),
        'load_sec': metrics.get('loading', {}).get('duration_seconds', 0)
    }
    
    with open(results_file, 'a', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=row.keys())
        if not file_exists:
            writer.writeheader()
        writer.writerow(row)

def setup_experiment_workspace(base_dir, config_path):
    """
    Creates a unique folder for the experiment run, backs up config, and prepares manifest.
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    experiment_id = f"exp_{timestamp}"
    experiment_dir = os.path.join(base_dir, "experiments_results", experiment_id)
    
    os.makedirs(experiment_dir, exist_ok=True)
    
    # 1. Backup Config
    shutil.copy(config_path, os.path.join(experiment_dir, "config_snapshot.yaml"))
    
    # 2. Results Path
    results_csv_path = os.path.join(experiment_dir, "benchmark_results.csv")
    
    print(f"📁 Experiment Workspace Created: {experiment_dir}")
    return experiment_dir, results_csv_path, experiment_id

def main():
    base_dir = os.getcwd()
    config_path = os.path.join(base_dir, "config", "experiment_matrix.yaml")
    
    # Setup Experiment Workspace
    experiment_dir, results_csv_path, experiment_id = setup_experiment_workspace(base_dir, config_path)
    
    # Initialize Configuration
    config_manager = ConfigManager(config_path, manifest_path=os.path.join(base_dir, "config", "library_manifest.yaml"))
    
    # Create dummy data based on config path
    etl_config = config_manager.get_etl_config('etl')
    
    # Track used datasets for manifest
    used_datasets = []
    
    if 'base_input_path' in etl_config:
        # Matrix Mode Data Generation
        # ... 
        base_input = etl_config['base_input_path'] + ".csv"
        # We assume dataset generation happens or exists.
        # Ideally we should log the specific files used/generated.
        
        from etl_framework.utils.data_generator import generate_complex_dataset
        from etl_framework.utils.external_data_loader import download_spanish_municipalities
        
        ext_path = etl_config.get('external_data', {}).get('municipalities_path')
        if ext_path:
            download_spanish_municipalities(ext_path)
            used_datasets.append({'type': 'external', 'path': ext_path})
            
        # Configurable rows
        num_rows = etl_config.get('rows_limit', 1000)
        print(num_rows)
        generate_complex_dataset(base_input, num_rows=num_rows)
        used_datasets.append({'type': 'main_input_base', 'path': base_input, 'rows': num_rows})
        
    elif 'extractor_params' in etl_config:
         input_file = etl_config['extractor_params']['file_path']
         create_dummy_data(input_file)
         used_datasets.append({'type': 'main_input', 'path': input_file})
    
    # Write Manifest
    import json
    with open(os.path.join(experiment_dir, "manifest.json"), "w") as f:
        json.dump({
            "experiment_id": experiment_id,
            "timestamp": datetime.now().isoformat(),
            "datasets": used_datasets
        }, f, indent=4)

    # Detect if we are using the simple config or the matrix config
    if 'libraries' in etl_config and 'sources' in etl_config:
        # MATRIX MODE
        libraries = etl_config['libraries']
        sources = etl_config['sources']
        destinations = etl_config['destinations']
        
        base_input = etl_config['base_input_path']
        output_base = etl_config['output_base_dir']
        
        print(f"🚀 Starting MATRIX Benchmark")
        
        for lib in libraries:
            for src in sources:
                for dst in destinations:
                    # Construct paths
                    input_file = f"{base_input}.{src}"
                    output_file_name = f"output_{lib}_from_{src}_to_{dst}.{dst}"
                    output_full_path = os.path.join(output_base, output_file_name)
                    
                    print(f"\n--- Testing: {lib} | In: {src} -> Out: {dst} ---")
                    
                    try:
                        metadata = config_manager.get_metadata_config()
                        factory = ETLFactoryProvider.get_factory(lib, metadata)
                        processor_monitor = PerformanceMonitor()
                        processor = ETLProcessor(factory, lib, monitor=processor_monitor)
                        
                        # Specific params
                        ext_opts = etl_config.get('extractor_options', {}).get(src, {})
                        ext_params = {'file_path': input_file}
                        ext_params.update(ext_opts)
                        load_params = {'output_path': output_full_path}
                        transformations = etl_config['transformations']
                        
                        processor.process(
                            source_type=src,
                            extractor_params=ext_params,
                            destination_type="file",
                            loader_params=load_params,
                            transformations=transformations
                        )
                        print(f"  ✅ Success")
                        
                        # SAVE RESULTS
                        run_meta = {
                            'library': lib,
                            'source_type': src,
                            'destination_type': dst,
                            'rows': num_rows # Should ideally come from config or detected
                        }
                        save_experiment_result(results_csv_path, run_meta, processor_monitor)
                        
                    except Exception as e:
                        print(f"  ❌ FAILED: {e}")
                        import traceback
                        traceback.print_exc()

    else:
        # LEGACY / SIMPLE MODE
        # Read libraries to test from config
        libraries = etl_config.get('libraries_to_test', ['pandas'])
        print(f"Running benchmarks for: {libraries}")
        
        results = {}
        for lib in libraries:
            results[lib] = test_library(lib, config_manager)
            
        print("\n\n=== Final Results ===")
        for lib, success in results.items():
            print(f"{lib}: {'PASS' if success else 'FAIL'}")
        
    # If legacy 'results' exists (meaning experimentation wasn't matrix mode), print summary.
    if 'results' in locals():
        print("\n\n=== Final Results ===")
        for lib, success in results.items():
            print(f"{lib}: {'PASS' if success else 'FAIL'}")

if __name__ == "__main__":
    main()
