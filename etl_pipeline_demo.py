import os
import sys

# Importamos componentes de TI librería
from etl_framework.etl_factory_provider import ETLFactoryProvider
from etl_framework.etl_processor import ETLProcessor
from etl_framework.utils.monitoring import PerformanceMonitor

# --- 1. DEFINICIÓN DE NEGOCIO (Metadata) ---
# Esto define QUÉ es el dato, independiente de CÓMO se procesa.
class CustomerTransactionsMetadata:
    def get_attributes(self):
        return ['id', 'name', 'email', 'salary', 'last_login']
    
    def get_types(self):
        return {
            'id': int,
            'name': str,
            'email': str,
            'salary': float,
            'last_login': 'datetime'
        }
    
    def get_rules(self):
        return {
            'name': 'capitalize_first_letter',
            'salary': 'impute_mean',
            'last_login': 'days_since'
        }

# --- 2. CONFIGURACIÓN DEL JOB ---
# Elige tu arma: 'pandas', 'polars', 'duckdb', 'dask'
ENGINE = "polars" 

# Rutas
BASE_DIR = os.getcwd()
SOURCE_FILE = os.path.join(BASE_DIR, "data_test", "complex_input.csv")
OUTPUT_FILE = os.path.join(BASE_DIR, "data_test", "production_output.parquet")

def run_production_pipeline():
    print(f"🏭 Starting Production ETL Pipeline")
    print(f"⚙️  Selected Engine: {ENGINE.upper()}")
    print("-" * 50)

    # A. Inicialización (Dependency Injection)
    # Inyectamos la metadata de negocio en la fábrica del motor seleccionado
    metadata = CustomerTransactionsMetadata()
    
    try:
        factory = ETLFactoryProvider.get_factory(ENGINE, metadata)
    except ValueError as e:
        print(f"Error: {e}")
        return

    # Monitor para observabilidad
    monitor = PerformanceMonitor()

    # Instanciamos el procesador genérico
    processor = ETLProcessor(factory, ENGINE, monitor=monitor)

    # B. Parámetros de Ejecución
    # Configuramos la extracción (Source)
    extract_params = {
        'file_path': SOURCE_FILE,
        'separator': ';'
    }

    # Configuramos la carga (Sink/Destination)
    load_params = {
        'output_path': OUTPUT_FILE
    }

    # C. Definición de Transformaciones
    # Estas se aplicarán usando las implementaciones optimizadas del motor elegido
    pipeline_transformations = {
        'general': [], # Transformaciones a nivel de dataframe completo
        'attributes': {
            'name': ['capitalize_first_letter'],
            'salary': ['impute_mean'],
            'last_login': ['days_since']
        }
    }

    # D. Ejecución del Pipeline
    try:
        processor.process(
            source_type='csv',
            extractor_params=extract_params,
            destination_type='file',
            loader_params=load_params,
            transformations=pipeline_transformations
        )
        
        print("\n✅ Pipeline Finalizado Exitosamente")
        print(f"📂 Datos guardados en: {OUTPUT_FILE}")
        
        # E. Observabilidad
        monitor.print_report()
        
    except Exception as e:
        print(f"\n❌ CRITICAL ERROR in Pipeline: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    # Generar datos dummy si no existen (para que el demo funcione)
    if not os.path.exists(SOURCE_FILE):
        print("⚠️  Data not found. Generating mock data for simulation...")
        from etl_framework.utils.data_generator import generate_complex_dataset
        generate_complex_dataset(SOURCE_FILE, num_rows=5000)
        
    run_production_pipeline()
