# ETL-Performance-Comparison-Project
# ETL Project
This project is an ETL (Extract, Transform, Load) framework designed to facilitate data extraction, transformation, and loading processes using multiple libraries, specifically **Pandas, Polars, DuckDB, Dask, and Vaex**. The framework is built to support flexibility and extensibility, incorporating several design patterns to improve modularity and scalability. Additionally, it includes functionality for performance measurement, allowing users to monitor the execution time and resource usage for each ETL operation.

Key Features

* *Data Extraction*: Allows data retrieval from various sources, such as CSV files, databases, and APIs.
* *Data Transformation*: Provides multiple transformation strategies to clean, normalize, and preprocess data according to specific requirements.
* *Data Loading*: Supports saving processed data to different destinations, including databases and files.
* *Performance Measurement*: Tracks and logs the performance of each ETL step (CPU, Memory, Time), facilitating optimization of data workflows.
* *Benchmarking*: Tooling to compare performance across different libraries.

Design Patterns Used

The framework leverages the following design patterns to enhance flexibility and maintainability:

* *Abstract Factory*: Enables the creation of library-specific ETL components (e.g., Extractor, Transformer, Loader) without tying the code to a particular implementation. This supports switching between Pandas, Polars, DuckDB, Dask, and Vaex seamlessly.
* *Singleton*: Used in logging and configuration management.
* *Strategy*: Allows different transformation strategies to be applied to the data, making it easy to switch or add new transformation methods based on the requirements.
* *Template Method*: Defines the skeleton of ETL operations in abstract base classes.

## Architectural Critical Analysis (Status 2025)

This section documents the evolution and critique of the design patterns implemented in this project to handle modern ETL requirements.

### 1. Abstract Factory
- **Status**: ✅ **Effective and Necessary**.
- **Analysis**: Essential for decoupling the business logic (`ETLProcessor`) from specific implementations (Pandas, Polars, DuckDB). It allows adding new libraries without modifying the core processor logic. It is the core strength of this architecture.

### 2. Singleton (PerformanceMonitor)
- **Status**: ⚠️ **Refactored to Dependency Injection**.
- **Critique & Evolution**: Initially, `PerformanceMonitor` was implemented as a Singleton. This proved to be an **anti-pattern** for this specific use case because:
  - It prevented parallel execution of benchmarks (global state would mix metrics from different threads/processes).
  - It complicated testing by requiring manual state resets (`reset_metrics`).
- **Improvement**: We successfully refactored this to use **Dependency Injection**. The monitor is now instantiated at the top level and injected into the operations, ensuring isolation and better testability.

### 3. Strategy Pattern (Transformers)
- **Status**: ⚠️ **Functional but needs Optimization for Lazy Execution**.
- **Critique**: Implementing every small transformation (e.g., `CapitalizeName`) as a separate class creates significant boilerplate ("Class Explosion"). More critically, strictly applying transformations one-by-one (`transform(data)`) interferes with **Lazy Execution** engines like Dask, Polars, and Vaex. These engines prefer to build a complete query plan rather than executing intermediate steps.
- **Future Improvment**:
  - **Functional Registry/Pipeline**: Move from Class-based Strategy to a functional pipeline approach.
  - **Composite Pattern**: Allow bundling multiple operations into a single "Lazy" plan that is executed only at the `Load` specific step.
  - This refactoring is planned for the next iteration to fully unlock the performance of lazy evaluation engines.

### 4. Translation Service (Optional Component)
- **Status**: ⏸️ **Inactive in Benchmarks**.
- **Purpose**: Provides semantic normalization of column names (e.g., detecting `fecha_nacimiento` is Spanish and renaming it to `birth_date`) using external APIs (DeepL, Google Translate).
- **Analysis**: While useful for raw data ingestion, this component is **excluded from standard performance benchmarks**.
- **Reason**: It relies on external network latency (API calls), which introduces significant variability and bottlenecks unrelated to data processing performance. Including it would distort the comparison between libraries like Polars and DuckDB. Currently, clear integration is maintained primarily in the Pandas path.

## Project structure 

## Description of Directories and Files 

- **config/**: Contains configuration files for the ETL process.
  - `config_exp.yaml`: Configuration file for the ETL process.
- **data/**: Directory where experiment data files will be saved.
- **logs/**: Directory for log files.
- **src/**: Source code directory.
  - `__init__.py`: Makes the `src` directory a package.
  - **etl/**: Subdirectory for ETL-related code.
    - `etl_factory_provider.py`: Provides the appropriate ETL factory based on the library type.
    - `etl_processor.py`: Handles the ETL process with performance monitoring.
    - **library_pandas/**: Pandas implementation.
    - **library_polars/**: Polars implementation.
    - **library_duckdb/**: DuckDB implementation.
    - **library_dask/**: Dask implementation.
    - **library_vaex/**: Vaex implementation.
    - **utils/**: Utilities including monitoring and databases.
  - **experiments/**: Subdirectory for experiment-related code.
  - **scripts/**: Subdirectory for script files.
- **.gitignore**: Specifies intentionally untracked files to ignore.
- **docker-compose.yml**: Configuration file for Docker Compose.
- **Dockerfile-python**: Dockerfile.
- **README.md**: This file.
- **benchmark_etl.py**: Script to run performance benchmarks across libraries.

## Running the project

The system is ready to run under Docker or in a local virtual environment.

### Using Virtual Environment (Recommended)

1. Create and activate virtual environment:
   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   ```
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
3. Run benchmarks:
   ```bash
   python benchmark_etl.py
   ```

### Using Docker

After execute `$ docker-compose up --build`, enter into the terminal and play with the code.

## Step-by-Step System Execution Flow

This section details how the system works internally during a benchmark execution (`benchmark_etl.py`), explaining the data flow and architectural components involved.

### 1. Initialization & Configuration
- **Script Entry**: `benchmark_etl.py` is the entry point.
- **Data Generation**: The `create_dummy_data()` function generates a synthetic CSV (`input.csv`) with 4000 rows to ensure a consistent baseline for all libraries.
- **Metadata Loading**: A `MockMetadata` object is created. In a production scenario, this would load rules from `config_exp.yaml` (e.g., *'age': 'calculate_age'*).

### 2. Dependency Injection & Factory Selection
- **Library Loop**: The script iterates through the configured libraries: `['pandas', 'polars', 'duckdb', 'dask']`.
- **Factory Provider**: `ETLFactoryProvider.get_factory(library, metadata)` is called.
  - *Magic happens here*: If `library='polars'`, it returns an instance of `PolarsETLFactory`. This abstracts all subsequent operations; the system now uses Polars without explicitly knowing it.
- **Monitor Injection**: A `PerformanceMonitor` instance is created and injected into the `ETLProcessor`. This ensures that metrics for this specific run are isolated.

### 3. The ETL Pipeline Execution
The `processor.process()` method is called, orchestrating the following steps:

#### A. Extraction (Setup & Read)
- **Timer Start**: `monitor.measure_time("extraction")` starts tracking.
- **Abstraction**: `factory.get_extractor("csv", ...)` returns a library-specific extractor (e.g., `PolarsCSVExtractor`).
- **Execution**: `extractor.extract()` reads the `input.csv`.
  - *Pandas*: Reads the file into memory immediately.
  - *Vaex/Dask*: May lazy-load (map) the file without full reading.

#### B. Transformation (Strategy Pattern)
- **Factory**: The processor asks for `factory.get_transformer_factory()`.
- **Strategy Selection**: The system iterates through the defined transformation rules (e.g., `'simple'`, `'remove_duplicates'`).
- **Execution**: `transformer.transform(data)` is called for each rule.
  - *Polars/DuckDB*: These libraries execute optimized query plans or SQL-like expressions.
  - *Critique*: Currently, this step applies transformations sequentially.

#### C. Loading (IO Write)
- **Timer Start**: `monitor.measure_time("loading")` starts.
- **Abstraction**: `factory.get_loader("file", ...)` creates the specific loader (e.g., `DuckDBFileLoader`).
- **Execution**: `loader.load(data)` writes the final processed data to disk (`output_*.csv`).

### 4. Reporting
- Once the pipeline finishes, `processor_monitor.print_report()` calculates and outputs:
  - **Duration**: Time taken for each step.
  - **Memory Delta**: RAM consumption difference.
  - **Peak Memory**: Maximum RAM usage detected.

## Configuration Guide (`benchmark_config.yaml`)

The configuration file is the control plane of the experiment. It allows you to modify the behavior without touching Python code.

### 1. The `etl` Section (Process Logic)
Defines **HOW** the data is processed in the current run.
```yaml
etl:
  # List of libraries to benchmark sequentially. 
  # You can remove items to test specific libraries only (e.g., just ['polars'])
  libraries_to_test: [ 'pandas', 'polars', 'duckdb', 'dask' ] 
  
  source_type: csv
  # ... paths and params ...
  
  # Explicit transformations to apply in THIS experiment run
  transformations:
    general: [ 'simple', 'remove_duplicates' ]
    attributes:
        price: [ 'apply_discount' ]
```

### 2. The `metadata` Section (Data Contract)
Defines **WHAT** the data represents and its business invariants. Decouples business rules from technical execution.
```yaml
metadata:
  attributes: ['name', 'age', 'price']
  types:
    age: int # Data type validation
  rules:
    # Default business rule associated with this attribute.
    # The ETL system can use this if no explicit transformations are provided.
    age: calculate_age 
```

## Testing the ETL process

Run `python benchmark_etl.py` to verify all libraries and satisfy the benchmarking requirement.

## Next developments

- **pytest integration**
- **MinIO Connection**
- **Enhanced Visualizations for Benchmarks**
