# Multi-Engine ETL Framework

This project creates a **Engine-Agnostic ETL Framework**, allowing users to define data transformations once and execute them using multiple high-performance data libraries: **Pandas, Polars, DuckDB, and Dask**.

It abstracts the specific implementation details (Extract, Transform, Load) using design patterns like **Abstract Factory** and **Strategy**, making it trivial to switch engines based on data volume or environment.

---

## 🚀 Key Features

*   **Engine Agnostic**: Switch between `pandas`, `polars`, `duckdb`, or `dask` by changing a single config string.
*   **Format Flexibility**: Full support for **CSV, Parquet, and JSON** inputs/outputs effectively across all engines.
*   **Business Logic decoupling**: Define *what* to do (Metadata/Rules) separate from *how* to do it (Implementation).
*   **Performance Benchmarking**: Includes a full suite to stress-test your data pipeline against different engines.
*   **Observability**: Built-in performance monitoring (Time & Memory).

---

## 📦 Installation

This framework is packaged as a standard Python library.

### 1. Requirements
*   Python 3.10+
*   Virtual Environment (recommended)

### 2. Install
```bash
# Create venv
python -m venv .venv
source .venv/bin/activate

# Install dependencies (Streamlit, Plotly, etc.)
pip install -r requirements.txt

# Install the library in editable mode
pip install -e .
```

---

## 🛠️ Usage (Library)

You can use `my_etl_framework` in any Python script or Airflow DAG.

```python
from etl_framework.etl_factory_provider import ETLFactoryProvider
from etl_framework.etl_processor import ETLProcessor
from etl_framework.utils.monitoring import PerformanceMonitor

# 1. Define Business Rules (Data Contract)
class CustomerTransactionsLogic:
    """
    Defines the Schema and Business Rules for the dataset.
    This acts as a 'Source of Truth', decoupled from the execution engine.
    """
    def get_attributes(self):
        """Defines the expected column schema"""
        return [
            'transaction_id', 
            'customer_name', 
            'transaction_date', 
            'amount', 
            'category'
        ]

    def get_types(self):
        """Defines strict data types for validation/casting"""
        return {
            'transaction_id': int,
            'customer_name': str,
            'transaction_date': 'datetime',
            'amount': float,
            'category': str
        }

    def get_rules(self):
        """
        Maps attributes to specific transformation strategies.
        These abstract names (e.g., 'impute_mean') are implemented 
        optimizedly in each specific library (Pandas, Polars, etc).
        """
        return {
            'customer_name': 'capitalize_first_letter',  # Standardization
            'amount': 'impute_mean',                     # Data Cleaning
            'transaction_date': 'days_since',            # Feature Engineering
            'category': 'fill_unknown'                   # Handling Missing Data
        }

# 2. Select Engine ('polars', 'duckdb', 'pandas', 'dask')
ENGINE = 'polars'

# 3. Instantiate Factory & Processor with the Business Logic
metadata = CustomerTransactionsLogic()
factory = ETLFactoryProvider.get_factory(ENGINE, metadata)
processor = ETLProcessor(factory, ENGINE, monitor=PerformanceMonitor())

# 4. Run Pipeline with specific Execution Parameters
processor.process(
    source_type='csv',
    extractor_params={'file_path': 'data/input_transactions.csv', 'separator': ';'},
    destination_type='parquet',
    loader_params={'output_path': 'data/processed_transactions.parquet'},
    
    # We apply the rules defined in our Metadata contract
    transformations={
        'attributes': {
            'customer_name': ['capitalize_first_letter'],
            'amount': ['impute_mean'],
            'transaction_date': ['days_since']
        }
    }
)
```

---

## 📊 Benchmarking Dashboard

This repository includes a powerful Streamlit Dashboard to compare engine performance.

### Running the Dashboard
```bash
streamlit run dashboard.py
```

### Features
1.  **Configuration**: GUI to select Libraries, Input/Output formats, and Data Size.
2.  **Execution**: Runs real benchmarks in the background.
3.  **Analysis**:
    *   Compare **Execution Time** vs **Memory Usage**.
    *   Analyze breakdown by phase (Setup, Extract, Transform, Load).
    *   **Save Reports**: Export interactive HTML charts of your results.

---

## 📂 Project Structure

```
├── my_etl_framework/       # Core Library (Main Package)
│   ├── etl_factory_provider.py
│   ├── etl_processor.py
│   ├── library_*/          # Engine implementations (Strategies)
│   └── utils/              # Monitoring & Helpers
├── dashboard.py            # Streamlit GUI App
├── benchmark_etl.py        # CLI Benchmarking Script
├── etl_pipeline_demo.py    # Example Production Pipeline script
├── experiments_results/    # Folder for benchmark artifacts & logs
├── setup.py                # Pip package configuration
└── requirements.txt        # Dependencies
```

## 🏗️ Design Patterns

*   **Abstract Factory**: Decouples the client from specific ETL implementations (e.g., `PolarsETLFactory`).
*   **Strategy**: Encapsulates transformation algorithms (`PandasImputeMeanTransformer`, `DuckDBImputeMeanTransformer`).
*   **Dependency Injection**: Injects Metadata and Monitors into the processor for better testing and isolation.
