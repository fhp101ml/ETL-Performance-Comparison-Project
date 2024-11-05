# ETL-Performance-Comparison-Project
# ETL Project
This project is an ETL (Extract, Transform, Load) framework designed to facilitate data extraction, transformation, and loading processes using multiple libraries, specifically Pandas and Vaex. The framework is built to support flexibility and extensibility, incorporating several design patterns to improve modularity and scalability. Additionally, it includes functionality for performance measurement, allowing users to monitor the execution time and resource usage for each ETL operation.
Key Features

*Data Extraction*: Allows data retrieval from various sources, such as CSV files, databases, and APIs.
*Data Transformation*: Provides multiple transformation strategies to clean, normalize, and preprocess data according to specific requirements.
*Data Loading*: Supports saving processed data to different destinations, including databases and files.
*Performance Measurement*: Tracks and logs the performance of each ETL step, facilitating optimization of data workflows.

Design Patterns Used

The framework leverages the following design patterns to enhance flexibility and maintainability:

*Abstract Factory*: Enables the creation of library-specific ETL components (e.g., Extractor, Transformer, Loader) without tying the code to a particular implementation. This supports switching between Pandas and Vaex without significant code changes.
*Singleton*: Ensures a single instance for performance tracking across the ETL operations, maintaining consistent performance metrics throughout the process.
*Strategy*: Allows different transformation strategies to be applied to the data, making it easy to switch or add new transformation methods based on the requirements.
## Project structure 


## Description of Directories and Files 

- **config/**: Contains configuration files for the ETL process.
  - `config_exp.yaml`: Configuration file for the ETL process.
- **data/**: Directory where experiment data files will be saved.
  - **experimentResults/**: Subdirectory for experiment results.
  - **experimentDataset/**: Subdirectory for experiment datasets.
- **logs/**: Directory for log files.
  - **cronlogs/**: Subdirectory for cron job logs.
    - `(cron job logs will be saved here)`: Placeholder for cron job logs.
- **scr/**: Source code directory.
  - `__init__.py`: Makes the `src` directory a package.
  - **etl/**: Subdirectory for ETL-related code.
    - `__init__.py`: Makes the `etl` directory a package.
    - `config_manager.py`: Manages the configuration.
    - `dataset_metadata.py`: Defines the metadata for the dataset.
    - `etl_factory_provider.py`: Provides the appropriate ETL factory based on the library type.
    - `etl_processor.py`: Handles the ETL process.
    - `pandas_etl_factory.py`: Factory for Pandas ETL operations.
    - `vaex_etl_factory.py`: Factory for Vaex ETL operations.
    - **transformers/**: Subdirectory for transformers.
      - `__init__.py`: Makes the `transformers` directory a package.
      - `pandas_transformers.py`: Transformers for Pandas.
      - `vaex_transformers.py`: Transformers for Vaex.
  - **experiments/**: Subdirectory for experiment-related code.
    - `__init__.py`: Makes the `experiments` directory a package.
    - `etl_experiment.py`: Defines the ETL experiment class.
    - `generate_datasets.py`: Script to generate datasets for experiments.
  - **scripts/**: Subdirectory for script files.
  - **utils/**: .
  - `main.py`: Main script to run the ETL experiments.
- **.gitignore**: Specifies intentionally untracked files to ignore.
- **docker-compose.yml**: Configuration file for Docker Compose.
- **Dockerfile-python**: Dockerfile to create a Docker image for the project.
- **README.md**: This file, providing an overview of the project.
- **projectStructure.txt**: Text file outlining the project structure.

## Running the project

The system is ready to run under Docker. 

After execute $docker-compose up --buid, enter into the terminal and play with the code

After cloning the repository, create all the non existing directories like /logs and logs/cronlogs

- Create database $python3 ~/scr/utils/sql_db_creation.py
- Create experiments_datasets $python3 ~/scr/experiments/generate_datasets.py

## Testing the ETL process

* 

## Next developments

- **pytest**
- **minioConnection**
- **login_process**
- **etl_loader_factory**
- **improving experiment**
