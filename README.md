# ETL-Performance-Comparison-Project
# ETL Project

This project is an ETL (Extract, Transform, Load) framework designed to work with multiple data processing libraries, specifically Pandas and Vaex. The framework includes functionalities for data extraction, transformation, and loading, and it supports performance measurement of these operations.

## Project structure 

 - `projectStructure.txt`

## Description of Directories and Files 

- **config/**: Contains configuration files for the ETL process.
  - `config_exp.yaml`: Configuration file for the ETL process.
- **data/**: Directory where experiment data files will be saved.
  - **experimentResults/**: Subdirectory for experiment results.
  - **experiment_dataset/**: Subdirectory for experiment datasets.
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
After cloning the repository, create all the non existing directories like /logs and logs/cronlogs

## Testing the ETL process

* 

## Next developments

- **pytest**
- **minioConnection**
- **login_process**
- **etl_loader_factory**
- **improving experiment**
