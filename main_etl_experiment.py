

from scr.etl.etl_experiment import ETLExperiment
from scr.etl.utils.configuration.config_manager import ConfigManager
import sys

def main():

    file_sizes = [25000, 50000, 100000, 200000, 400000, 800000, 1600000]

    config_manager = ConfigManager(config_path='/home/ETL-PCP/config/config_exp.yaml')
    experiment = ETLExperiment(config_manager=config_manager)
    experiment.run(file_sizes)

if __name__ == "__main__":
    main()