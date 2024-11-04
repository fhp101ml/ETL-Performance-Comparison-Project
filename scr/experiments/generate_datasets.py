import pandas as pd
import numpy as np
from faker import Faker


def generate_datasets(sizes, base_path):
    fake = Faker()
    Faker.seed(0)
    np.random.seed(0)

    for size in sizes:
        prices = np.random.uniform(10, 1000, size)
        ages = np.random.randint(18, 70, size)
        names = [fake.name() for _ in range(size)]

        data = pd.DataFrame({'price': prices, 'age': ages, 'name': names})
        # file_path = base_path.replace('\\', '/')  # Replacing backslashes with forward slashes

        data.to_csv(f'{base_path}/experiment_data_{size}.csv', index=False)


if __name__ == "__main__":
    sizes = [25000, 50000, 100000, 200000, 400000, 800000, 1600000, 3200000]
    # sizes = [25000, 50000, 75000, 150000]
    base_path = r'/home/ETL-PCP/data/experiments_dataset'

    generate_datasets(sizes, base_path)
