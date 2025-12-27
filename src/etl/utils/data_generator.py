import pandas as pd
import numpy as np
from faker import Faker
import random
import os

def generate_complex_dataset(path, num_rows=1000):
    """
    Generates a complex dataset with diverse data types for ETL benchmarking.
    Fields:
    - id: int (sequence)
    - name: str (dirty)
    - birth_date: date (with nulls)
    - email: str
    - phone: str (dirty formats)
    - zip_code: str (to join with external data)
    - lat: float
    - lon: float
    - salary: float (with nulls for imputation)
    - category: categorical (Low, Medium, High)
    - last_login: datetime
    """
    fake = Faker('es_ES')
    Faker.seed(42)
    random.seed(42)
    
    print(f"Generating complex dataset with {num_rows} rows...")
    
    # Pre-generate lists for speed
    data = {
        'id': range(1, num_rows + 1),
        'name': [fake.name() if random.random() > 0.05 else fake.name().lower() for _ in range(num_rows)],
        'birth_date': [fake.date_of_birth(minimum_age=18, maximum_age=90) if random.random() > 0.1 else None for _ in range(num_rows)],
        'email': [fake.email() for _ in range(num_rows)],
        'zip_code': [str(random.choice([28001, 8001, 46001, 41001, 48001]) + random.randint(0, 49)).zfill(5) for _ in range(num_rows)],
        'current_lat': [float(fake.latitude()) for _ in range(num_rows)],
        'current_lon': [float(fake.longitude()) for _ in range(num_rows)],
        'salary': [round(random.uniform(20000, 120000), 2) if random.random() > 0.15 else None for _ in range(num_rows)],
        'category': [random.choice(['A', 'B', 'C']) for _ in range(num_rows)],
        'last_login': [fake.date_time_this_year() for _ in range(num_rows)]
    }
    
    df = pd.DataFrame(data)
    
    # Introduce some dirty data in 'name' (accents, casing mixed handled by generator logic slightly)
    
    os.makedirs(os.path.dirname(path), exist_ok=True)
    
    # Save as CSV
    df.to_csv(path, sep=';', index=False)
    print(f"✅ Complex dataset generated at {path}")
    
    # Save as Parquet
    parquet_path = path.replace('.csv', '.parquet')
    df.to_parquet(parquet_path, index=False)
    print(f"✅ Complex dataset generated at {parquet_path}")
    
    # Save as JSON
    json_path = path.replace('.csv', '.json')
    df.to_json(json_path, orient='records', indent=4)
    print(f"✅ Complex dataset generated at {json_path}")

if __name__ == "__main__":
    generate_complex_dataset("data_test/complex_input.csv", 5000)
