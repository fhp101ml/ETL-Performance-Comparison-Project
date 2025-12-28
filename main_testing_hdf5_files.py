import vaex

import os

# Ruta del archivo HDF5 generado previamente
base_dir = os.path.dirname(os.path.abspath(__file__))
file_path = os.path.join(base_dir, 'data_test', 'experiment_data_25000_transformation_test.hdf5')


try:
    # Abrir el archivo HDF5 con Vaex
    df_vaex = vaex.open(file_path)
    print(df_vaex)
    print(type(df_vaex))

    # Convertir el DataFrame de Vaex a un DataFrame de Pandas
    df = df_vaex.to_pandas_df()
    print(type(df))

except Exception as e:
    print(f"Error al abrir el archivo HDF5 con Vaex: {e}")