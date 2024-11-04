import vaex

# Ruta del archivo HDF5 generado previamente
file_path = '/home/ETL-PCP/data/experiments_dataset/experiment_data_25000_transformation_test.hdf5'


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