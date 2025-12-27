import sqlite3
from sqlite3 import IntegrityError

# Conectar a la base de datos (se creará si no existe)
database_path = '/home/ETL-PCP/data/dataBaseTest/testing_functions.db'
conn = sqlite3.connect(database_path)
cursor = conn.cursor()

# Crear la tabla 'usuarios'
cursor.execute('''
CREATE TABLE IF NOT EXISTS usuarios (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    nombre TEXT NOT NULL,
    edad INTEGER NOT NULL,
    correo TEXT NOT NULL UNIQUE
)
''')

# Insertar algunos datos en la tabla 'usuarios'
cursor.executemany('''
INSERT INTO usuarios (nombre, edad, correo)
VALUES (?, ?, ?)
''', [
    ('Juan Pérez', 30, 'juan.perez@example.com'),
    ('María López', 25, 'maria.lopez@example.com'),
    ('Carlos Sánchez', 35, 'carlos.sanchez@example.com'),
    ('Ana González', 28, 'ana.gonzalez@example.com')
])

# Guardar (commit) los cambios
conn.commit()

# Consultar y mostrar los datos de la tabla 'usuarios'
cursor.execute('SELECT * FROM usuarios')
rows = cursor.fetchall()
for row in rows:
    print(row)

# Cerrar la conexión
conn.close()
