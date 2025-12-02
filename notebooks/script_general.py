
import pandas as pd
from sqlalchemy import create_engine, text 
mysql_user = "user"
mysql_password = "password"
mysql_host = "127.0.0.1"
mysql_port = "3306"
mysql_db = "testdb"
engine = create_engine(f"mysql+mysqlconnector://{mysql_user}:{mysql_password}@{mysql_host}:{mysql_port}/{mysql_db}") 

df = pd.read_csv("data/netflix_titles.csv") 
tabla_nombre = "Netflix" 
df.to_sql(name=tabla_nombre, con=engine, if_exists='replace', index=False)

print(f"Datos cargados en la tabla '{tabla_nombre}' correctamente.") 

with engine.connect() as conn:
    result = conn.execute(text(f"SELECT COUNT(*) FROM {tabla_nombre};"))
    count = result.scalar()
    print(f"Número de filas cargadas: {count}")

    result = conn.execute(text(f"SELECT * FROM {tabla_nombre} LIMIT 5;"))
    for row in result:
        print(row)

    pais = "Spain"
    result = conn.execute(text(f"""
                SELECT title, country
                FROM {tabla_nombre}
                WHERE country = :pais
                LIMIT 10;
            """), {"pais": pais})
    for row in result:
        print(row)

    query_todos_generos = text(f"""
             SELECT listed_in
             FROM {tabla_nombre};
        """)

    result = conn.execute(query_todos_generos)
    df_raw_generos = pd.DataFrame(result.fetchall(), columns=result.keys()) 
    df_split = df_raw_generos['listed_in'].str.split(', ', expand=True).stack()
    df_split = df_split.str.strip()

    conteo_generos = df_split.value_counts().head(10)

    df_generos_individuales = conteo_generos.reset_index()
    df_generos_individuales.columns = ['genero', 'total']

    print ("\n Top 10 Géneros Individuales más comunes son: ")
    print(df_generos_individuales)

import matplotlib.pyplot as plt
plt.style.use('seaborn-v0_8-darkgrid')

fig, ax = plt.subplots(figsize=(10, 6))
ax.barh(df_generos_individuales['genero'], df_generos_individuales['total'])

ax.set_title('Top 10 Géneros Individuales más Comunes', fontsize=16, fontweight='bold')
ax.set_xlabel('Número Total de Títulos (por ocurrencia)', fontsize=12)
ax.set_ylabel('Género Individual', fontsize=12)

ax.invert_yaxis() 
for i, (total, genero) in enumerate(zip(df_generos_individuales['total'], df_generos_individuales['genero'])):
    ax.text(total + 10, i, str(total), va='center')

plt.tight_layout()
plt.show()

# CONEXIÓN A MYSQL
mysql_user = "user"
mysql_password = "password"
mysql_host = "127.0.0.1"
mysql_port = "3306"
mysql_db = "testdb"
table_name = "mi_tabla"

engine = create_engine(f"mysql+mysqlconnector://{mysql_user}:{mysql_password}@{mysql_host}:{mysql_port}/{mysql_db}")

# GUARDAMOS LOS DATOS SACADOS DE LA TABLA DE MYSQL EN UN DATAFRAME DE PANDAS
df = pd.read_sql(f"SELECT * FROM {table_name}", engine)
from cassandra.cluster import Cluster

# CONEXIÓN AL CLUSTER DE CASSANDRA
cluster = Cluster(['localhost'], port=9042)
session = cluster.connect()

# CREACIÓN DEL KEYSPACE SI NO EXISTE
session.execute("""
CREATE KEYSPACE IF NOT EXISTS netflix_keyspace
WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor': 1 }
""")

# SE SELECCIONA LA KEYSPACE
session.set_keyspace('netflix_keyspace')

# CREAR LA TABLA SI NO EXISTE
session.execute("""
CREATE TABLE IF NOT EXISTS netflix_table (
    show_id TEXT PRIMARY KEY,
    type TEXT,
    title TEXT,
    director TEXT,
    cast TEXT,
    country TEXT,
    date_added TEXT,
    release_year INT,
    rating TEXT,
    duration TEXT,
    listed_in TEXT,
    description TEXT
)
""")
from cassandra.query import PreparedStatement

# PREPARACIÓN DE LA QUERY PARA INSERTAR LOS DATOS ALMACENADOS EN EL DATAFRAME SACADO DE MYSQL. SE APLICAN 
# LOS INTERROGANTES PARA PASARLE LA INFORMACIÓN ALMACENADA EN EL DATAFRAME.
insert_query = session.prepare("""
INSERT INTO netflix_table (
    show_id, type, title, director, cast, country,
    date_added, release_year, rating, duration,
    listed_in, description
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
""")

# SE LE AÑADEN LOS DATOS A LA QUERY HACIENDO UNA ITERACIÓN POR FILA DEL DATASET.
for _, row in df.iterrows():
    session.execute(insert_query, (
        row['show_id'], row['type'], row['title'], row['director'],
        row['cast'], row['country'], row['date_added'], row['release_year'],
        row['rating'], row['duration'], row['listed_in'], row['description']
    ))
import matplotlib.pyplot as plt

# SE GUARDAN LOS DATOS DE LA TABLA EN UN DATAFRAME DE PANDAS PARA CONSULTARLOS MEJOR.
query = "SELECT * FROM netflix_table"
rows = session.execute(query)
netflix_cassandra = pd.DataFrame(rows)

# SE FILTRAN LOS DIRECTORES Y LUEGO SE GUARDAN LOS 10 QUE MÁS TÍTULOS TIENEN.
directores = netflix_cassandra['director'].dropna().str.split(',').explode().str.strip()
top_directores = directores.value_counts().head(10)

# GRÁFICA PARA MOSTRAR LOS DIRECTORES GUARDADOS EN UN GRÁFICO DE BARRAS.
top_directores.plot(kind='bar')
plt.title("Top 10 directores con más títulos en Netflix")
plt.xlabel("Director")
plt.ylabel("Número de títulos")
plt.show()

# ¡¡IMPORTANTE CERRAR EL CLUSTER DESPUÉS DE TRABAJAR CON ÉL!!
cluster.shutdown()

from cassandra.cluster import Cluster
import redis

cluster = Cluster(['127.0.0.1'], port=9042)   # Cambia por tu IP
session = cluster.connect('netflix_keyspace')  # Tu keyspace 

query = "SELECT show_id, description FROM netflix_table;"   # Cambia por tus columnas
rows = session.execute(query)

r = redis.Redis(host='127.0.0.1', port=6379, db=0)

for row in rows:
    key = str(row.show_id)         # clave
    value = row.description        # valor
    r.set(key, value)
    print(f"Guardado en Redis → {key}: {value}")

print("\nVerificación en Redis:")
for test_key in r.keys():
    print(test_key.decode(), "→", r.get(test_key).decode())
    
engine = create_engine(f"mysql+mysqlconnector://{mysql_user}:{mysql_password}@{mysql_host}:{mysql_port}/{mysql_db}")
df = pd.read_sql(text("SELECT * FROM mi_tabla"), con=engine)

table_name = "mi_tabla"

with engine.connect() as conn:
    # Contar filas
    result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name};"))
    count = result.scalar()
    print(f"Número de filas cargadas: {count}")

    # Mostrar as primeiras 5 filas
    result = conn.execute(text(f"SELECT * FROM {table_name} LIMIT 5;"))
    for row in result:
        print(row)

from pymongo import MongoClient
#Crear conexión a MongoDB
mongo_user = "root"
mongo_password = "rootpassword"
mongo_host = "127.0.0.1"
mongo_port = "27017"

mongo_client = MongoClient(f"mongodb://{mongo_user}:{mongo_password}@{mongo_host}:{mongo_port}/")

mongo_dns = mongo_client["testdb"]
mongo_collection = mongo_dns["mi_tabla"]
mongo_collection.insert_many(df.to_dict('records'))


print(mongo_collection.find_one({"show_id": "s2"}))
print("\nNúmero de documentos en la colección:", mongo_collection.count_documents({}))
print("\nPrimeros 5 documentos en la coleccion:")
for doc in mongo_collection.find().limit(5):
    print(doc)

import matplotlib.pyplot as plt

mas_titulos = df['country'].value_counts().head(5)

mas_titulos.plot(kind='pie', startangle=90)
plt.title("5 países con máis títulos")
plt.show()

#Cerrar la conexión
mongo_client.close()