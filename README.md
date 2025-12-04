# 1. Conxunto de datos utilizado 📌 

Para este proxecto utilizouse o conxunto de datos netflix_titles.csv, descargado de Kaggle, en formato CSV.
O ficheiro inclúe varias columnas e máis de 8810 filas, o que permite probar a transferencia de datos entre distintos sistemas de bases de datos.


# 2. Como executar os servizos Docker 🐳 

Hai de ter Docker instalados no sistema.

Situámonos no directorio do proxecto onde está o ficheiro docker-compose.yml.

Executamos o seguinte comando:
docker-compose up -d

Isto iniciará os catro servizos:

* MySQL — porto 3306

* Cassandra — porto 9042

* Redis — porto 6379

* MongoDB — porto 27017


# 3. Como reproducir os notebooks 📓 

O repositorio inclúe catro Notebooks, correspondentes ás distintas fases do traballo.

### Notebook 1 — Carga de datos en MySQL

Pasos:

* Conectarse ao contedor MySQL.

* Cargar o CSV no esquema.

* Crear a táboa correspondente.

* Verificar con consultas SQL simples.

Librerías:

* Pandas

* sqlalchemy

* matplotlib.pyplot


### Notebook 2 — Lectura desde MySQL e inserción en Cassandra

Pasos:

* Conectarse ao contedor MySQL.

* Lector dos datos de MySQL nun DataFrame.

* Conexión con Cassandra.

* Creación de keyspace e táboa.

* Inserción dos datos no modelo Cassandra.

Librerías:

* Pandas

* sqlalchemy

* cassandra.cluster

* cassandra.query

* matplotlib.pyplot


### Notebook 3 — Exportación de columnas de Cassandra a Redis

Pasos:

* Conexión con Cassandra.

* Consulta de dúas columnas: unha como clave e outra como valor.

* Conexión con Redis.

* Inserción no almacenamento clave-valor Redis.

* Comprobación de consultas a Redis.

Librerías:

* cassandra.cluster

* redis


### Notebook 4 — Exportación de datos de MySQL a MongoDB

Pasos:

* Conectarse ao contedor MySQL.
  
* Lectura da táboa completa desde MySQL.

* Conexión con MongoDB.

* Creación dunha colección.

* Inserción de documentos baseados en filas.

* Consultas simples (find, count_documents).

Librerías:

* Pandas

* sqlalchemy

* matplotlib.pyplot
