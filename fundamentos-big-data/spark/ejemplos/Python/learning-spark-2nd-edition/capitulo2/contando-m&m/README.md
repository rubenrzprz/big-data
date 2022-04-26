# Contando M&Ms

El siguiente programa lee un fichero con el formato de línea ```<state, mnm_color, count>``` como un Dataframe de Spark. Se trabajan los siguientes aspectos:

- Leer un fichero *.csv* con *header* como Dataframe e inferirle el esquema desde el fichero
- Funciones de la API de SQL *select*, *where*, *groupBy*, *orderBy*
- Funciones de agregación (*count*, *sum*, *min*, *max*, *avg*)
- Creación de vistas temporales

Para ejecutar el programa se necesita el siguiente [csv](https://github.com/databricks/LearningSparkV2/blob/master/chapter2/py/src/data/mnm_dataset.csv).

- [Notebook](./mnm_notebook_python.dbc)
- [App](src/mnmcount.py) 