# -*- coding: utf-8 -*-
# Importamos las librerias necesarias
import sys
from pyspark.sql import SparkSession

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Uso: elquijote <archivo>", file=sys.stderr)
        sys.exit(-1)
    # Construimos el SparkSession, si no existe se crea una instancia
    spark = (SparkSession
        .builder
        .appName("PythonMnMCount")
        .getOrCreate())
    
    # Obtenemos el nombre del fichero de datos de los argumentos de la linea de comandos
    quijote_file = sys.argv[1]

    # Leemos el fichero y persistimos el DF que generamos
    quijote_df = (spark.read.text(quijote_file))
    quijote_df.persist()

    # Mostrar numero de filas
    quijote_df.count()

    # Mostrar 20 primeras filas
    quijote_df.show()

    # Mostrar 5 primeras filas
    quijote_df.show(5)

    # Mostrar 5 primeras filas sin truncar
    quijote_df.show(5, False)

    # Mostar filas sin truncar y en vertical
    quijote_df.show(truncate=False, vertical=True)

    # Mostrar 2 primeras filas truncadas y en vertical
    quijote_df.show(2, vertical=True)

    # Obtener primera fila del DF
    quijote_df.head()
    # Equivalente
    quijote_df.first()

    # Obtener 5 primeras filas del DF
    quijote_df.head(5)
    # Equivalente
    quijote_df.take(5)

    # Detenemos el SparkSession
    spark.stop()