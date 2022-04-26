# Importamos las librerias necesarias
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import count, avg, col, max, min, sum

# Comprobamos que estamos ejecutando el programa directamente
# y si el numero de argumentos es correcto
if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Uso: mnmcount <archivo>", file=sys.stderr)
        sys.exit(-1)
    # Construimos el SparkSession, si no existe se crea una instancia
    spark = (SparkSession
        .builder
        .appName("PythonMnMCount")
        .getOrCreate())
    
    # Obtenemos el nombre del fichero de datos de los argumentos de la linea de comandos
    mnm_file = sys.argv[1]

    # Leemos el fichero csv y lo convertimos a un Spark Data Frame
    # Le inferimos el esquema desde el fichero y especificamos que el archivo
    # tiene un header con los nombres de las columnas

    mnm_df = (spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(mnm_file))

    # Persistimos el DF en memoria, ya que se accedera a el muchas veces
    mnm_df.persist()

    # Usamos la API de DataFrames
    # 1. Selecionamos los campos "State", "Color" y "Count"
    # 2. Agrupamos por "State" y por "Color"
    # 3. Agregamos los "Count" de cada grupo en una columna "Total"
    # 4. Ordenamos por el "Total" en orden descendente
    count_mnm_df = (mnm_df
        .select("State", "Color", "Count")
        .groupBy("State", "Color")
        .agg(count("Count").alias("Total"))
        .orderBy("Total", ascending=False))

    # Mostramos los resultados
    count_mnm_df.show(n=60, truncate=False)

    # Ejemplo usando avg, calculamos la media de cada color entre todos los estados
    avg_mnm_color_df = (mnm_df
        .select("State", "Color", "Count")
        .groupBy("State", "Color")
        .agg(count("Count").alias("Total"))
        .groupBy("Color")
        .agg(avg("Total").alias("Average"))
        .orderBy("Average", ascending=True)
    )
    avg_mnm_color_df.show()

    # Ejemplo usando la cláusula where, filtramos los estados que empiezan por vocal
    vowel_states_df = (mnm_df
        .select("State")
        .distinct()
        .where(col("State").rlike("^[AEIOU]"))
    )
    vowel_states_df.show()

    # Ejemplo usando varias funciones de agregación. 
    # Se calcula el mínimo, máximo y media de votos y la suma total de votos por cada color
    stats_mnm_color_df = (mnm_df
        .select("State", "Color", "Count")
        .groupBy("Color", "State")
        .agg(count("Count").alias("Total"))
        .groupBy("Color")
        .agg(min("Total").alias("Min votes"), max("Total").alias("Max votes"), avg("Total").alias("Average votes"), sum("Total").alias("Sum of votes"))
    )
    stats_mnm_color_df.show()

    # Guardamos un dataframes como una vista temporal de SQL
    stats_mnm_color_df.createOrReplaceTempView("stats_color")
    spark.table("stats_color").show()

    # Detenemos el SparkSession
    spark.stop()