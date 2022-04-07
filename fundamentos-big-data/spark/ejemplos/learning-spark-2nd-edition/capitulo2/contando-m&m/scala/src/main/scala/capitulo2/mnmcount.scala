package main.scala.capitulo2

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object MnMcount {
    def main(args: Array[String]) {
        //Comprobamos si el numero de argumentos es correcto
        if(args.length < 1) {
            print("Uso: MnMcount <archivo_mnm_dataset>")
            sys.exit(1)
        }
        //Construimos el SparkSession, si no existe se crea una instancia
        val spark = SparkSession
            .builder
            .appName("MnMCount")
            .getOrCreate()
        
        // Obtenemos el nombre del fichero de datos de los argumentos de la linea de comandos
        val mnmFile = args(0)

        // Leemos el fichero csv y lo convertimos a un Spark Data Frame
        // Le inferimos el esquema desde el fichero y especificamos que el archivo
        // tiene un header con los nombres de las columnas
        val mnmDf = spark.read.format("csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .load(mnmFile)
        
        // Usamos la API de DataFrames
        // 1. Selecionamos los campos "State", "Color" y "Count"
        // 2. Agrupamos por "State" y por "Color"
        // 3. Agregamos los "Count" de cada grupo en una columna "Total"
        // 4. Ordenamos por el "Total" en orden descendente
        val countMnmDf = mnmDf
            .select("State", "Color", "Count")
            .groupBy("State", "Color")
            .agg(count("Count").alias("Total"))
            .orderBy(desc("Total"))
        
        // Mostramos los 60 primeros resultados
        countMnmDf.show(60)

        //Ejemplo usando avg, calculamos la media de cada color entre todos los estados
        val avgMnmColorDf = mnmDf
            .select("State", "Color", "Count")
            .groupBy("State", "Color")
            .agg(count("Count").alias("Total"))
            .groupBy("Color")
            .agg(avg("Total").alias("Average"))
            .orderBy(asc("Average")
        )
        avgMnmColorDf.show()

        // Ejemplo usando la cláusula where, filtramos los estados que empiezan por vocal y contamos los votos
        val countVowelStatesDf = mnmDf
            .select("State", "Count")
            .where(col("State").rlike("^[AEIOU]"))
            .groupBy("State")
            .agg(count("Count").alias("Total"))
        countVowelStatesDf.show()
        
        // Ejemplo usando varias funciones de agregación (min, max, avg, sum)
        val statsMnmColorDf = mnmDf
            .select("State", "Color", "Count")
            .groupBy("Color", "State")
            .agg(count("Count").alias("Total"))
            .groupBy("Color")
            .agg(min("Total").alias("Min votes"), max("Total").alias("Max votes"), avg("Total").alias("Average votes"), sum("Total").alias("Sum of votes"))
        statsMnmColorDf.show()
        
        // Guardamos un dataframes como una vista temporal de SQL y ejcutamos consultas en ella
        statsMnmColorDf.createOrReplaceTempView("stats_color")
        spark.table("stats_color").select("*").where(col("Color").isin("Orange", "Red")).show()
            
        // Detenmos el SparkSession
        spark.stop()
    }
}