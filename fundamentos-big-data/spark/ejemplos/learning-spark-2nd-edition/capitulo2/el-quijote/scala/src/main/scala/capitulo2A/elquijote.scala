package main.scala.capitulo2A

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object QuijoteCount {
    def main(args: Array[String]) {
        //Comprobamos si el numero de argumentos es correcto
        if(args.length < 1) {
            print("Uso: QuijoteCount <archivo_el_quijote.txt>")
            sys.exit(1)
        }
        //Construimos el SparkSession, si no existe se crea una instancia
        val spark = SparkSession
            .builder
            .appName("QuijoteCount")
            .getOrCreate()
        
        // Obtenemos el nombre del fichero de datos de los argumentos de la linea de comandos
        val quijoteFile = args(0)

        // Leemos el fichero txt, lo convertimos a un Dataset y lo persistimos
        val quijoteDs = spark.read.text(quijoteFile)
        quijoteDs.persist()
        
        // Mostrar numero de filas
        quijoteDs.count()

        // Mostrar 20 primeras filas
        quijoteDs.show()
    
        // Mostrar 5 primeras filas
        quijoteDs.show(5)
    
        // Mostrar 5 primeras filas sin truncar
        quijoteDs.show(5, false)
    
        // Mostar filas sin truncar
        quijoteDs.show(false)
    
        // Obtener primera fila del DF
        quijoteDs.head()
        // Equivalente
        quijoteDs.first()
    
        // Obtener 5 primeras filas del DF
        quijoteDs.head(5)
        // Equivalente
        quijoteDs.take(5)
            
        // Detenemos el SparkSession
        spark.stop()
    }
}