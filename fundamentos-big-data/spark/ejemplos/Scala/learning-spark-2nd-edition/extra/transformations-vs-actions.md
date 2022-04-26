# Transformaciones VS Acciones

Las transformaciones en Spark son una operación que transforma un DataFrame en un nuevo, sin alterar los datos originales, confiriéndoles así inmutabilidad.

En Spark, las transformaciones son *lazy evaluated*, es decir, no se computan hasta que no se invoca una acción.

Ejemplos *(Capítulo 2 - Página 30)*:

```scala
scala> import org.apache.spark.sql.functions._
scala> val strings = spark.read.text("../README.md") //Transformación
scala> val filtered = strings.filter(col("value").contains("Spark")) //Transformación
scala> filtered.count() //Acción
res0: Long = 20
```

No se ejecuta nada hasta que se llama a la acción *count()*.
