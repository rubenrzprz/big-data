# Transformaciones VS Acciones

Las transformaciones en Spark son una operación que transforma un DataFrame en un nuevo, sin alterar los datos originales, confiriéndoles así inmutabilidad.

En Spark, las transformaciones son *lazy evaluated*, es decir, no se computan hasta que no se invoca una acción.

Ejemplos *(Capítulo 2 - Página 30)*:

```python
>>> strings  = spark.read.text("../README.md") #Transformación
>>> filtered = strings.filter(strings.value.contains("Spark")) #Transformación
>>> filtered.count() #Acción
20
```

No se ejecuta nada hasta que se llama a la acción *count()*.
