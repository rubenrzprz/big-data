# Impala

## ¿Qué es Imapala?

- Impala es un motor SQL MPP (procesamiento masivo en paralelo) pensado para operar sobre grandes volúmenes de datos.
- Se ejecuta en clusters Hadoop, lo que le permite ejecutar queries sobre HDFS o Hbase y leer y escribir datos sobre ficheros con tipos de datos de Hadoop.
- Fue originalmente desarrollada por Cloudera, aunque hoy en día es open source y pertenece al ASF (Apache Software Foundation).

## Características

- Ejecuta las queries sobre el cluster, sin ejecutar MapReduce para procesarlas, por lo que puede llegar a ser entre 5 y 20 veces más rápido que Hive o Pig.
- Es extensible mediante Java.
- Es soportado por muchas herramientas de Business Intelligence, al igual que Hive.

## Impala vs Hadoop

### Similitudes

- Ambas emplean variantes de SQL.
- Comparten el mismo metastore y data warehouse en el clúster.
  - La estructura y la localización de las tablas se definen cuando se crean y se almacenan en el metastore.
  - Los datos se guardan por defecto en el directorio ```/user/hive/warehouse```, donde cada tabla es un subdirectorio.
- Las queries operan sobre tablas, al igual que en un sistema de base de datos relacional.

### Diferencias

- Hive fue una de las primeras herramientas del ecosistema Hadoop, por lo que está más desarrollada y ofrece muchas más funcionalidades que Impala, por ejemplo:
  - Ficheros con tipos de datos a medida
  - Tipo de datos DATE
  - Funciones XML y JSON
  - Sampling (ejecutar queries sólo sobre un subset de la tabla)
- Hive está basado en lotes y MapReduce, mientras que Impala se basa en MPP.
- Impala, al contrario que Hive, no es tolerante a fallos, por lo que si ocurre un error durante la ejecución de una query, se debe lanzar otra vez.

#### Tabla comparativa

|Aspecto|Hive|Impala|
|-|:-:|:-:|
|Tipos complejos|Sí|No|
|Latencia|Alta|Baja|
|Procesamiento en paralelo|No|Sí|
|Soporta MapReduce|Sí|No|
|Tolerante a fallos|Sí|No|
|Tipo de DB|Batch MapReduce|MPP|
|Conversión de código|En tiempo de compilación|En tiempo de ejecución|

### ¿Cuándo usar Hive y cuándo usar Impala?

Hive está más enfocado a proyectos grandes, que procesen volúmenes muy grandes de datos y de diversos tipos y orígenes. Además, está pensado para que se pueda escalar el sistema a medida que crezca el proyecto y la tolerancia a fallos provee mayor seguridad a la hora de procesar los datos.

Por otro lado, Impala está pensado para ejecutar queries más sencillas y obtener los resultados rápidamente, por lo que es mejor para explorar los datos de manera interactiva.

## Funcionamiento de Impala

### Demonios de Impala

#### Nodo maestro

- *State Store*: comprueba el estado de los demonios Impala de los nodos esclavos.
- *Catalog*: trasmite los cambios en los metadatos a todos los demonios del clúster.

#### Nodos esclavos

- *Impala Daemon*: planifica las queries y cachean los metadatos.

### Funcionamiento

1. El cliente se conecta con un demonio Impala, llamado *coordinador*.
2. El *coordinador* pide una lista de demonios Impala activos al *State Store*
3. El *coordinador* distribuye la query entre los demonios disponibles.
4. Cada demonio devuelve el resultado al *coordinador*, que los combina y los muestra al cliente.

### Cacheo de metadatos

Al inicio de la sesión, los demonios Impala cachean los metadatos de definiciones de tablas y localización de los bloques HDFS desde el metastore.

Cuando un demonio Impala cambia el metastore, se lo notifica al servicio *Catalog*, que se encarga de notificar los cambios al resto de demonios Impala para que actualicen su caché.

Si los cambios en los metadatos se realizan desde fuera de Impala (mediante Hive, Pig, Hue Metadata Manager...), estos cambios no serán conocidos por Impala, por lo que no actualizará la caché de los demonios. En estos casos, se debe refrescar manualmente los metadatos de la caché de Impala.

Según los cambios realizados, se deben tomar las siguientes acciones:

|Cambio externo|Acción|Efecto en la caché|
|:-:|:-:|-|
|Añadida una nueva tabla|INVALIDATE METADATA|Marca la caché como *stale* y la recarga a medida que sea necesario|
|Esquema de una tabla modificado o añadidos datos a una tabla|REFRESH \<tabla>|Recarga los metadatos de la tabla y recarga las ubicaciones de los bloques HDFS de los nuevos datos|
|Cambios muy drásticos en los datos de una tabla|INVALIDATE METADATA \<tabla>|Marca los metadatos de esa tabla como *stale* y los recarga cuando sea necesario|

## Adicional: optimización de queries

Se pueden optimizar la ejecución de las queries informando de las características (STATS) de las tablas involucradas. Es recomendable ejecutarlo justo después de cargar las tablas y cuando se modifique más del 20% de una tabla.

Ejemplo:

```Impala
COMPUTE STATS orders;
COMPUTE STATS order_details;
SELECT COUNT(o.order_id)
    FROM orders o
    JOIN order_details d
    ON (o.order_id = d.order_id)
    WHERE YEAR(o.order_date) = 2008;
```

Estas características se pueden ver mediante los comandos:

```Impala
SHOW TABLE STATS <tabla>;
SHOW COLUMN STATS <tabla>;
```
