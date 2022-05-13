# Práctica Hive + Impala + HDFS + Spark

Partiendo del [CSV de datos de Padrón de Madrid](https://datos.madrid.es/egob/catalogo/200076-1-padron.csv), realizar los siguientes apartados:

- [Práctica Hive + Impala + HDFS + Spark](#práctica-hive--impala--hdfs--spark)
  - [1. Creación de tablas en formato texto](#1-creación-de-tablas-en-formato-texto)
  - [2. Investigamos el formato columnar parquet](#2-investigamos-el-formato-columnar-parquet)
  - [3. Juguemos con Impala](#3-juguemos-con-impala)
  - [4. Sobre tablas particionadas](#4-sobre-tablas-particionadas)
  - [5. Trabajando con tablas en HDFS.](#5-trabajando-con-tablas-en-hdfs)
  - [6. Un poquito de Spark](#6-un-poquito-de-spark)
  - [7. ¿Y si juntamos Spark y Hive?](#7-y-si-juntamos-spark-y-hive)
    - [Crear base de datos](#crear-base-de-datos)
    - [Leer datos y crear tabla](#leer-datos-y-crear-tabla)
    - [Consultas](#consultas)
    - [Crear las tablas particionadas](#crear-las-tablas-particionadas)
    - [Consultas sobre tablas particionadas](#consultas-sobre-tablas-particionadas)

## 1. Creación de tablas en formato texto

1. Crear base de datos "datos_padron"

Abrimos Hive en una terminal:

```cmd
hive
```

y lanzamos los comandos:

```sql
DROP DATABASE IF EXISTS datos_padron CASCADE;
CREATE DATABASE datos_padron;
```

Por último, usamos la base de datos:

```sql
USE datos_padron;
```

2. Crear la tabla *padron_txt* con todos los campos del fichero CSV y cargar los datos mediante el comando ```LOAD DATA LOCAL INPATH```. 
   - La tabla tendrá formato texto y como delimitador de campo el caracter ';'
   - Los campos que en el documento original están encerrados en comillas dobles '"' no deben estar envueltos en estos caracteres en la tabla de Hive.
   - Se deberá omitir la cabecera del fichero de datos al crear la tabla.

Creamos la tabla con los campos del CSV:

```sql
DROP TABLE IF EXISTS padron_txt;
CREATE TABLE padron_txt (
    COD_DISTRITO INT,
    DESC_DISTRITO STRING,
    COD_DIST_BARRIO INT,
    DESC_BARRIO STRING,
    COD_BARRIO INT,
    COD_DIST_SECCION INT,
    COD_SECCION INT,
    COD_EDAD_INT INT,
    EspanolesHombres INT,
    EspanolesMujeres INT,
    ExtranjerosHombres INT,
    ExtranjerosMujeres INT
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    "separatorChar" = "\;",
    "quoteChar" = "\""
)
STORED AS TEXTFILE
TBLPROPERTIES("skip.header.line.count"="1");
```

Cargamos los datos:

```sql
LOAD DATA LOCAL INPATH './200076-1-padron.csv' INTO TABLE padron_txt;
```

3. Hacer trim sobre los datos para eliminar los espacios innecesarios guardando la tabla resultado como padron_txt_2.

```sql
DROP TABLE IF EXISTS padron_txt_2_trim;
CREATE TABLE padron_txt_2_trim AS
SELECT 
    TRIM(COD_DISTRITO),
    TRIM(DESC_DISTRITO),
    TRIM(COD_DIST_BARRIO),
    TRIM(DESC_BARRIO),
    TRIM(COD_BARRIO),
    TRIM(COD_DIST_SECCION),
    TRIM(COD_SECCION),
    TRIM(COD_EDAD_INT),
    TRIM(EspanolesHombres),
    TRIM(EspanolesMujeres),
    TRIM(ExtranjerosHombres),
    TRIM(ExtranjerosMujeres)
FROM padron_txt;
```

4. Investigar y entender la diferencia de incluir la palabra LOCAL en el comando LOAD DATA.

El comando ```LOAD DATA``` toma como sistema de ficheros HDFS. Al incluir la palabra ```LOCAL``` en el comando, indicamos que el sistema de ficheros es el nativo de nuestra máquina.

5. En este momento te habrás dado cuenta de un aspecto importante, los datos nulos de nuestras tablas vienen representados por un espacio vacío y no por un identificador de nulos comprensible para la tabla. Esto puede ser un problema para el tratamiento posterior de los datos. Podrías solucionar esto creando una nueva tabla utiliando sentencias case when que sustituyan espacios en blanco por 0. Para esto aplicaremos las sentencias case when para sustituir por 0 los espacios en blanco. (Pista: es útil darse cuenta de que un espacio vacío es un campo con longitud 0). Haz esto solo para la tabla padron_txt.

```sql
ALTER TABLE padron_txt RENAME TO old_padron_txt;
CREATE TABLE padron_txt AS
SELECT 
    CAST(COD_DISTRITO AS INT),
    DESC_DISTRITO,
    CAST(COD_DIST_BARRIO AS INT),
    DESC_BARRIO,
    CAST(COD_BARRIO AS INT),
    CAST(COD_DIST_SECCION AS INT),
    CAST(COD_SECCION AS INT),
    CAST(COD_EDAD_INT AS INT),
    CAST(CASE 
        WHEN LENGTH(EspanolesHombres) = 0 THEN 0
        ELSE EspanolesHombres
    END AS INT) AS EspanolesHombres,
    CAST(CASE 
        WHEN LENGTH(EspanolesMujeres) = 0 THEN 0
        ELSE EspanolesMujeres
    END AS INT) AS EspanolesMujeres,
    CAST(CASE 
        WHEN LENGTH(ExtranjerosHombres) = 0 THEN 0
        ELSE ExtranjerosHombres
    END AS INT) AS ExtranjerosHombres,
    CAST(CASE 
        WHEN LENGTH(ExtranjerosMujeres) = 0 THEN 0
        ELSE ExtranjerosMujeres
    END AS INT) AS ExtranjerosMujeres
FROM old_padron_txt;
DROP TABLE old_padron_txt;
```

6. Una manera tremendamente potente de solucionar todos los problemas previos (tanto las comillas como los campos vacíos que no son catalogados como null y los espacios innecesarios) es utilizar expresiones regulares (regex) que nos proporciona OpenCSV.

Para ello utilizamos :

```sql
 ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe' WITH SERDEPROPERTIES ('input.regex'='XXXXXXX')
```

Donde XXXXXX representa una expresión regular que debes completar y que identifique el formato exacto con el que debemos interpretar cada una de las filas de nuestro CSV de entrada. Para ello puede ser útil el portal "regex101". Utiliza este método para crear de nuevo la tabla padron_txt_2.

Una vez finalizados todos estos apartados deberíamos tener una tabla padron_txt que conserve los espacios innecesarios, no tenga comillas envolviendo los campos y los campos nulos sean tratados como valor 0 y otra tabla padron_txt_2 sin espacios innecesarios, sin comillas envolviendo los campos y con los campos nulos como valor 0. Idealmente esta tabla ha sido creada con las regex de OpenCSV.

```sql
DROP TABLE IF EXISTS padron_txt_2_trim;
DROP TABLE IF EXISTS padron_txt_2_raw;
CREATE TABLE padron_txt_2_raw (
    COD_DISTRITO INT,
    DESC_DISTRITO STRING,
    COD_DIST_BARRIO INT,
    DESC_BARRIO STRING,
    COD_BARRIO INT,
    COD_DIST_SECCION INT,
    COD_SECCION INT,
    COD_EDAD_INT INT,
    EspanolesHombres INT,
    EspanolesMujeres INT,
    ExtranjerosHombres INT,
    ExtranjerosMujeres INT
) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe'
WITH SERDEPROPERTIES (
    "input.regex" = '"(\\d*)"\;"\\s*([^\\s]*)\\s*"\;"(\\d*)"\;"\\s*([^\\s]*)\\s*"\;"(\\d*)"\;"(\\d*)"\;"(\\d*)"\;"(\\d*)"\;"(\\d*)"\;"(\\d*)"\;"(\\d*)"\;"(\\d*)"'
    )
STORED AS TEXTFILE    
TBLPROPERTIES("skip.header.line.count" = "1");

LOAD DATA LOCAL INPATH './200076-1-padron.csv' INTO TABLE padron_txt_2_raw;

DROP TABLE IF EXISTS padron_txt_2;
CREATE TABLE padron_txt_2 AS
SELECT
    COD_DISTRITO,
    DESC_DISTRITO,
    COD_DIST_BARRIO,
    DESC_BARRIO,
    COD_BARRIO,
    COD_DIST_SECCION,
    COD_SECCION,
    COD_EDAD_INT,
    NVL(EspanolesHombres, 0) AS EspanolesHombres,
    NVL(EspanolesMujeres, 0) AS EspanolesMujeres,
    NVL(ExtranjerosHombres, 0) AS ExtranjerosHombres,
    NVL(ExtranjerosMujeres, 0) AS ExtranjerosMujeres
FROM padron_txt_2_raw;
DROP TABLE padron_txt_2_raw;
```

## 2. Investigamos el formato columnar parquet

1. ¿Qué es CTAS?

CTAS es el acrónimo para ```CREATE TABLE AS SELECT```, es decir, crear una tabla a partir de una consulta ```SELECT``` a otra. Por ejemplo, en el apartado 1.3 creamos la tabla *padron_txt_2* seleccionando los campos de la tabla *padron_txt*:

> ```sql
> CREATE TABLE padron_txt_2 AS
> SELECT 
>    ...
> FROM padron_txt;
> ```

2. Crear tabla Hive padron_parquet (cuyos datos serán almacenados en el formato columnar parquet) a través de la tabla padron_txt mediante un CTAS.

```sql
CREATE TABLE padron_parquet 
    STORED AS parquet
AS SELECT * FROM padron_txt;
```

3. Crear tabla Hive padron_parquet_2 a través de la tabla padron_txt_2 mediante un CTAS. En este punto deberíamos tener 4 tablas, 2 en txt (padron_txt y padron_txt_2, la primera con espacios innecesarios y la segunda sin espacios innecesarios) y otras dos tablas en formato parquet (padron_parquet y padron_parquet_2, la primera con espacios y la segunda sin ellos).

```sql
CREATE TABLE padron_parquet_2
    STORED AS parquet
AS SELECT * FROM padron_txt_2;
```

4. Opcionalmente también se pueden crear las tablas directamente desde 0 (en lugar de mediante CTAS) en formato parquet igual que lo hicimos para el formato txt incluyendo la sentencia STORED AS PARQUET. Es importante para comparaciones posteriores que la tabla padron_parquet conserve los espacios innecesarios y la tabla padron_parquet_2 no los tenga. Dejo a tu elección cómo hacerlo.

5. Investigar en qué consiste el formato columnar parquet y las ventajas de trabajar con este tipo de formatos.

Parquet es un formato open source de datos desarrollado por Apache. Sus características principales son:

- Está basado en columnas, en lugar de filas. Esto permite ahorrar espacio y ganar velocidad de procesamiento de queries.
- Es muy eficiente comprimiendo y descomprimiendo datos.
- Soporta tipos de datos complejos.

Ventajas de los formatos columnares como Parquet:

- Son buenos para almacenar grandes volúmenes de datos ya que permiten opciones de compresión flexibles y rápidas.
- Son más eficientes que los formatos en filas como CSV, ya que al lanzar consultas se pueden omitir los datos no relevantes de manera muy rápida.
- Soportan estructuras de datos anidadas.

6. Comparar el tamaño de los ficheros de los datos de las tablas padron_txt (txt), padron_txt_2 (txt pero no incluye los espacios innecesarios), padron_parquet y padron_parquet_2 (alojados en hdfs cuya ruta se puede obtener de la propiedad location de cada tabla por ejemplo haciendo "show create table").

Haciendo ```SHOW CREATE TABLE padron_txt``` obtenemos la ubicación de la tabla padron_txt:

```
LOCATION
    'hdfs://quickstart.cloudera:8020/user/hive/warehouse/datos_padron.db/padron_txt'
```

Desde la carpeta padre (datos_padron.db) podemos ver el tamaño de las cuatro tablas de nuestra base de datos, mediante:

```cmd
hadoop fs -ls -R -h /user/hive/warehouse/datos_padron.db
```

Obteniendo el siguiente resultado:

```txt
drwxrwxrwx   - cloudera supergroup          0 2022-05-10 10:09 /user/hive/warehouse/datos_padron.db/padron_parquet
-rwxrwxrwx   1 cloudera supergroup    912.7 K 2022-05-10 10:09 /user/hive/warehouse/datos_padron.db/padron_parquet/000000_0
drwxrwxrwx   - cloudera supergroup          0 2022-05-10 10:10 /user/hive/warehouse/datos_padron.db/padron_parquet_2
-rwxrwxrwx   1 cloudera supergroup    420.9 K 2022-05-10 10:10 /user/hive/warehouse/datos_padron.db/padron_parquet_2/000000_0
drwxrwxrwx   - cloudera supergroup          0 2022-05-10 10:07 /user/hive/warehouse/datos_padron.db/padron_txt
-rwxrwxrwx   1 cloudera supergroup     21.6 M 2022-05-10 10:07 /user/hive/warehouse/datos_padron.db/padron_txt/200076-1-padron.csv
drwxrwxrwx   - cloudera supergroup          0 2022-05-10 10:08 /user/hive/warehouse/datos_padron.db/padron_txt_2
-rwxrwxrwx   1 cloudera supergroup      8.9 M 2022-05-10 10:08 /user/hive/warehouse/datos_padron.db/padron_txt_2/000000_0
```

Podemos observar que las tablas guardadas en formato texto ocupan mucho más que las versiones en parquet y que las versiones con espacios innecesarios ocupan más que las que no los tienen.

> padron_txt es 2.42 veces > padron_txt_2
>
> padron_parquet es 2.12 veces > padron_parquet_2
>
> padron_txt es 24.22 veces > padron_parquet
>
> padron_txt_2 es 21.66 veces > padron_parquet_2

Por lo tanto, el formato texto ocupa más de 20 veces más que los mismos datos en parquet. Además, los espacios innecesarios aumentan el tamaño de los ficheros a más del doble.

## 3. Juguemos con Impala

1. ¿Qué es Impala?

Impala es un motor SQL MPP (procesamiento masivo en paralelo) pensado para operar sobre grandes volúmenes de datos que se ejecuta en clusters Hadoop, lo que le permite ejecutar queries sobre HDFS o Hbase y leer y escribir datos sobre ficheros con tipos de datos de Hadoop.

2. ¿En qué se diferencia de Hive?

- Hive fue una de las primeras herramientas del ecosistema Hadoop, por lo que está más desarrollada y ofrece muchas más funcionalidades que Impala, por ejemplo:
  - Ficheros con tipos de datos a medida
  - Tipo de datos DATE
  - Funciones XML y JSON
  - Sampling (ejecutar queries sólo sobre un subset de la tabla)
- Hive está basado en lotes y MapReduce, mientras que Impala se basa en MPP.
- Impala, al contrario que Hive, no es tolerante a fallos, por lo que si ocurre un error durante la ejecución de una query, se debe lanzar otra vez.

3. Comando ```INVALIDATE METADATA```, ¿en qué consiste?

Los demonios de Impala cachean los metadatos de definición de tablas y la localización de los bloques HDFS desde el metastore al inicio de la sesión. Los cambios en el metastore realizados por un demonio de Impala son notificados al servicio Catalog de Impala, lo que hace que los demás demonios recarguen su caché. 

Sin embargo, si el cambio en los metadatos se realiza desde fuera de Impala (Hive, Pig, Hue...) es necesario refrescar manualmente la caché.

El comando ```INVALIDATE METADATA``` marca la caché de los demonios como *stale* y la recarga a medida que sea necesario.

Por otro lado, el mismo comando, añadiendo el nombre de una tabla ```INVALIDATE METADATA <tabla>``` marca solo los metadatos de esa tabla como *stale*.

4. Hacer invalidate metadata en Impala de la base de datos datos_padron.

En primer lugar, accedemos a la shell de Impala:

```bash
impala-shell
```

Seleccionamos la base de datos ```datos_padron```:

```sql
USE datos_padron;
```

E invalidamos los metadatos de los demonios:

```sql
INVALIDATE METADATA;
```

5. Calcular el total de EspanolesHombres, EspanolesMujeres, ExtranjerosHombres y ExtranjerosMujeres agrupado por DESC_DISTRITO y DESC_BARRIO.

```sql
SELECT DESC_DISTRITO, 
    DESC_BARRIO, 
    SUM(EspanolesHombres) AS totalEspanolesHombres,
    SUM(EspanolesMujeres) AS totalEspanolesMujeres,
    SUM(ExtranjerosHombres) AS totalExtranjerosHombres,
    SUM(ExtranjerosMujeres) AS totalExtranjerosMujeres
FROM padron_parquet_2 
GROUP BY DESC_DISTRITO, DESC_BARRIO;
```

6. Llevar a cabo las consultas en Hive en las tablas padron_txt_2 y padron_parquet_2 (No deberían incluir espacios innecesarios). ¿Alguna conclusión?

Los tiempos de consulta son similares, aunque ligeramente inferiores en formato texto.

Texto|Parquet
-|-
31.119s|31.291s

7. Llevar a cabo la misma consulta sobre las mismas tablas en Impala. ¿Alguna
conclusión?

En Impala es más evidente que en formato texto tarda menos que en formato parquet.

Texto|Parquet
-|-
4.33s|6.60s

8. ¿Se percibe alguna diferencia de rendimiento entre Hive e Impala?

Sí, Impala es mucho más rápido que Hive a la hora de realizar consultas. En este caso, es entre 5 y 7 veces más rápido.

## 4. Sobre tablas particionadas

1. Crear tabla (Hive) padron_particionado particionada por campos DESC_DISTRITO y DESC_BARRIO cuyos datos estén en formato parquet.

```sql
CREATE TABLE padron_particionado (
    COD_DISTRITO INT,
    COD_DIST_BARRIO INT,
    COD_BARRIO INT,
    COD_DIST_SECCION INT,
    COD_SECCION INT,
    COD_EDAD_INT INT,
    EspanolesHombres INT,
    EspanolesMujeres INT,
    ExtranjerosHombres INT,
    ExtranjerosMujeres INT
)
PARTITIONED BY (DESC_DISTRITO STRING, DESC_BARRIO STRING)
STORED AS PARQUET;
```

2. Insertar datos (en cada partición) dinámicamente (con Hive) en la tabla recién creada a partir de un select de la tabla padron_parquet_2.

En primer lugar, debemos activar las particiones dinámicas y el modo no estricto, para poder particionar sin tener una columna estática.

```sql
SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;
```

A continuación, insertamos los datos haciendo un SELECT de padron_parquet_2.

```sql
INSERT OVERWRITE TABLE padron_particionado PARTITION (DESC_DISTRITO, DESC_BARRIO) 
SELECT COD_DISTRITO,
    COD_DIST_BARRIO,
    COD_BARRIO,
    COD_DIST_SECCION,
    COD_SECCION,
    COD_EDAD_INT,
    EspanolesHombres,
    EspanolesMujeres,
    ExtranjerosHombres,
    ExtranjerosMujeres,
    DESC_DISTRITO,
    DESC_BARRIO
FROM padron_parquet_2;
```

3. Hacer invalidate metadata en Impala de la base de datos padron_particionado.

```sql
INVALIDATE METADATA padron_particionado;
```

4. Calcular el total de EspanolesHombres, EspanolesMujeres, ExtranjerosHombres y ExtranjerosMujeres agrupado por DESC_DISTRITO y DESC_BARRIO para los distritos CENTRO, LATINA, CHAMARTIN, TETUAN, VICALVARO y BARAJAS.

```sql
SELECT DESC_DISTRITO, 
    DESC_BARRIO, 
    SUM(EspanolesHombres) AS totalEspanolesHombres,
    SUM(EspanolesMujeres) AS totalEspanolesMujeres,
    SUM(ExtranjerosHombres) AS totalExtranjerosHombres,
    SUM(ExtranjerosMujeres) AS totalExtranjerosMujeres
FROM padron_particionado
WHERE DESC_DISTRITO IN ('CENTRO', 'LATINA', 'CHAMARTIN', 'TETUAN', 'VICALVARO', 'BARAJAS') 
GROUP BY DESC_DISTRITO, DESC_BARRIO
ORDER BY DESC_DISTRITO;

SELECT DESC_DISTRITO, 
    DESC_BARRIO, 
    SUM(EspanolesHombres) AS totalEspanolesHombres,
    SUM(EspanolesMujeres) AS totalEspanolesMujeres,
    SUM(ExtranjerosHombres) AS totalExtranjerosHombres,
    SUM(ExtranjerosMujeres) AS totalExtranjerosMujeres
FROM padron_parquet_2
WHERE DESC_DISTRITO IN ('CENTRO', 'LATINA', 'CHAMARTIN', 'TETUAN', 'VICALVARO', 'BARAJAS') 
GROUP BY DESC_DISTRITO, DESC_BARRIO
ORDER BY DESC_DISTRITO;
```

5. Llevar a cabo la consulta en Hive en las tablas padron_parquet y padron_partitionado. ¿Alguna conclusión?

Sin particionar|Particionada
-|-
63.235s|60.163s

Los tiempos son prácticamente iguales.

6. Llevar a cabo la consulta en Impala en las tablas padron_parquet y padron_particionado. ¿Alguna conclusión?

Sin particionar|Particionada
-|-
0.75s|0.66s

Una vez más, los tiempos son prácticamente iguales.

7. Hacer consultas de agregación (Max, Min, Avg, Count) tal cual el ejemplo anterior con las 3 tablas (padron_txt_2, padron_parquet_2 y padron_particionado) y comparar rendimientos tanto en Hive como en Impala y sacar conclusiones.

```sql
SELECT 
    DESC_BARRIO,
    COUNT(DESC_BARRIO) AS totalBarrio,
    AVG(EspanolesHombres) AS averageEspanolesHombres,
    MAX(EspanolesMujeres) AS maxEspanolesMujeres,
    MIN(ExtranjerosHombres) AS minExtranjerosHombres
FROM padron_parquet_2 
GROUP BY DESC_DISTRITO, DESC_BARRIO;

SELECT 
    DESC_BARRIO,
    COUNT(DESC_BARRIO) AS totalBarrio,
    AVG(EspanolesHombres) AS averageEspanolesHombres,
    MAX(EspanolesMujeres) AS maxEspanolesMujeres,
    MIN(ExtranjerosHombres) AS minExtranjerosHombres
FROM padron_txt_2 
GROUP BY DESC_DISTRITO, DESC_BARRIO;

SELECT 
    DESC_BARRIO,
    COUNT(DESC_BARRIO) AS totalBarrio,
    AVG(EspanolesHombres) AS averageEspanolesHombres,
    MAX(EspanolesMujeres) AS maxEspanolesMujeres,
    MIN(ExtranjerosHombres) AS minExtranjerosHombres
FROM padron_particionado
GROUP BY DESC_DISTRITO, DESC_BARRIO;
```

||Hive|Impala
|-|-|-|
parquet_2|31.037s|0.53s
txt_2|33.096s|0.53s
particionado|35.519s|1.17s

De estos datos podemos observar que Impala es mucho más rápido que Hive (entre 30 y 60 veces), y que el orden de mayor a menor velocidad en cuanto a formatos/particiones es: ```parquet > txt > particionado```.

## 5. Trabajando con tablas en HDFS.
A continuación vamos a hacer una inspección de las tablas, tanto externas (no gestionadas) como internas (gestionadas).
1. Crear un documento de texto en el almacenamiento local que contenga una secuencia de números distribuidos en filas y separados por columnas, llámalo datos1 y que sea por ejemplo:

```txt
1,2,3
4,5,6
7,8,9
```

Para crear el fichero lanzamos el comando:

```bash
echo "1,2,3
4,5,6
7,8,9" > datos1
```

2. Crear un segundo documento (datos2) con otros números pero la misma estructura.

```bash
echo "21,27,56
12,75,48
5,13,2" > datos2
```

3. Crear un directorio en HDFS con un nombre a placer, por ejemplo, /test. Si estás en una máquina Cloudera tienes que asegurarte de que el servicio HDFS está activo ya que puede no iniciarse al encender la máquina (puedes hacerlo desde el Cloudera Manager). A su vez, en las máquinas Cloudera es posible (dependiendo de si usamos Hive desde consola o desde Hue) que no tengamos permisos para crear directorios en HDFS salvo en el directorio /user/cloudera.

Comprobamos que los servicios están habilitados:

```bash
chkconfig --list | grep hadoop
```

Obteniendo el siguiente resultado:

```txt
hadoop-hdfs-datanode	        0:off	1:off	2:off	3:on	4:on	5:on	6:off
hadoop-hdfs-journalnode	        0:off	1:off	2:off	3:on	4:on	5:on	6:off
hadoop-hdfs-namenode	        0:off	1:off	2:off	3:on	4:on	5:on	6:off
hadoop-hdfs-secondarynamenode	0:off	1:off	2:off	3:on	4:on	5:on	6:off
hadoop-httpfs  	                0:off	1:off	2:off	3:on	4:on	5:on	6:off
hadoop-mapreduce-historyserver	0:off	1:off	2:off	3:on	4:on	5:on	6:off
hadoop-yarn-nodemanager	        0:off	1:off	2:off	3:on	4:on	5:on	6:off
hadoop-yarn-proxyserver	        0:off	1:off	2:off	3:on	4:on	5:on	6:off
hadoop-yarn-resourcemanager	    0:off	1:off	2:off	3:on	4:on	5:on	6:off
```

Esto nos indica que todos los servicios de Hadoop se inician automáticamente desde el nivel de ejecución 3 hasta el 5.

A continuación, comprobamos que todos los servicios están ejecutándose:

```bash
for line in $(chkconfig --list | grep hadoop | cut -f 1); do service $line status; done;
```

Obteniendo:

```bash
Hadoop datanode is running                                 [  OK  ]
Hadoop journalnode is running                              [  OK  ]
Hadoop namenode is running                                 [  OK  ]
Hadoop secondarynamenode is running                        [  OK  ]
Hadoop httpfs is running                                   [  OK  ]
Hadoop historyserver is running                            [  OK  ]
Hadoop nodemanager is running                              [  OK  ]
Hadoop proxyserver is dead and pid file exists             [FAILED]
Hadoop resourcemanager is running                          [  OK  ]
```

No vamos a necesitar el proxyserver, por lo que podemos continuar al siguiente paso.

Crearemos la carpeta /test en el raíz de nuestro sistema HDFS:

```bash
hadoop fs -mkdir /test
```

4. Mueve tu fichero datos1 al directorio que has creado en HDFS con un comando desde consola.

```bash
hadoop fs -put datos1 /test
```

5. Desde Hive, crea una nueva database por ejemplo con el nombre numeros. Crea una tabla que no sea externa y sin argumento location con tres columnas numéricas, campos separados por coma y delimitada por filas. La llamaremos por ejemplo numeros_tbl.

En primer lugar, creamos y usamos la base de datos *numeros*:

```sql
DROP DATABASE IF EXISTS numeros;
CREATE DATABASE numeros;
USE numeros;
```

A continuación, creamos la tabla *numeros_tbl*:

```sql
CREATE TABLE numeros_tbl(
    num1 INT,
    num2 INT,
    num3 INT
)
ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ',';
```

6. Carga los datos de nuestro fichero de texto datos1 almacenado en HDFS en la tabla de Hive. Consulta la localización donde estaban anteriormente los datos almacenados. ¿Siguen estando ahí? ¿Dónde están?. Borra la tabla, ¿qué ocurre con los datos almacenados en HDFS?

Cargamos los datos desde el fichero datos1:

```sql
LOAD DATA INPATH '/test/datos1' INTO TABLE numeros_tbl;
```

Desde una terminal, si listamos los ficheros del directorio '/test', comprobamos que los datos no están:

```bash
hadoop fs -ls /test
```

Para obtener la localización, lanzamos el siguiente comando desde Hive:

```sql
SHOW CREATE TABLE numeros_tbl;
```

Obteniendo:

```txt
CREATE TABLE `numeros_tbl`(
  `num1` int, 
  `num2` int, 
  `num3` int)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
WITH SERDEPROPERTIES ( 
  'field.delim'=',', 
  'serialization.format'=',') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://quickstart.cloudera:8020/user/hive/warehouse/numeros.db/numeros_tbl'
TBLPROPERTIES (
  'COLUMN_STATS_ACCURATE'='true', 
  'numFiles'='1', 
  'totalSize'='18', 
  'transient_lastDdlTime'='1652179970')
```

El apartado ```LOCATION``` nos da la ubicación de los datos que, como cabía esperar, se encuentran dentro del warehouse de Hive.

Si borramos la tabla:

```sql
DROP TABLE numeros_tbl;
```

Los datos se pierden, puesto que Hive los borra del warehouse.

```bash
hadoop fs -ls /user/hive/warehouse/numeros.db
```

7. Vuelve a mover el fichero de texto datos1 desde el almacenamiento local al directorio anterior en HDFS.

```bash
hadoop fs -put datos1 /test
```

8. Desde Hive, crea una tabla externa sin el argumento location. Y carga datos1 (desde HDFS) en ella. ¿A dónde han ido los datos en HDFS? Borra la tabla. ¿Qué ocurre con los datos en hdfs?

```sql
CREATE EXTERNAL TABLE numeros_tbl(
    num1 INT,
    num2 INT,
    num3 INT
)
ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ',';
LOAD DATA INPATH '/test/datos1' INTO TABLE numeros_tbl;
```

Si mostramos la información de la tabla mediante ```SHOW CREATE TABLE numeros_tbl```, obtenemos la localización:

```txt
LOCATION
  'hdfs://quickstart.cloudera:8020/user/hive/warehouse/numeros.db/numeros_tbl'
```

Borramos la tabla:

```sql
DROP TABLE numeros_tbl;
```

Si comprobamos la localización señalada anteriormente:

```bash
hadoop fs -ls /user/hive/warehouse/numeros.db/numeros_tbl
```

Comprobamos que los datos no se han borrado.

9. Borra el fichero datos1 del directorio en el que estén. Vuelve a insertarlos en el directorio que creamos inicialmente (/test). Vuelve a crear la tabla numeros desde hive pero ahora de manera externa y con un argumento location que haga referencia al directorio donde los hayas situado en HDFS (/test). No cargues los datos de ninguna manera explícita. Haz una consulta sobre la tabla que acabamos de crear que muestre todos los registros. ¿Tiene algún contenido?

Borramos el fichero datos1:

```bash
hadoop fs -rm /user/hive/warehouse/numeros.db/numeros_tbl/datos1
```

Volvemos a insertarlos en /test:

```bash
hadoop fs -put datos1 /test
```

Creamos la tabla externa especificando el *location*:

```sql
CREATE EXTERNAL TABLE numeros_tbl(
    num1 INT,
    num2 INT,
    num3 INT
)
ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
LOCATION '/test';
```

Si lanzamos una consulta SELECT sobre la tabla:

```sql
SELECT * FROM numeros_tbl;
```

Obtenemos como resultado los datos del fichero datos1.

10. Inserta el fichero de datos creado al principio, "datos2" en el mismo directorio de HDFS que "datos1". Vuelve a hacer la consulta anterior sobre la misma tabla. ¿Qué salida muestra?

Insertamos el fichero:

```bash
hadoop fs -put datos2 /test
```

Y volvemos a lanzar la sentencia SELECT:

```sql
SELECT * FROM numeros_tbl;
```

Se nos muestran los datos tanto de *datos1* como de *datos2*.

11. Extrae conclusiones de todos estos anteriores apartados.

Si creamos una tabla interna, los datos se almacenan en el warehouse de Hive y se borran al eliminar la tabla. Por el contrario, si la tabla es externa, los metadatos se borran pero los datos no.

Además, si definimos una localización para los datos de una tabla y añadimos ficheros a esa ubicación, Hive los empleará al lanzar consultas.

## 6. Un poquito de Spark

La siguiente sección de la práctica se abordará si ya se tienen suficientes conocimientos de Spark, en concreto de el manejo de DataFrames, y el manejo de tablas de Hive a través de Spark.sql.

1. Comenzamos realizando la misma práctica que hicimos en Hive en Spark, importando el csv. Sería recomendable intentarlo con opciones que quiten las "" de los campos, que ignoren los espacios innecesarios en los campos, que sustituyan los valores vacíos por 0 y que infiera el esquema.

Comenzamos lanzando el spark-shell:

```bash
spark-shell
```

Creamos el SparkSession:

```scala
import org.apache.spark.sql.SparkSession

val spark = (SparkSession
    .builder
    .appName("padron")
    .getOrCreate())
```

Leemos el fichero:

```scala
val csvFile = "file:///home/cloudera/200076-1-padron.csv"
val padronDF = (spark.read
    .format("csv")
    .option("header", true)
    .option("inferSchema", true)
    .option("samplingRatio", 0.1)
    .option("ignoreTrailingWhiteSpace", true)
    .option("nullValue", 0)
    .option("quote", "\u0000")
    .load(csvFile)
)
```

2. De manera alternativa también se puede importar el csv con menos tratamiento en la importación y hacer todas las modificaciones para alcanzar el mismo estado de limpieza de los datos con funciones de Spark.

```scala
import org.apache.spark.sql.functions._
import org.apache.spark.sql.implicits._
val csvFile = "file:///home/cloudera/200076-1-padron.csv"
val padronDF = (spark.read
    .format("csv")
    .option("header", true)
    .option("inferSchema", true)
    .option("samplingRatio", 0.1)
    .option("delimiter", ";")
    .load(csvFile)
    .withColumn("DESC_DISTRITO", trim($"DESC_DISTRITO"))
    .withColumn("DESC_BARRIO", trim($"DESC_BARRIO"))
    .withColumn("EspanolesHombres", when(col("EspanolesHombres").isNull, 0).otherwise($"EspanolesHombres"))
    .withColumn("EspanolesHombres", when(col("EspanolesHombres").isNull, 0).otherwise($"EspanolesHombres"))
    .withColumn("EspanolesMujeres", when(col("EspanolesMujeres").isNull, 0).otherwise($"EspanolesMujeres"))
    .withColumn("ExtranjerosHombres", when(col("ExtranjerosHombres").isNull, 0).otherwise($"ExtranjerosHombres"))
    .withColumn("ExtranjerosMujeres", when(col("ExtranjerosMujeres").isNull, 0).otherwise($"ExtranjerosMujeres"))
)
```

3. Enumera todos los barrios diferentes.

```scala
padronDF.select("DESC_BARRIO").distinct().collect()
```

4. Crea una vista temporal de nombre "padron" y a través de ella cuenta el número de barrios diferentes que hay.

```scala
padronDF.createOrReplaceTempView("padron")
spark.sql("SELECT COUNT(DISTINCT DESC_BARRIO) AS count_barrios FROM padron").show()
```

5. Crea una nueva columna que muestre la longitud de los campos de la columna
DESC_DISTRITO y que se llame "longitud".

```scala
val padronDF_2 = padronDF.withColumn("longitud", expr("LENGTH(DESC_DISTRITO)"))
padronDF_2.show()
```

6. Crea una nueva columna que muestre el valor 5 para cada uno de los registros de la tabla.

```scala
val padronDF_3 = padronDF_2.withColumn("valor", lit(5))
padronDF_3.show()
```

7. Borra esta columna.

```scala
val padronDF_4 = padronDF_3.drop("valor")
padronDF_4.show()
```

8. Particiona el DataFrame por las variables DESC_DISTRITO y DESC_BARRIO.

```scala
val padronDF_5 = padronDF_4.repartition($"DESC_DISTRITO", $"DESC_BARRIO")
```

9.  Almacénalo en caché. Consulta en el puerto 4040 (UI de Spark) de tu usuario local el estado de los rdds almacenados.

```scala
padronDF_5.cache()
```

10. Lanza una consulta contra el DF resultante en la que muestre el número total de "espanoleshombres", "espanolesmujeres", extranjeroshombres" y "extranjerosmujeres" para cada barrio de cada distrito. Las columnas distrito y barrio deben ser las primeras en aparecer en el show. Los resultados deben estar ordenados en orden de más a menos según la columna "extranjerosmujeres" y desempatarán por la columna "extranjeroshombres".

```scala
(padronDF_5
    .select("DESC_DISTRITO", "DESC_BARRIO", "EspanolesHombres", "EspanolesMujeres", "ExtranjerosHombres", "ExtranjerosMujeres")
    .groupBy("DESC_DISTRITO", "DESC_BARRIO")
    .agg(sum("EspanolesHombres").alias("totalEspanolesHombres"),
         sum("EspanolesMujeres").alias("totalEspanolesMujeres"),
         sum("ExtranjerosHombres").alias("totalExtranjerosHombres"),
         sum("ExtranjerosMujeres").alias("totalExtranjerosMujeres"))
    .orderBy($"totalExtranjerosMujeres".desc, $"totalExtranjerosHombres".desc)
).show()
```

11. Elimina el registro en caché.

```scala
padronDF_5.unpersist()
```

12. Crea un nuevo DataFrame a partir del original que muestre únicamente una columna con DESC_BARRIO, otra con DESC_DISTRITO y otra con el número total de "espanoleshombres" residentes en cada distrito de cada barrio. Únelo (con un join) con el DataFrame original a través de las columnas en común.

```scala
val padronDF_nuevo = (padronDF_5
    .select("DESC_BARRIO", "DESC_DISTRITO", "espanolesHombres")
    .groupBy("DESC_BARRIO", "DESC_DISTRITO")
    .agg(sum("espanolesHombres").alias("totalEspanolesHombres")))
padronDF_nuevo.join(padronDF_5,
    padronDF_nuevo("DESC_BARRIO") === padronDF_5("DESC_BARRIO") &&
    padronDF_nuevo("DESC_DISTRITO") === padronDF_5("DESC_DISTRITO")
).show()
```

13. Repite la función anterior utilizando funciones de ventana. (over(Window.partitionBy.....)).

```scala
import org.apache.spark.sql.expressions.Window
val windowSpec = Window.partitionBy("DESC_BARRIO", "DESC_DISTRITO")
padronDF_5.withColumn("totalEspanolesHombres", sum($"espanolesHombres").over(windowSpec)).show();
```

14. Mediante una función Pivot muestra una tabla (que va a ser una tabla de contingencia) que contenga los valores totales (la suma de valores) de espanolesmujeres para cada distrito y en cada rango de edad (COD_EDAD_INT). Los distritos incluidos deben ser únicamente CENTRO, BARAJAS y RETIRO y deben figurar como columnas.

```scala
val padronEdadDistrito = (padronDF_5
    .where($"DESC_DISTRITO".isin("CENTRO", "BARAJAS", "RETIRO"))
    .groupBy("COD_EDAD_INT")
    .pivot("DESC_DISTRITO")
    .agg(sum($"espanolesMujeres"))
    .orderBy("COD_EDAD_INT"))
padronEdadDistrito.show()
```

15.  Utilizando este nuevo DF, crea 3 columnas nuevas que hagan referencia a qué porcentaje de la suma de "espanolesmujeres" en los tres distritos para cada rango de edad representa cada uno de los tres distritos. Debe estar redondeada a 2 decimales.

```scala
(padronEdadDistrito
    .withColumn("totalEdad", expr("CENTRO + BARAJAS + RETIRO"))
    .withColumn("porcentaje_barajas", expr("CAST(BARAJAS*100/totalEdad AS DECIMAL(38,2))"))
    .withColumn("porcentaje_centro", expr("CAST(CENTRO*100/totalEdad AS DECIMAL(38,2))"))
    .withColumn("porcentaje_retiro", expr("CAST(RETIRO*100/totalEdad AS DECIMAL(38,2))"))
    .drop("totalEdad")
).show()
```

16.  Guarda el archivo csv original particionado por distrito y por barrio (en ese orden) en un directorio local. Consulta el directorio para ver la estructura de los ficheros y comprueba que es la esperada.


Guardamos el fichero en formato CSV:

```scala
padronDF.write.format("csv").partitionBy("DESC_DISTRITO", "DESC_BARRIO").save("file:///tmp/padron-csv")
```

Comprobamos la estructura de ficheros y el peso de los mismos a través del comando:

```bash
ls -lh /tmp/padron-csv
```

Y también comprabamos el peso total del direactorio:

```bash
du -sh /tmp/padron-csv
```

Obteniendo:

```txt
8.2M	/tmp/padron-csv
```

17.  Haz el mismo guardado pero en formato parquet. Compara el peso del archivo con el resultado anterior.

```scala
padronDF.write.format("parquet").partitionBy("DESC_DISTRITO", "DESC_BARRIO").save("file:///tmp/padron-parquet")
```

Una vez más, comprobamos la estructura de ficheros y su peso a través del comando:

```bash
du -sh /tmp/padron-parquet
```

Obteniendo:

```txt
2.6M	/tmp/padron-parquet
```

Por lo tanto, el formato parquet ocupa, en este caso, 3,15 veces menos que el formato csv.

## 7. ¿Y si juntamos Spark y Hive?

1. Por último, prueba a hacer los ejercicios sugeridos en la parte de Hive con el csv "Datos Padrón" (incluyendo la importación con Regex) utilizando desde Spark EXCLUSIVAMENTE sentencias spark.sql, es decir, importar los archivos desde local directamente como tablas de Hive y haciendo todas las consultas sobre estas tablas sin transformarlas en ningún momento en DataFrames ni DataSets.

### Crear base de datos

En primer lugar, creamos la base de datos (para no borrar la anterior, la llamaremos *datos_padron_2*) y la usamos:

```scala
spark.sql("DROP DATABASE IF EXISTS datos_padron_2 CASCADE")
spark.sql("CREATE DATABASE datos_padron_2")
spark.sql("USE datos_padron_2")
```

### Leer datos y crear tabla

Crearemos la tabla como en el punto 1.6, haciendo uso de Regex en una tabla temporal y posteriormente haciendo un CTAS para cambiar los valores nulos por 0.

Primero, creamos la tabla temporal:

```scala
spark.sql("""
CREATE TABLE padron_txt_2_raw (
    COD_DISTRITO INT,
    DESC_DISTRITO STRING,
    COD_DIST_BARRIO INT,
    DESC_BARRIO STRING,
    COD_BARRIO INT,
    COD_DIST_SECCION INT,
    COD_SECCION INT,
    COD_EDAD_INT INT,
    EspanolesHombres INT,
    EspanolesMujeres INT,
    ExtranjerosHombres INT,
    ExtranjerosMujeres INT
) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe'
WITH SERDEPROPERTIES (
    "input.regex" = '"(\\d*)"\;"\\s*([^\\s]*)\\s*"\;"(\\d*)"\;"\\s*([^\\s]*)\\s*"\;"(\\d*)"\;"(\\d*)"\;"(\\d*)"\;"(\\d*)"\;"(\\d*)"\;"(\\d*)"\;"(\\d*)"\;"(\\d*)"'
    )
STORED AS TEXTFILE    
TBLPROPERTIES("skip.header.line.count" = "1")
""")
```

Cargamos los datos:

```scala
spark.sql("LOAD DATA LOCAL INPATH './200076-1-padron.csv' INTO TABLE padron_txt_2_raw")
```

Y creamos la tabla definitiva:

```scala
spark.sql("""
CREATE TABLE padron_txt_2 AS
SELECT
    COD_DISTRITO,
    DESC_DISTRITO,
    COD_DIST_BARRIO,
    DESC_BARRIO,
    COD_BARRIO,
    COD_DIST_SECCION,
    COD_SECCION,
    COD_EDAD_INT,
    NVL(EspanolesHombres, 0) AS EspanolesHombres,
    NVL(EspanolesMujeres, 0) AS EspanolesMujeres,
    NVL(ExtranjerosHombres, 0) AS ExtranjerosHombres,
    NVL(ExtranjerosMujeres, 0) AS ExtranjerosMujeres
FROM padron_txt_2_raw""")
spark.sql("DROP TABLE padron_txt_2_raw")
```

Ahora, crearemos la tabla que almacena los datos en parquet:

```scala
spark.sql("""
CREATE TABLE padron_parquet_2
    STORED AS parquet
AS SELECT * FROM padron_txt_2""")
```

### Consultas

Realizamos la consulta para obtener el total de EspanolesHombres, EspanolesMujeres, ExtranjerosHombres y ExtranjerosMujeres agrupado por DESC_DISTRITO y DESC_BARRIO.

```scala
spark.sql("""
SELECT DESC_DISTRITO, 
    DESC_BARRIO, 
    SUM(EspanolesHombres) AS totalEspanolesHombres,
    SUM(EspanolesMujeres) AS totalEspanolesMujeres,
    SUM(ExtranjerosHombres) AS totalExtranjerosHombres,
    SUM(ExtranjerosMujeres) AS totalExtranjerosMujeres
FROM padron_parquet_2 
GROUP BY DESC_DISTRITO, DESC_BARRIO
""").show()
```

### Crear las tablas particionadas

Definimos la tabla:

```scala
spark.sql("""
CREATE TABLE padron_particionado (
    COD_DISTRITO INT,
    COD_DIST_BARRIO INT,
    COD_BARRIO INT,
    COD_DIST_SECCION INT,
    COD_SECCION INT,
    COD_EDAD_INT INT,
    EspanolesHombres INT,
    EspanolesMujeres INT,
    ExtranjerosHombres INT,
    ExtranjerosMujeres INT
)
PARTITIONED BY (DESC_DISTRITO STRING, DESC_BARRIO STRING)
STORED AS PARQUET
""")
```

Y realizamos la inserción de los datos:

```scala
spark.sql("INSERT OVERWRITE TABLE padron_particionado PARTITION (DESC_DISTRITO, DESC_BARRIO)")
spark.sql("""
SELECT COD_DISTRITO,
    COD_DIST_BARRIO,
    COD_BARRIO,
    COD_DIST_SECCION,
    COD_SECCION,
    COD_EDAD_INT,
    EspanolesHombres,
    EspanolesMujeres,
    ExtranjerosHombres,
    ExtranjerosMujeres,
    DESC_DISTRITO,
    DESC_BARRIO
FROM padron_parquet_2
""")
```

### Consultas sobre tablas particionadas

Calculamos la suma total de españoles y extranjeros de unos determinados distritos:

```scala
spark.sql("""
SELECT DESC_DISTRITO, 
    DESC_BARRIO, 
    SUM(EspanolesHombres) AS totalEspanolesHombres,
    SUM(EspanolesMujeres) AS totalEspanolesMujeres,
    SUM(ExtranjerosHombres) AS totalExtranjerosHombres,
    SUM(ExtranjerosMujeres) AS totalExtranjerosMujeres
FROM padron_particionado
WHERE DESC_DISTRITO IN ('CENTRO', 'LATINA', 'CHAMARTIN', 'TETUAN', 'VICALVARO', 'BARAJAS') 
GROUP BY DESC_DISTRITO, DESC_BARRIO
ORDER BY DESC_DISTRITO""")
```

Por último, lanzamos diferentes consultas de agregación:

```scala
spark.sql("""
SELECT 
    DESC_BARRIO,
    COUNT(DESC_BARRIO) AS totalBarrio,
    AVG(EspanolesHombres) AS averageEspanolesHombres,
    MAX(EspanolesMujeres) AS maxEspanolesMujeres,
    MIN(ExtranjerosHombres) AS minExtranjerosHombres
FROM padron_particionado
GROUP BY DESC_DISTRITO, DESC_BARRIO
""")
```
