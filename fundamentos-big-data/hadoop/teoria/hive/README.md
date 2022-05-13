# Hive

## ¿Qué es Hive?

- Hive es una infraestructura basada en Hadoop diseñada para facilitar la consulta y almacenaje de grandes volúmenes de datos. 
- Emplea un lenguaje de consultas llamado HiveQL (HQL) basado en SQL-92.
- Las consultas expresadas en HQL se traducen a trabajos MapReduce que se ejecutan en Hadoop.
- Hive no es un gestor de base de datos relacionales, sino un sistema de procesamiento por lotes o *batch*. Por lo tanto, tiene una alta latencia y no ofrece consultas en tiempo real.
- Funciona mejor con grandes cantidades de datos, puesto que con conjuntos de datos pequeños ofrece un redimiento mucho peor que sistemas tradicionales como Oracle o MySQL.

## Conceptos

- **Base de datos**: espacio de nombres que agrupa tablas y otras unidades de datos
- **Tablas**: unidades de datos que comparten un mismo esquema
- **Particiones**: claves que determinan como se almacenan los datos en una tabla, optimizando las consultas
- **Buckets o clusters**: subdivisiones de particiones para manejar los datos de manera más cómoda y optimizar los *joins*

### Tipos de datos

#### Primitivos

- Numéricos

|Nombre|Tamaño|Precisión|
|-|-|-|
|TINYINT|1 byte|-2<sup>7</sup> a 2<sup>7</sup>-1|
|SMALLINT|2 byte|-2<sup>15</sup> a 2<sup>15</sup>-1|
|INT|4 byte|-2<sup>31</sup> a 2<sup>31</sup>-1|
|BIGINT|8 byte|-2<sup>63</sup> a 2<sup>63</sup>-1|
|FLOAT|4 byte||
|DOUBLE|8 byte||
|DECIMAL|Arbitrario|Definido por el usuario|

- Fecha y hora

<table>
    <tr>
        <th>Nombre</th>
        <th>Formato</th>
        <th>Representación</th>
    </tr>
    <tr>
        <td rowspan="3">TIMESTAMP</td>
        <td>Numérico (entero)</td>
        <td>Unix timestamp en segundos</td>
    </tr>
    <tr>
        <td>Numérico (coma flotante)</td>
        <td>Unix timestamp en segundos con precisión decimal</td>
    </tr>
    <tr>
        <td>STRING</td>
        <td>YYYY-MM-DD HH:MM:SS.ffffffff</td>
    </tr>
    <tr>
        <td>DATE</td>
        <td>STRING</td>
        <td>YYYY-MM-DD</td>
    </tr>
</table>

- Caracteres

|Nombre|Longitud|
|-|-|
|STRING|Variable|
|VARCHAR|Variable con máximo (entre 1 y 2<sup>16</sup>-1)|
|CHAR|Fija (máximo 255)|

- Otros

|Nombre|Valores|
|-|-|
|BOOLEAN|TRUE/FALSE|
|BINARY|Datos en binario|

#### Complejos

|Nombre|Ejemplo de formato|Creación|Acceso|
|-|-|-|-|
|Array|ARRAY\<STRING>|array('John', 'Doe)|name[0]|
|Map|MAP\<STRING,STRING>|map('first','John','last','Doe)|name['first']|
|Struct|ARRAY\<first STRING; last STRING>|struct('John', 'Doe)|name.first|
|Union|UNIONTYPE\<data_type, data_type>|||

### Operadores

- Relacionales

|Operador|Tipos|Descripción|
|-|-|-|
|A = B|Primitivos|TRUE si A es equivalente a B, si no FALSE|
|A != B <br>A <> B|Primitivos|TRUE si A no es equivalente a B, si no FALSE|
|A < B|Primitivos|TRUE si A es menor que B, si no FALSE|
|A <= B|Primitivos|TRUE si A es menor o igual que B, si no FALSE|
|A > B|Primitivos|TRUE si A es mayor que B, si no FALSE|
|A >= B|Primitivos|TRUE si A es mayor o igual que B, si no FALSE|
|A IS NULL|Todos|TRUE si A se evalúa a NULL, si no FALSE|
|A IS NOT NULL|Todos|FALSE si A se evalúa a NULL, si no TRUE|
|A LIKE B|Strings|TRUE si la cadena A coincide con la expresión regular SQL sencilla B, si no FALSE|
|A RLIKE B<br>A REGEXP B|Strings|NULL si A o B son NULL<br>TRUE si cualquier subcadena de A coincide con la expresión regular de Java B,<br>si no FALSE|

- Aritméticos

|Operador|Tipos|Descripción|
|-|-|-|
|A + B|Númericos|Suma|
|A - B|Númericos|Resta|
|A * B|Númericos|Multiplicación|
|A / B|Númericos|División|
|A % B|Númericos|Módulo (resto de la división entera)|
|A & B|Númericos|AND lógico|
|A \| B|Númericos|OR lógico|
|A ^ B|Númericos|XOR exclusivo lógico|
|~ A|Númericos|NOT lógico|

- Lógicos

|Operador|Tipos|Descripción|
|-|-|-|
|A AND B<br>A && B|Boolean|TRUE si A y B son TRUE, si no FALSE|
|A OR B<br>A \|\| B|Boolean|TRUE si A o B (o ambos) son TRUE, si no FALSE|
|NOT A<br>!A|Boolean|TRUE si A es FALSE, si no FALSE|

- Sobre tipos complejos

|Operador|Tipos|Descripción|
|-|-|-|
|A[n]|*A* es un array y *n* es un INT|Devuelve el n-ésimo elemento del array A|
|M[key]|*M* es un Map<K, V> y *key* tiene de tipo K|Devuelve el valor correspondiente a la clave en el map|
|S.x|*S* es un struct|Devuelve el campo x de S|

### Manejo de base de datos

#### Base de datos

- Crear base de datos

> CREATE DATABASE my_db;

- Cambiar a la base de datos

> USE my_db;

- Borrar la base de datos

> DROP DATABASE my_db;

### HQL

#### Formato de los campos

> Ejemplo: ip STRING COMMENT 'Direccion IP del usuario'

|Parámetro|Descripción|Requerido|
|-|-|-|
|ip|Nombre del campo|Sí|
|STRING|Tipo de dato del campo|Sí|
|COMMENT ...|Descripción del campo|Opcional|

#### Creación de tablas

Ejemplo:

```HQL
CREATE TABLE page_view(
    viewTime INT, userId BIGINT,
    page_url STRING, ip STRING COMMENT 'Direccion IP del usuario'
) COMMENT 'Tabla page view'
PARTITIONED BY(dt STRING, country STRING)
CLUSTERED BY(userId) SORTED BY (viewTime) INTO 32 BUCKETS
ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '1'
    COLLECTION ITEMS TERMINATED BY '2'
    MAP KEYS TERMINATED BY '3'
    LINES TERMINATED BY '4'
STORED AS SEQUENCEFILE;
```

Análisis del ejemplo:

|Línea|Código|Explicación|
|:-:|-|-|
|1|CREATE TABLE \<nombre_tabla> (|Se define el nombre de la tabla|
|2 y 3|\<campos de la tabla>|Se definen los campos siguiendo el formato visto anteriormente
|4|) COMMENT \<comentario>|Se cierra el parentésis entre los que se definen los campos y se define un comentario para la tabla
|5|PARTITIONED BY (\<campo> \<tipo de dato>...)|Define los campos que particionan la tabla
|6|CLUSTERED BY (\<campo>)|Los campos se agruparán en buckets a través de este campo
|6|SORTED BY (\<campo>)|Dentro de cada bucket se ordenarán por este campo
|6| INTO \<n> BUCKETS|Se generarán n buckets
|7|ROW FORMAT DELIMITED|Establece que el formato de las filas será de campos separados por un delimitador
|8|FIELDS TERMINATED BY (\<delimiter>)|Establece el delimitador de los campos
|9|COLLECTION ITEMS TERMINATED BY (\<delimiter>)|Establece el delimitador de arrays y maps
|10|MAP KEYS TERMINATED BY (\<delimiter>)|Establece el delimitador de maps
|11|LINES TERMINATED BY (\<delimiter>)|Establece el delimitador de fin de línea
|12|STORED AS \<formato>;|Define el formato de almacenamiento

##### REGEX SerDe

Permite analizar datos que no tienen delimitadores específicos o que tienen un formato fijo y delimitado mediante expresiones regulares. Cada par de paréntesis corresponde a uno de los campos, que siempre serán de tipo STRING.

- Ejemplo: logs
  
``` console
05/23/2013 19:45:19 312-555-7834 CALL_RECEIVED ""
05/23/2013 19:48:37 312-555-7834 COMPLAINT "Item not received"
```

``` HQL
CREATE TABLE calls (
  event_date STRING,
  event_time STRING,
  phone_num STRING,
  event_type STRING,
  details STRING
) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe'
WITH SERDEPROPERTIES ("input.regex" = 
  "([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) \"([^\"]*)\"");
)
```

Resultado:

|event_date|event_time|phone_num|event_type|details|
|-|-|-|-|-|
|05/23/2013|19:45:19|312-555-7834|CALL_RECEIVED||
|05/23/2013|19:48:37|312-555-7834|COMPLAINT|Item not received|

> [Documentación CREATE TABLE](https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DDL#LanguageManualDDL-CreateTable)

#### Exploración de tablas y particiones

- Mostrar las tablas en la base de datos actual

``` HQL
SHOW TABLES;
```

- Mostrar las tablas con el prefijo *page*

``` HQL
SHOW TABLES 'page.*';
```

- Mostrar las particiones de la tabla *page_view*

``` HQL
SHOW PARTITIONS page_view;
```

- Mostrar información de las columnas y sus tipos de la tabla *page_view*

``` HQL
DESCRIBE page_view;
```

- Mostrar información más detallada de la tabla *page_view*

``` HQL
DESCRIBE EXTENDED page_view;
```

> [Documentación SHOW](https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DDL#LanguageManualDDL-Show)
>
> [Documentación DESCRIBE](https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DDL#LanguageManualDDL-Describe)

#### Modificación de tablas

- Cambio de nombre de una tabla

``` HQL
ALTER TABLE old_table_name RENAME TO new_table_name;
```

- Modificación de nombres y/o tipos de columnas

``` HQL
ALTER TABLE table_name REPLACE COLUMNS (col1 TYPE, col2 TYPE...);
```

- Añadir columnas a una tabla

``` HQL
ALTER TABLE table ADD COLUMNS (c1 INT, c2 STRING DEFAULT 'def val');
```

- Borrar una tabla

``` HQL
DROP TABLE page_view;
```

- Borrar una partición

``` HQL
ALTER TABLE page_view DROP PARTITION (ds='2008-08-08');
```

> [Documentación ALTER](https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DDL#LanguageManualDDL-AlterTable/Partition/Column)
>
> [Documentación DROP TABLE](https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DDL#LanguageManualDDL-DropTable)

#### Carga de datos

- Copiar datos de un fichero local a una tabla

``` HQL
LOAD DATA LOCAL INPATH '/path/file.txt'
INTO TABLE page_view PARTITION(date='2008-08-08', country='us')
```

- Mover datos de un fichero en HDFS a una tabla

``` HQL
LOAD DATA INPATH '/user/path/file.txt'
INTO TABLE page_view PARTITION(date='2008-08-08', country='us')
```

- Sobrescribir datos de una tabla

``` HQL
LOAD DATA INPATH '/user/path/file.txt'
OVERWRITE INTO TABLE page_view PARTITION(date='2008-08-08', country='us')
```

> [Documentación LOAD DATA](https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DML#LanguageManualDML-Loadingfilesintotables)

#### Exportación de datos

- Copiar datos de una tabla a un fichero HDFS

``` HQL
INSERT OVERWRITE DIRECTORY '/user/path
SELECT name, salary. address FROM employees WHERE se.state = 'CA';
```

- Copiar datos de una tabla a un fichero local

``` HQL
INSERT OVERWRITE LOCAL DIRECTORY '/path
SELECT name, salary. address FROM employees WHERE se.state = 'CA';
```

> [Documentación INSERT DIRECTORY](https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DML#LanguageManualDML-Writingdataintothefilesystemfromqueries)

#### Consultas

Al estar Hive basado en SQL-92, las consultas se realizan de manera muy similar. Se incluyen las siguientes cláusulas:

- SELECT ... FROM
- LIMIT
- AS
- SELECT anidados
- CASE, WHEN, THEN, ELSE
  - Ejemplo:

``` HQL
SELECT name, salary,
  CASE
    WHEN salary < 50000.00 THEN 'low'
    WHEN salary >= 50000.00 AND salary < 70000.00 THEN 'middle'
    ELSE 'high'
  END AS bracket
FROM employees;
```

- WHERE (AND, OR)
- LIKE
- RLIKE y REGEXP (expresiones regulares Java)
  - Ejemplo: direcciones que incluyan Chicago u Ontario

``` HQL
SELECT * FROM employees WHERE address RLIKE '.*(Chicago|Ontario).*';
```

- GROUP BY, HAVING
- JOINS (LEFT, RIGHT, INNER, FULL OUTER)
- ORDER BY (se ordenan los resultados en un reducer)
- SORT BY (cada reducer ordena sus resultados)
- CAST
- UNION ALL

> [Documentación SELECT](https://cwiki.apache.org/confluence/display/Hive/LanguageManual+Select)
