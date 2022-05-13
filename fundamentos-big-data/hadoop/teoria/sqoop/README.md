# Sqoop

## ¿Qué es Sqoop?

- Sqoop es una herramienta para transferir datos entre bases de datos relacionales (*RDBMs*) y Hadoop
- Originalmente creado por Cloudera, actualmete pertenece a Apache Software Foundation y es Open Source

## Características de Sqoop

- Su objetivo principal es transferir datos entre RDBMS y Hadoop, y permite varias opciones para ello:
  - Transferir solo una tabla.
  - Transferir todas las tablas de una base de datos.
  - Transferir partes de una tabla mediante la cláusula WHERE de SQL

- Importa los datos mediante MapReduce, al que se le puede espeficicar cuántos maps se ejecutarán a la vez; y que provee de paralelización y tolerancia a fallos.
- Usa una interfaz JDBC, por lo que es compatible con casi todas las base4s de datos.
- Los datos se importan a HDFS como text files delimitados o SequenceFiles.
  - Estos ficheros se obtiene mediante MapReduce, por lo que se almacenan en formato part-\*-.0\*
- Permite la importación de datos incrementales, por lo que el primer import toma todas las filas de la tabla y el resto de imports solo toman las filas nuevas.
- El resultado de la importación genera un fichero java.class, que permite reutilizar el código en posteriores MapReduce Jobs y parsear los registros con un formato de texto delimitado.

## Conectores

Existen conectores específicos para diferentes BBDD, cuyo objetivo es proveer de una interfaz más rápida de importación. No suelen ser Open Source, pero sí gratuitos.

Ejemplos:

|Base de datos|Conector|
|-|-|
|HSQLDB|jdbc:hsqldb:*//|
|MySQL|jdbc:mysql://|
|Oracle|jdbc:oracle:*//|
|PostgreSQL|jdbc:postgresql://|

## Sintaxis

La sintaxis básica es la siguiente:

```Sqoop
sqoop <tool-name> [tool-options]
```

Ejemplos de tool-name:

- import
- import-all-tables
- list-tables
- export

Ejemplos de tool-options:

- \-\-conect
- \-\-username
- \-\-password

## Ejemplos de funcionamiento

### Importar una tabla de una base de datos a HDFS

Importa la tabla empleados de la base de datos personal, a través de MySQL. El resultado lo guarda en /tmp/empleados

```Sqoop
sqoop import --username <usuario base de datos> --password <contraseña base de datos> \ --connect jdbc:mysql://database.example.com/personal \ --table empleados --where 'edad>35' --target-dir /tmp/empleados
```

### Importar tabla directamente a Hive

Importa la tabla *prueba* de la base de datos *pruebadb* en la tabla *tabla_prueba_hive* de la base de datos *prueba_sqoop_hive* sobrescribiendo los datos previos.

```Sqoop
sqoop import --username user --password pass \ --connect jdbc:mysql://database.example.com/pruebadb \ --table prueba
--hive-import --hive-overwrite --hive-table prueba_sqoop_hive.tabla_prueba_hive
```

> [Documentación de Sqoop](https://sqoop.apache.org/docs/1.4.6/SqoopUserGuide.html)
