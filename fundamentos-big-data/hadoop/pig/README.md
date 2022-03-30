# Pig Hadoop

## ¿Qué es Pig Hadoop?

- Pig Hadoop es una plataforma de alto nivel para crear programas MapReduce en Hadoop.
- Emplea el lenguaje Pig Latin, que abstrae la programación del MapReduce de Java en una notación de alto nivel que permite manejar el flujo de datos de manera sencilla.
- Fue originalmente desarrollada por Yahoo! y actualmente forma parte de Apache Software Foundation.

## Pig Latin

Es el lenguaje de flujo de datos que emplea Pig. Viene con funciones predefinidas para cargar, filtrar y aplicar transformacionen a los datos; y también permite crear funciones propias desde otros lenguajes como Java, Javscript o Python.

## Sintaxis

### Conceptos

- Identificadores: nombres asignados a campos u otras estructuras.
- *Keywords*: palabras reservadas, no se pueden usar como identificadores.

### Carga de datos

Se emplea la función PigStorage, que viene implícito en la instrucción ```LOAD```.

- Si no se especifica una ruta absoluta, se toma como ruta relativa al directorio home del usuario, que normalmente es */user/\<nombre_usuario>*

Ejemplo de carga del fichero 'sales':

```Pig Latin
allsales = LOAD 'sales' AS (name, price);
```

Pig asume el formato de texto separando las columnas por tabulador, aunque también se pueden específicar separadores mediante un argumento de la función PigStorage:

```Pig Latin
allsales = LOAD 'sales.csv' USING PigStorage(',') AS (name, price);
```

### Almacenamiento de datos

Se pueden tanto mostrar los datos por pantalla ```DUMP```, como enviarlos a un fichero en el disco HDFS ```STORE```.

El comando ```STORE``` funciona igual que ```LOAD```, pero con escritura en lugar de lectura. Es decir, las rutas y los delimitadores se especifican de la misma forma.

Ejemplo de salida del identificador 'allsales':

```Pig Latin
STORE allsales INTO '/tmp/report' USING PigStorage(';');
```

### Tipos de datos

|Tipo|Ejemplo|
|-|-|
|int|2022|
|long|536521412L|
|float|3.14159F|
|double|3.1415926535|
|boolean|true|
|datetime|2022-03-30T12:43:00.00+00:00|
|chararray|Music|
|bytearray||

- Pig trata a los campos no identificados como arrays de bytes (bytearray en Pig).
- Según el contexto, Pig asigna el tipo de datos más adecuado. Por ejemplo, al escribir ```price * 0.1```, Pig asumirá que el tipo de este valor será dobule.
- Sin embargo, es mejor espeificar el tipo de datos siempre que sea posible:

```Pig Latin
allsales = LOAD 'sales.csv' USING PigStorage(',') AS (name:chararray, price:double);
```

- Si hay datos no válidos (por ejemplo, un campo de tipo int que recibe un string), se sustituyen por NULL.

### Filtrado y ordenación de datos

- Se pueden filtrar las tuplas que cumplan un determinado requisito mediante el comando ```FILTER```, permitiendo concatenar varias condiciones mediante ```AND``` y ```OR```.

```Pig Latin
us_gb_big_sales = FILTER allsales BY price > 3000 AND (country == 'US' OR country == 'GB') 
```

- Se pueden comparar todos los tipos de datos mediante el operador ```==```
- También se pueden utilizar expresiones regualres de Java, mediante el operador ```MATCHES```

```Pig Latin
gmail = FILTER senders BY email_adddr MATCHES '.*@gmail\\.com$';
```

- Es posible extraer columnas mediante los operadores ```FOREACH``` y ```GENERATE```, generando nuevos campos, a los que se les puede definir un nombre y un tipo de datos.

```Pig Latin
t = FOREACH allsales GENERATE price * 0.07 AS tax:float;
```

- El comando ```DISTINCT``` elimina los registros duplicados, esto es, las tuplas que tengan todos los campos iguales.

```Pig Latin
unique_prices = DISTINCT t;
```

- Con ```ORDER BY``` se pueden ordenar los registros de una tabla en orden ascendente (por defecto), o descendente indicando el operador ```DESC```.

```Pig Latin
sorted_sales = ORDER allsales BY country DESC;
```

- Con los operadores ```IS NULL``` e ```IS NOT NULL```, se pueden filtrar los datos que sean nulos.

```Pig Latin
has_prices = FILTER allsales BY price IS NOT NULL;
```

### Funciones predefinidas

En Pig existen multitud de funciones ya definidas, que se pueden consultar en la documentación. Ejemplos: AVG, CONCAT, COUNT, MAX, MIN...

> [Documentación de funciones de Pig](https://pig.apache.org/docs/latest/func.html)
