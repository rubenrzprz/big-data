# Guía Práctica de Kafka

Este documento tiene como objeto mostrar operaciones de Presto con Kafka. Para ello, emplearemos un máquina virtual Ubuntu con Kafka 2.1.0.

## Arrancar los servicios

Para comenzar a usar Kafka, debemos arrancar los servicios de Zookeeper y Kafka Server. Nos situamos en la carpeta raíz de Kafka y en dos consolas diferentes lanzamos los siguientes comandos:

Zookeeper

```sh
zookeeper-server-start.sh config/zookeeper.properties
```

Kafka

```sh
kafka-server-start.sh config/server.properties
```

En una tercera consola creamos un nuevo topic, que llamaremos *text-topic*:

```sh
kafka-topics.sh --zookeeper 127.0.0.1:2181 --topic text_topic --create --partitions 3 --replication-factor 1
```

En esta misma consola lanzamos un producer e introducimos mensajes en el topic que acabamos de crear:

```sh
kafka-console-producer.sh --broker-list 127.0.0.1:9092 --topic text_topic
Hola mundo
Holita mundo
Tercera frase
Cuarta y telón
```

## Configurar Servidor Presto

En una cuarta consola, configuraremos Presto. Para empezar, accedemos a la carpeta de Presto:

```sh
cd ~/presto-server-0.213
```

Y crearemos el fichero kafka.properties dentro del directorio etc/catalog, mediante *vi* o *nano*:

```sh
mkdir etc/catalog
nano etc/catalog/kafka.properties
```

Le añadiremos las siguientes líneas:

```properties
connector.name=kafka
kafka.nodes=localhost:9092
kafka.table-names=text_topic
kafka.hide-internal-columns=false
```

Guardamos el fichero y arrancamos el servidor de Presto:

```sh
bin/launcher start
```

## Cliente de Presto

Abrimos otra consola y lanzamos el cliente de consulta de línea de comandos de Presto:

```sh
~/presto --catalog kafka --schema default
```

### Listar tablas y describirlas

Para listar las tablas lanzamos la instrucción:

```sql
SHOW TABLES;
```

```sql
presto:default> SHOW TABLES;
   Table    
------------
 text_topic 
(1 row)

Query 20220526_110326_00004_6qwfq, FINISHED, 1 node
Splits: 19 total, 19 done (100,00%)
```

Si queremos una descripción de la tabla lanzamos el comando:

```sql
DESCRIBE text_topic;
```

```sql
presto:default> DESCRIBE text_topic;
      Column       |  Type   | Extra |                   Comment          
-------------------+---------+-------+------------------------------------
 _partition_id     | bigint  |       | Partition Id                       
 _partition_offset | bigint  |       | Offset for the message within the p
 _segment_start    | bigint  |       | Segment start offset               
 _segment_end      | bigint  |       | Segment end offset                 
 _segment_count    | bigint  |       | Running message count per segment  
 _message_corrupt  | boolean |       | Message data is corrupt            
 _message          | varchar |       | Message text                       
 _message_length   | bigint  |       | Total number of message bytes      
 _key_corrupt      | boolean |       | Key data is corrupt                
 _key              | varchar |       | Key text                           
 _key_length       | bigint  |       | Total number of key bytes          
(11 rows)

Query 20220526_110337_00005_6qwfq, FINISHED, 1 node
Splits: 19 total, 19 done (100,00%)
0:07 [11 rows, 1,04KB] [1 rows/s, 147B/s]
```

Este comando nos mostrará únicamente las consultas internas, es decir, las relacionadas con Kafka.

### Consultas

Consultamos los mensajes del topic a través de la columna ```_message```:

```sql
SELECT _message FROM text_topic;
```

```sql
presto:default> SELECT _message FROM text_topic;
    _message    
----------------
 Hola mundo     
 Cuarta y telón 
 Tercera frase  
 Holita mundo   
(4 rows)

Query 20220526_110406_00006_6qwfq, FINISHED, 1 node

```

Probamos a introducir más texto desde el producer:

```txt
Buenas tardes
Sandwich
眠い猫眠い猫

```

Y volvemos a lanzar la consulta desde el cliente de Presto, obteniendo como resultado:

```sql
presto:default> SELECT _message FROM text_topic;
    _message    
----------------
 Holita mundo   
 Buenas tardes  
 Hola mundo     
 Cuarta y telón 
 眠い猫眠い猫   
 Tercera frase  
 Sandwich       
(7 rows)
```

## Trabajando con CSV

Empezaremos creando un nuevo topic, *csv_topic*. Para ello, cancelamos el producer que envíaba los mensajes a *text_topic* y creamos el topic con factor de replicación 1 y 3 particiones.

```sh
kafka-topics.sh --zookeeper 127.0.0.1:2181 --topic csv_topic --create --replication-factor 1 --partitions 3 
```

Y arrancamos el producer para insertar los elementos en el topic que acabamos de crear:

```sh
kafka-console-producer.sh --broker-list 127.0.0.1:9092 --topic csv_topic 
```

Añadimos un elemento al topic en formato CSV. Por ello, hay que tener especial atención a las comillas:

```sh
"Shelly","21","60","C/ Fantástico 4, 20A"
```

El siguiente paso será detener el servidor Presto.

```sh
bin/launcher stop
```

Una vez hecho, editamos el fichero kafka.properties
> ~/presto-server-0.213/etc/catalog/kafka.properties

Y añadimos el topic *csv_topic* a la propiedad ```kafka.table-names```:

```properties
kafka.table-names=text_topic,csv_topic
```

Guardamos el fichero y volvemos a arrancar el servidor Presto:

```sh
~/presto-server-0.213/bin/launcher start
```

Ahora, desde el cliente, mostramos las tablas y describimos la tabla *csv_topic*:

```sql
SHOW TABLES;
DESCRIBE csv_topic;
```

Posteriormente, comprobaremos los mensajes del topic:

```sql
SELECT _message FROM csv_topic;
```

```sql
presto:default> SELECT _message FROM csv_topic;
                 _message                  
-------------------------------------------
 "Shelly","21","60","C/ Fantástico 4, 20A" 
(1 row)

```

Como podemos observar, el mensaje se muestra pero en formato texto, ya que no hemos definido el esquema de la tabla.

Para definirlo, paramos el servidor Presto y creamos un fichero JSON con la estructura de la tabla:

```sh
mkdir etc/kafka
nano etc/kafka/csv_topic.json
```

En este fichero definiremos la tabla, asignandole un nombre a cada campo delimitado:

```json
{
    "tableName": "csv_topic",
    "topicName": "csv_topic",
    "schemaName": "default",
    "message": {
        "dataFormat": "csv",
        "fields": [
            {
                "name": "nombre",
                "mapping": 0,
                "type": "VARCHAR"
            },
            {
                "name": "edad",
                "mapping": 1,
                "type": "INT"
            },
            {
                "name": "peso",
                "mapping": 2,
                "type": "DOUBLE"
            },
            {
                "name": "direccion",
                "mapping": 3,
                "type": "VARCHAR"
            }
        ]
    }
}
```

Una vez guardado el fichero, lanzamos de nuevo el servidor:

```sh
bin/launcher start
```

Y comprobamos de nuevo la definición de la tabla *csv_topic*:

```sql
DESCRIBE csv_topic;
```

Y comprobaremos que ahora nos aparecen los campos que añadimos en el esquema:

```sql
presto:default> DESCRIBE csv_topic;
      Column       |  Type   | Extra |                   Comment                   
-------------------+---------+-------+---------------------------------------------
 nombre            | varchar |       |                                             
 edad              | integer |       |                                             
 peso              | double  |       |                                             
 direccion         | varchar |       |                                             
 _partition_id     | bigint  |       | Partition Id                                
 _partition_offset | bigint  |       | Offset for the message within the partition 
 _segment_start    | bigint  |       | Segment start offset                        
 _segment_end      | bigint  |       | Segment end offset                          
 _segment_count    | bigint  |       | Running message count per segment           
 _message_corrupt  | boolean |       | Message data is corrupt                     
 _message          | varchar |       | Message text                                
 _message_length   | bigint  |       | Total number of message bytes               
 _key_corrupt      | boolean |       | Key data is corrupt                         
 _key              | varchar |       | Key text                                    
 _key_length       | bigint  |       | Total number of key bytes                   
(15 rows)
```

Y ahora podemos lanzar una consulta select sobre los campos de la tabla:

```sql
SELECT nombre, edad, peso, direccion FROM csv_topic;
```

```sql
 nombre | edad | peso |      direccion       
--------+------+------+----------------------
 Shelly |   21 | 60.0 | C/ Fantástico 4, 20A 
```

Si añadimos más mensajes a través del producer y volvemos a lanzar la consulta:

```csv
"Byron","56","62.5","C/ Error 25"
"Griff","34","54","C/ Descuento 2"
```

```sql
SELECT nombre, edad, peso, direccion FROM csv_topic;
```

Observamos que se añaden correctamente:

```sql
 nombre | edad | peso |      direccion       
--------+------+------+----------------------
 Shelly |   21 | 60.0 | C/ Fantástico 4, 20A 
 Byron  |   56 | 62.5 | C/ Error 25          
 Griff  |   34 | 54.0 | C/ Descuento 2       
(3 rows)
```

## Trabajando con JSON

Comenzamos creando un nuevo topic llamado *json_topic* con las mismas propiedades que los anteriores:

```sh
kafka-topics.sh --zookeeper 127.0.0.1:2181 --topic json_topic --create --replication-factor 1 --partitions 3 
```

Detenemos el producer que teníamos activo anteriormente y lo lanzamos de nuevo para que lance mensajes a *json_topic*:

```sh
kafka-console-producer.sh --broker-list 127.0.0.1:9092 --topic json_topic
```

Insertamos nuestro JSON en una única línea, ya que los registros no pueden ocupar más de una línea:

```json
{"nombre":"Shelly","edad":21,"peso":60,"direccion":"C/ Falsa 123"}
```

Detenemos el servidor Presto y configuramos de nuevo el etc/catalog/kafka.properties para añadir la nueva tabla a la lista:

```properties
kafka.table-names=text_topic,csv_topic,json_topic
```

Antes de iniciar de nuevo el servidor, añadiremos la definición de la tabla, como hicimos en el apartado anterior. Para ello, creamos el fichero etc/kafka/json_topic.json y le añadimos el siguiente contenido:

```json
{
    "tableName": "json_topic",
    "topicName": "json_topic",
    "schemaName": "default",
    "message": {
        "dataFormat": "json",
        "fields": [
            {
                "name": "nombre",
                "mapping": "nombre",
                "type": "VARCHAR"
            },
            {
                "name": "edad",
                "mapping": "edad",
                "type": "INT"
            },
            {
                "name": "peso",
                "mapping": "peso",
                "type": "DOUBLE"
            },
            {
                "name": "direccion",
                "mapping": "direccion",
                "type": "VARCHAR"
            }
        ]
    }
}
```

Ahora iniciamos el servidor:

```sh
bin/launcher start
```

Y lanzamos una consulta sobre la tabla *json_topic*:

```sql
SELECT nombre, edad, peso, direccion FROM json_topic;
```

```sql
 nombre | edad | peso |  direccion   
--------+------+------+--------------
 Shelly |   21 | 60.0 | C/ Falsa 123 
```

Como ejercicio, podemos añadir unos cuantos registros más a través del producer y realizar consultas con cláusulas WHERE:

```json
{"nombre":"Byron","edad":56,"peso":65,"direccion":"C/ Falsa 123"}
{"nombre":"Griff","edad":35,"peso":57,"direccion":"C/ Falsa 321"}
{"nombre":"Mimichi","edad":5,"peso":8, "direccion":"C/ Camita 1"}
```

Seleccionamos los mayores de 30:

```sql
SELECT nombre, edad, peso, direccion FROM json_topic WHERE edad > 30;
```

```sql
 nombre | edad | peso |  direccion   
--------+------+------+--------------
 Griff  |   35 | 57.0 | C/ Falsa 321 
 Byron  |   56 | 65.0 | C/ Falsa 123 
```

Seleccionamos los registros cuya dirección comience con "C/ Falsa":

```sql
SELECT nombre, edad, peso, direccion FROM json_topic WHERE direccion LIKE 'C/ Falsa%';
```

```sql
 nombre | edad | peso |  direccion   
--------+------+------+--------------
 Shelly |   21 | 60.0 | C/ Falsa 123 
 Byron  |   56 | 65.0 | C/ Falsa 123 
 Griff  |   35 | 57.0 | C/ Falsa 321 
```
