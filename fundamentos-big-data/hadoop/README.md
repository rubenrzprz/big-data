# Hadoop

## ¿Qué es Hadoop?

Hadoop es un framework Open Source que permite almacenar datos y ejecutar aplicaciones en clústeres de hardware usando discos.

> **Clúster**
> Un clúster es un conjunto de servidores (nodos) que trabajan conjuntamente para alcanzar un objetivo común.
> Se componen de dos tipos de nodos:
>
> - Maestro *(master)*: controla el clúster
> - Esclavos *(slaves)*: procesan y almacenan la información

## Características

- Inspirado en los documentos de Google para MapReduce y Google File System.
- Programado en Java, aunque existen herramientas para interactuar con Hadoop mediante otros lenguajes.
- Escalable horizontalmente con un coste bajo para procesar más datos y añadir más potencia de procesamiento.
- Es tolerante a fallos, por lo que aunque un servidor falle, los sistemas siguen funcionando y no hay pérdida de datos.

## Componentes

Hadoop se compone de:

- HDFS
- MapReduce
- Ecocistema Hadoop

### HDFS

HDFS es un sistema de archivos escrito en Java nativo de Google, que se despliega sobre el sistema de archivos nativo de cada servidor. Su principal característica es que provee de almacenamiento redundante de grandes volúmenes de datos de manera transparente al usuario, sin necesidad de especificar que se quiere replicar la información.

- HDFS está pensado para escribir una única vez y leer muchas, por lo que para modificar archivos se elimina el anterior y se copia nuevamente actualizado.

- Trabaja con discos ópticos y funciona mejor con un volumen de archivos del orden de millones y que cada uno pese de más de 100MB.

---

#### Demonios de HDFS

##### Nodo master

- NameNode
  - Almacena y gestiona los metadatos
  - Contiene la información de los permisos y usuarios
  - Sabe qué bloques componen un archivo y dónde se encuentra cada bloque
- Secondary NameNode
  - Realiza labores de mantenimiento
- Standby NameNode
  - Usado en arquitecturas de alta disponiblidad
  - Realiza labores de mantenimiento, relevando a Secondary Namenode cuando es necesario automáticamente

##### Nodos esclavos

- DataNode
  - Gobierna los nodos, que almacenan los bloques de datos
  - Se encarga de acceder a los bloques
  - Cada bloque se almacena varias veces, dependiendo del factor de replicación
  - Gestionan las *tasks* que componen un *job*

---

#### Procedimiento de escritura en HDFS

1. El cliente conecta con el NameNode.
2. El NameNOde busca en sus metadatos y devuelve el nombre del bloque y la lista de los DataNode.
3. El cliente conecta con el primer DataNode de la lista y comienza el proceso de envío de datos.
4. El primer DataNode conecta con el segundo DataNode y realiza el envío, y este proceso se repite hasta que se alcance el factor de replicación.
5. Finaliza el envío de los datos.
6. El cliente indica al NameNode dónde ha realizado la escritura.

---

#### Procedimiento de lectura en HDFS

1. El cliente conecta con el NameNode.
2. El NameNode devuelve una lista de DataNode que contienen ese bloque.
3. El cliente conecta con el primer DAtaNode de la lista y comienza la lectura del bloque.

---

#### Comandos de HDFS

Las operaciones con HDFS se pueden realizar a través de la línea de comandos, que comparten muchas similitudes con los de LInux:

- Copiar un fichero del disco local a HDFS

> hadoop fs -put <ruta_local> <ruta_hdfs>

- Copiar un fichero de HDFS a local

> hadoop fs -put <ruta_local> <ruta_hdfs>

- Lista directorios

> hadoop fs -ls \<directorio>

- Mostrar el contenido de un fichero

> hadoop fs -cat <ruta_fichero>

- Crear un directorio

> hadoop fs -mkdir <ruta_directorio>

- Borrar un directorio y su contenido

> hadoop fs -rm <ruta_directorio>

### MapReduce

MapReduce es un paradigma de programación que comprende las siguientes tres etapas:

1. Fase Map
2. Fase Shuffle&Sort
3. Fase Reduce

Además es un método que distribuye y paraleliza las tareas a lo largo del clúster automáticamente. Está pensado para ser una abstracción para los programadores, que tan solo deben encargarse de definir el *Mapper*, el *Reducer* y el *Driver*.

#### Mapper

- Actúa sobre cada registro de entrada
- Cada *task* actúa sobre un único bloque de HDFS y en el nodo en el que está almacenado ese bloque
- Su salida es un par clave/valor

#### Shuffle&Sort

- Ordena por claves y agrupa los datos de todos los *Mappers* con una misma clave (clave, \[valor<sub>1</sub> … valor<sub>n</sub>])
- Ocurre después de que hayan terminado todos los *Mappers* y antes que comiencen los *Reducers*

#### Reducer

- Realiza operaciones con la salida de *Shuffle&Sort*
- Produce la salida final

Ejemplo: Contar palabras

[Ejemplo Map Reduce](images/ejemplo-map-reduce.jpg)

---

#### Demonios de MapReduce V2-Yarn

- Resource Manager
  - Existe uno por clúster
  - Su función es dotar de recuersos (CPU y RAM) a los nodos esclavos para que desempeñen su trabajo y arrancar los *Application Masters*
- Application Masters
  - Existe uno por *job*
  - Pide recursos y gestiona los *tasks map* y *reduce* de los nodos en los que se ejecutan las tareas asociadas al *job*
- Node Manager
  - Existe uno por cada nodo esclavo
  - Se encarga de gestionar los recursos de su nodo
- Containers
  - Conjunto de recursos cedidos por el *Resource Manager*
- Job History
  - Existe uno por clúster
  - Almacena los metadatos y la información de los *jobs*

---

### Ecosistema Hadoop

Conjunto de herramientas que se integran con Hadoop. Estas herramientas surgieron por diversos motivos:

- Hadoop por sí solo no es suficiente para entornos de trabajo en producción
- Muchos programadores no estaban acostumbrados a programar en Java
- Estas herramientas ofrecen muchísimas más posibilidades de exploración de los datos

#### Principales herramientas del Ecosistema Hadoop

##### Hive

- Software para consultar y manipular datos escritos en HDFS
- Creado por Facebook
- Permite trabajar con datasets en un lenguaje similar a SQL llamado HQL
- Traduce queries HQL a MapReduce

##### Pig Hadoop

- Plataforma para analizar grandes datasets almacenados en HDFS
- Creado por Yahoo!
- Usa un lenguaje propio llamado Pig Latin, similar a HQL
- Traduce consultas Pig Latin a MapReduce

##### Sqoop

- Herramienta que obtiene datos desde BBDD relacionales y las almacena en HDFS
- Originalmente creado por Cloudera
- Existen conectores con casi todas las bases de datos relacionales
- Su sintaxis consta de una parte en SQL y una parte de configuración

##### Flume

- Herramienta diseñada para importar datos a un clúster en tiempo real
- Se usa para datos de orígenes diferentes a BBDD relacionales
- Originalmente creado pro Cloudera

##### Kafka

- Proyecto que pretende proporcionar una plataforma unificada, de baja latencia y alto rendimiento para la manipulación en tiempo real de fuentes de datos
- Desarrollada por Linkedin
- Está concebida como un registro de transacciones distribuidas

##### Bases de datos No-SQL

- No usan por defecto el lenguaje SQL para realizar consultas
- Trabajan con datos multiestructurados
- Altamente escalables horizontalmente
- Permiten distribución de datos y ejecución en paralelo
- No garantizan ACID (atomicidad, consistencia, aislamiento y durabilidad) de los datos
- Están pensadas para millones de columnas con billones de filas

> |Tipo|Ejemplo|
> |--|--|
> |Clave-Valor|Cassandra|
> |Orientadas a documentos|MongoDB|
> |Orientadas a grafos|Neo4j|
> |Orientadas a columnas de clave-valor|Hbase|
> |Otras|Rocket D3 DBMS, ObjectDB, Jbase...|
