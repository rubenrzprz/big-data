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

#### Procedimiento de escritura en HDFS

1. El cliente conecta con el NameNode.
2. El NameNOde busca en sus metadatos y devuelve el nombre del bloque y la lista de los DataNode.
3. El cliente conecta con el primer DataNode de la lista y comienza el proceso de envío de datos.
4. El primer DataNode conecta con el segundo DataNode y realiza el envío, y este proceso se repite hasta que se alcance el factor de replicación.
5. Finaliza el envío de los datos.
6. El cliente indica al NameNode dónde ha realizado la escritura.

#### Procedimiento de lectura en HDFS

1. El cliente conecta con el NameNode.
2. El NameNode devuelve una lista de DataNode que contienen ese bloque.
3. El cliente conecta con el primer DAtaNode de la lista y comienza la lectura del bloque.

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
