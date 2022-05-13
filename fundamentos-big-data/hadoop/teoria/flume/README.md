# Flume

## ¿Qué es Flume?

- Flume es una herramienta distribuida cuya función es recopilar, agregar y mover de manera eficiente grandes cantidades de datos desde diversas fuentas a un almacén de datos centralizado.
- Posee una arquitectura sencilla y flexible basada en flujos de datos en streaming.
- Es tolerante a fallos, contando con varios mecanismos de configuración.
- Usa la transaccionalidad para la comunicación entre sus componentes.

## Componentes de Flume

Flume se divide en los siguientes componentes principales:

- **Event**: unidad de datos que se propaga a través de la arquitectura.
- **Source**: origen de los datos.
- **Sink**: destino de los datos.
- **Channel**: buffer de almacenamiento intermedio que conecta el source con el sink.

## Funcionamiento de Flume

Flume funciona como un agente en la máquina en la que se quiere recoger la información, a través de un fichero de configuración local.

- Este fichero contiene las propiedades de los sources, channels y sinks; que son un nombre, un tipo y un set de propiedades.
- Cada agente debe conocer cada componente y la forma en la que se relacionan, es decir, debe conocer el flujo de los datos (flow).
- Para iniciar un agente, se lanza un script en el directorio de instalación de flume, indicando el nombre del agente y el archivo de configuración del mismo.

```console
bin/flume-ng agent -n <nombre_del_agente> -c conf -f <ruta_fichero_configuración>
```

## Opciones de configuración

- Un único flujo/agente
  - Se conecta el source con el sink a través del channel.
  - Un source puede estar conectado a varios channels, pero un sink solo puede estar conectado a un channel.
- Múltiples agentes
  - Requiere el que el sink del agente previo y el source del actual sean de tipo AVRO (sistema de serialización de datos).
  - Ejemplo: múltiples webs enviando logs a un sistema HDFS.
- Múltiples flows
  - Su objetivo es multiplexar los eventeos a uno o más destinatarios según las condiciones especificadas en el fichero de configuración.
  - Existen dos tipos: replica, en el que todos los eventos se mandan sobre todos los canales; y multiplexación, en el que los eventos se mandan sobre ciertos canales en función de las propiedades de selección.

## Detalle de configuración

Esquema del fichero de configuración:

```config
# Propiedades de los sources
<Agent>.sources.<Source>.<someProperty> = <someValue>
#  Propiedades de los channels
<Agent>.channels.<Channel>.<someProperty> = <someValue>
#  Propiedades de los sinks
<Agent>.sinks.<Sink>.<someProperty> = <someValue>
```

- Una de las propiedades más importantes para configurar es "type", que indica qué tipo de componente es.

Ejemplo de dos source conectados a dos sinks a través de dos channels:

```config
# sources, sinks and channels en el agente
agent_foo.sources = avro-AppSrv-source exec-tail-source2
agent_foo.sinks = hdfs-Cluster1-sink1 avro-forward-sink2
agent_foo.channels = mem-channel-1 file-channel-2

# configuración del flow #1
agent_foo.sources.avro-AppSrv-source1.channels = mem-channel-1
agent_foo.sinks.hdfs-Cluster1-sink1.channel = mem-channel-1

# configuración del flow #2
agent_foo.sources.exec-tail-source2.channels = file-channel-2
agent_foo.sinks.avro-forward-sink2.channel = file-channel-2
```

### Sources

Desde el punto de vista funcional, existen de dos tipos:

- *EvenDriverSource*: son activos, por lo que controlan cómo y cuándo se añaden los eventos al channel.
- *PollableSource*: son pasivos, de modo que no existe manera de saber cuándo llega un nuevo dato que debe ser enviado a través del flow. Por ello, Flume debe hacerles 'pull' cada cierto tiempo. Son fáciles de configurar pero ofrecen un menor control.

#### Sources predefinidos

- **Avro**: escucha de un puerto Avro y recibe eventos desde streams de clientes externos Avro.
- **Thrift**: similar a Avro, puede autentificarse usando Kerberos.
- **Exec**: ejecuta comandos Unix al iniciar la fuente.
- **JMS**: leen mensajes de una cola.
- **Spooling directory**: lee desde ficheros que han sido movidos a un directorio. Una vez se ha enviado, el fichero se marca como 'ya leído'.
- **Twitter**: conecta a la API de streaming de Twitter.
- **Kafka**: se leen topics de mensajes almacenados en Kafka.
- **Syslog**: leen datos de los syslog de la máquina.
- **Http**: acepta eventos desde peticiones HTTP GET o POST.
- **Custom**: permite implentar clases Java propias.
- **Stress**: simula un test de carga.

### Sinks predefinidos

- **HDFS**: almacena eventos en el sistema de ficheros de Hadoop, tanto en formato text como SequenceFile.
- **Hive**: alamacena eventos en formato texto o JSON en tablas o particiones de Hive.
- **Logger**:
- **Avro**: almacena los evento sobre un host de Avro.
- **Thrift**: almacena los evento sobre un host de Thrift.
- **FileRoll**: almacena eventos en el sistema de ficheros local.
- **Null**: descarta los eventos.
- **Hbase**: almacena los eventos en una base de datos Hbase.
- **Custom**: se pueden construir sinks específicos.

### Channels predefinidos

- **Memoria**: se almacenan en una cola de memoria de tamaño predefinido.
- **JDBC**: los eventos persisten sobre una base de datos.
- **Kafka**: los eventos persisten en un cluster de Kafka, proporcionando alta disponiblidad y replicación.
- **File**: los eventos son guardados en un fichero en el sistema local.
- **Spillable Memory**: los eventos se guardan por defecto en una cola de memoria, que si ésta se sobrecargase, pasarían a guardarse en disco.
- **Custom Channel**: permite implementar un channel propio.

### Interceptores

Mediante los interceptores, Flume puede modificar eventos en caliente. Si hay varios interceptores, se ejecutan en el orden en el que se han definido.

#### Interceptores predefinidos

- **Timestamp**: inserta un timestamp en la cabecera de los eventos.
- **Host**: añade el host o su IP al evento.
- **Static**: añade una cabecera fija a los eventos.
- **UUID**: añade un identificador único a la cabecera.
- **Morphline**: permite hacer transformaciones predefinidas en un fichero de configuración.
- **Search&Replace**: busca cadenas en el evento y las remplaza por otras.
- **Regex**: utiliza una expresión regular para extraer datos.
