# Apache Kafka

## Introducción

Apache Kafka es una plataforma de streaming distribuido y de código abierto capaz de manejar trillones de eventos al día. Fue originalmente concebida como una cola de mensajes por LinkedIn en 2011, aunque ha evolucionado rápidamente hasta convertirse en una plataforma de streaming completa.

### Plataforma de streaming distribuido

Cuenta con tres capacidades clave:

- Publicar y suscribirse a flujos de registros, al igual que una cola de mensajes o un sistema de mensajería empresarial.
- Almacena flujos de registros de manera duradera y con tolerancia fallos.
- Procesa los flujos de registros a medida que se producen.

### ¿Por qué Kafka?

Kafka permite desacoplar los flujos de datos y los sistemas y se emplea principalmente en dos amplias clases de aplicaciones:

- Construir flujos de datos en tiempo real para obtener datos confiables entre sistemas o aplicaciones.
- Creación de aplicaciones de streaming en tiempo real que transforman o reaccionan a los flujos de datos.

#### Características

- Arquitectura distribuida y resistente.
- Tolerante a fallos.
- Escalabilidad horizontal.
- Alto rendimiento, con latencias inferiores a los 10 milisegundos.

#### Casos de uso

- Sistemas de mensajería.
- Seguimiento de actividad.
- Recopilación de métricas.
- Recopilación de logs de aplicaciones.
- Integración con Spark, Hadoop, Storm y otras tecnologías Big Data para procesar datos en streaming.

## Conceptos en Kafka

Kafka se ejecuta como un clúster sobre uno o más servidores.

El clúster de Kafka almacena los flujos de registros en categorías llamadas "topics". Cada registro tiene una clave, un valor y una marca de tiempo.

### Topics

Un flujo de datos en particular.

- Similar a una tabla en una base datos (aunque sin todas las restricciones).
- Se identifican por un nombre y se pueden tener tantos como se desee.
- Están divididos en particiones, que son secuencias ordenadas e inmutables de registros. A los registros de particiones se les asigna un número de identificación secuencial llamado offset y que solo tiene significado dentro de una misma partición. El orden de los datos solo se garantiza dentro de una partición.
- Los datos se mantienen por un periodo de tiempo limitado (una semana por defecto).
- Deben tener un factor de replicación > 1, para garantizar la disponibilidad y durabilidad cuando los nodos individuales fallen.

### Brokers

Múltiples servidores que componen el clúster Kafka.

- Se identifican con un ID.
- Cada broker contiene un cierto número de particiones de topics.
- Al conectarse a un broker (llamado bootstrap broker), se está conectado a todo el clúster.
- Para cada partición, un único broker es el líder (puede recibir y servir datos para una partición) y el resto sincronizar los datos (in-sync replica).

### Producers

Los encargados de escribir datos en los topics.

- Conocen en qué broker y partición escribir.
- Si falla un broker, los producers se recuperan automáticamente.
- Pueden enviar una clave con el mensaje:
  - Si la clave es *null*, los datos se envían en round robin.
  - Si se envía una clave, todos los mensajes para esa clave irán siempre a la misma partición.
  - Permite ordenar los datos por un campo en específico.

### Consumers

Leen los datos desde un topic.

- Saben de qué broker leer los datos.
- En caso de fallo de un broker, se recuperan automáticamente.
- Los datos se leen en orden dentro de cada partición.
- Leen los datos en grupos de consumo:
  - Cada consumer de un grupo lee desde unas particiones exclusivas.
  - Si hay más consumers que particiones, algunos consumers estarán inactivos.
  - Usan un GroupCoordinator y un ConsumerCoordinator para asignar las particiones.

#### Semántica de entrega para los consumers

- *At most once*: los offsets se entregan tan pronto como el mensaje es recibido y, si el proceso va mal, el mensaje se pierde y no será leído de nuevo.
- *At least once*: los offsets se entregan después de que el mensaje es procesado y, si éste va mal, se lee de nuevo. Esto puede generar mensajes duplicados.
- *Exactly once*: se asegura de que los mensajes lleguen y no se produzcan duplicados.

### Zookeeper

Almacena metadatos sobre el clúster y gestiona la lista de brokers.

- Es una de las dependencias de Kafka, no puede trabajar sin Zookeeper.
- Por diseño, opera sobre un número impar de servidores.
- Envía notificaciones a Kafka en caso de cambios (nuevo topic, un broker muere, un broker se levanta...).
- Ayuda a realizar la elección de líder para las particiones.
- Tiene un líder que maneja las escrituras y el resto de servidores son seguidores, que manejan las lecturas.
