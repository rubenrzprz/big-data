# 2. Instalación pseudo-distribuida

En una instalación pseudo-distribuida (también denominada *single-node cluster*, clúster de un solo nodo), tanto el NameNode como el DataNode residen en la misma máquina.

> [Más información sobre los nodos de Hadoop](../../README.md#nodo-master)

En esta guía, detallaremos como realizar la configuración para usar Hadoop en este modo.

---

En primer lugar, crearemos un directorio denominado *datos* en la raíz de nuestro sistema de ficheros.

```sh
mkdir /datos
```

A continuación, cambiamos el propietario del fichero y el grupo a *hadoop*.

```sh
chown hadoop:hadoop /datos
```

Accederemos al directorio de configuración de Hadoop, que se encuentra en la ruta ```/opt/hadoop/etc/hadoop```.

```sh
cd /opt/hadoop/etc/hadoop
```

Comenzaremos modificando el archivo ```core-site.xml```, que contiene la configuración general del clúster. Se trata de un fichero xml, al que le agregaremos las siguientes líneas:

```xml
<configuration>
    <property>
        <name>fs.defaultFS</name>
        <value>hdfs://nodo1:9000</value>
    </property>
</configuration>
```