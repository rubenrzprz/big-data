# Creación de un clúster en Hadoop 2.10.1

En esta práctica, detallaremos paso a paso como instalar y crear un clúster de Hadoop 2.10.1.

Requisitos:
---

- Software de virtualización (en este caso usaremos Oracle VirtualBox)
- Máquina virtual con un sistema operativo Linux. En esta guía emplearemos la distribución [CentOs Linux 7](https://www.centos.org/download/)

## 1. Instalación de Hadoop

Comenzamos arrancando nuestra máquina virtual y accederemos como usuario *root* al sistema.

### 1.1 Crear usuario Hadoop

Ejecutamos el siguiente comando para crear un nuevo usuario llamado *hadoop*:

```sh
useradd hadoop
```

Y le añadimos una contraseña:

```sh
passwd hadoop
```

Comprobamos que efectivamente se ha creado si existe el directorio ```/home/hadoop```:

```sh
ls /home
```

### 1.2 Instalar OpenJDK de Java

Instalaremos la OpenJDK 1.8 de Java mediante el comando:

```sh
yum install java-1.8.0-openjdk-devel
```

Y podemos comprobar que se ha instalado correctamente con el comando:

```sh
java -version
```

Obteniendo la siguiente salida:

```txt
openjdk version "1.8.0_332"
OpenJDK Runtime Environment (build 1.8.0_332-b09)
OpenJDK 64-Bit Server VM (build 25.332-b09, mixed mode)
```

### 1.3 Configurar variables de entorno de Java

A continuación, configuraremos las variables de entorno para el usuario *hadoop*.

Estableceremos las variables en el fichero *.bashrc* de nuestro usuario, que se encuentra en la ruta ```/home/hadoop/.bashrc```.

Añadimos al final del fichero las siguientes líneas:

```sh
export JAVA_HOME=$(readlink -f /usr/bin/javac | sed "s:/bin/javac::")
export PATH=$PATH:$JAVA_HOME/bin
```

Guardamos y reiniciamos la terminal para cargar el fichero *.bashrc* o lo leemos mediante:

```sh
source /home/hadoop/.bashrc
```

### 1.4 Descargar e instalar Hadoop

Comenzamos descargando el fichero binario *.tar.gz* de la versión 2.10.1 de la [página de descargas oficial de Hadoop](https://hadoop.apache.org/releases.html).

Lo copiamos en el directorio ```/opt```:

```sh
cp hadoop-2.10.1-src.tar.gz /opt
```

Y lo descomprimimos:

```sh
cd /opt
tar xvf hadoop-2.10.1.tar.gz
```

Se nos habrá creado el directorio *hadoop-2.10.1*. Para facilitarnos el trabajo, le cambiaremos el nombre a simplemente *hadoop*:

```sh
mv hadoop-2.10.1/ hadoop
```

Cambiamos el propietario de todos los ficheros del directorio *hadoop* a nuestro usuario y grupo. 

```sh
chown -R $USER:$(id -gn) hadoop
```

Y por último, listamos el directorio para comprobar que todo está correcto:

```sh
ls -l hadoop
```


```log
drwxr-xr-x. 2 hadoop hadoop    194 Sep 14  2020 bin
drwxr-xr-x. 3 hadoop hadoop     20 Sep 14  2020 etc
drwxr-xr-x. 2 hadoop hadoop    106 Sep 14  2020 include
drwxr-xr-x. 3 hadoop hadoop     20 Sep 14  2020 lib
drwxr-xr-x. 2 hadoop hadoop    239 Sep 14  2020 libexec
-rw-r--r--. 1 hadoop hadoop 106210 Sep 14  2020 LICENSE.txt
-rw-r--r--. 1 hadoop hadoop  15841 Sep 14  2020 NOTICE.txt
-rw-r--r--. 1 hadoop hadoop   1366 Sep 14  2020 README.txt
drwxr-xr-x. 3 hadoop hadoop   4096 Sep 14  2020 sbin
drwxr-xr-x. 4 hadoop hadoop     31 Sep 14  2020 share
```

### 1.5 Configurar las variables de entorno de Hadoop

Configuraremos las variables de entorno, una vez más, desde el fichero ```~/.bashrc```. Le añadiremos las siguientes líneas:

```sh
export HADOOP_HOME=/opt/hadoop
export PATH=$PATH:/$HADOOP_HOME/bin
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
```

Guardamos y reiniciamos el terminal para cargar el fichero *.bashrc* o ejecutamos el siguiente comando:

```sh
cd
. ./.bashrc
```

Una vez hecho, comprobamos si podemos acceder correctamente a Hadoop mediante:

```sh
hadoop version
```

Obteniendo un resultado similar al siguiente:

```log
Hadoop 2.10.1
Subversion https://github.com/apache/hadoop -r 1827467c9a56f133025f28557bfc2c562d78e816
Compiled by centos on 2020-09-14T13:17Z
Compiled with protoc 2.5.0
From source with checksum 3114edef868f1f3824e7d0f68be03650
This command was run using /opt/hadoop/share/hadoop/common/hadoop-common-2.10.1.jar
```

### 1.6 Configurar SSH

Para trabajar con Hadoop, es necesario configurar la conectividad SSH en todos los nodos, incluso cuando trabajemos con solo uno.

Comenzamos lanzando siguiente comando, dejando todas las opciones que se nos preguntan en blanco:

```sh
ssh-keygen
```

Tras ejecutarlo, se crearán dos ficheros, uno con la clave pública y otro con la privada; en el directorio ```/home/hadoop/.ssh```.

```sh
ls -l .ssh
```

```log
-rw-------. 1 hadoop hadoop 1675 May 30 16:09 id_rsa
-rw-r--r--. 1 hadoop hadoop  394 May 30 16:09 id_rsa.pub
```

A continuación, crearemos el fichero *authorized_keys*, que posteriormente emplearemos para pasar la clave pública al resto de los nodos.

```sh
cd .ssh
cp id_rsa.pub authorized_keys
```

Por último, comprobamos que podemos acceder a través de ssh a nuestro propio nodo:

```sh
ssh nodo1
```
