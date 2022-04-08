# Cómo lanzar los ejemplos

Para esta guía tomamos como raíz siempre la carpeta `python` o `scala` de cada ejemplo.

## Python

En Python no hace falta compilación, por lo que basta con lanzar el siguiente comando:

**Linux**

```bash
$SPARK_HOME/bin/spark-submit <ruta_fichero.py> [...parametros_adicionales]
```

**Windows**

```cmd
%SPARK_HOME%\bin\spark-submit <ruta_fichero.py> [...parámetros_adicionales]
```

### Ejemplo

Queremos compilar el programa [Contando M&M](./learning-spark-2nd-edition/capitulo2/contando-m%26m/python/).

Para ejecutarlo, llamamos al siguiente comando desde la carpeta `python`:

**Linux**

```bash
$SPARK_HOME/bin/spark-submit src/mnmcount.py ../mnm_dataset.csv
```

**Windows**

```cmd
%SPARK_HOME%\bin\spark-submit src\mnmcount.py ..\mnm_dataset.csv
```

> El último parámetro es la ruta al dataset `mnm_dataset.csv`, si se encuentra en otra ruta, se debe cambiar por la ubicación en la que esté.

## Scala

En Scala sí hace falta compilar el código. Desde la ruta donde se encuentra el fichero `.sbt`, lanzamos el siguiente comando (hace falta tener instalado *sbt*):

```cmd
sbt clean package
```

A continuación, ejecutamos el siguiente comando:

**Linux**

```bash
$SPARK_HOME/bin/spark-submit --class main.scala.<nombre_del_subpaquete>.<clase> <ruta_al_fichero.jar> [...parámetros_adicionales]
```

**Windows**

```cmd
%SPARK_HOME%\bin\spark-submit --class main.scala.<nombre_del_subpaquete>.<clase> <ruta_al_fichero.jar> [...parámetros_adicionales]
```

### Ejemplo

Queremos compilar el programa [Contando M&M](./learning-spark-2nd-edition/capitulo2/contando-m%26m/scala/).

Nos situamos en la carpeta donde se encuentra el fichero `build.sbt`, y lanzamos el comando `sbt clean package`.

Nuestro archivo compilado se generará en `target/scala-2.12/main-scala-capitulo2_2.12-1.0.jar`

Para ejecutarlo, llamamos al comando:

**Linux**

```bash
$SPARK_HOME/bin/spark-submit --class main.scala.capitulo2.MnMcount target/main-scala-capitulo2_2.12-1.0.jar ../mnm_dataset.csv
```

**Windows**

```cmd
%SPARK_HOME%\bin\spark-submit --class main.scala.capitulo2.MnMcount target\main-scala-capitulo2_2.12-1.0.jar ..\mnm_dataset.csv
```

> El último parámetro es la ruta al dataset `mnm_dataset.csv`, si se encuentra en otra ruta, se debe cambiar por la ubicación en la que esté.
