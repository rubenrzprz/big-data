//Nombre del paquete
name := "main/scala/capitulo2B"
//Version del paquete
version := "1.0"
//Version de Scala
scalaVersion := "2.12.10"
// Dependencias de librerias
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.0.0",
  "org.apache.spark" %% "spark-sql"  % "3.0.0"
)