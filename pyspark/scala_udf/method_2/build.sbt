name := "my_udf_3"

version := "1.01"

scalaVersion := "2.11.12"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "2.4.0" % "provided"
)
