name := "my_udf"

version := "1.0"

scalaVersion := "2.11.12"

// add spark.sql as dependencies (no need spark-core)
// check scalarVersion and spark version by running spark-shell (the splash screen)
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.0"

// skip the errors: [error] deduplicate: different file contents found...
assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
    case x => MergeStrategy.first
}
