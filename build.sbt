import sbt.util

name := "appsflyer"

version := "0.1"

scalaVersion := "2.12.8"

val sparkVersion = "2.4.3"

javaOptions in run ++= Seq("-Xmx4G", "-Xms1G", "-XX:+CMSClassUnloadingEnabled")
fork in run := true
connectInput in run := true
outputStrategy := Some(StdoutOutput)
logLevel in run := util.Level.Error
showTiming := true

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion
)
