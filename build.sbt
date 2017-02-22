import sbt.Keys._

organization := "org.tmoerman"
name := "brassica"
version := "1.0"

scalaVersion := "2.11.8"
sparkVersion := "2.0.2"
sparkComponents ++= Seq("core", "mllib", "sql")

javaOptions ++= Seq("-Xms1G", "-Xmx8G", "-XX:MaxPermSize=8G", "-XX:+CMSClassUnloadingEnabled")
parallelExecution in Test := false

// See http://stackoverflow.com/questions/28565837/filename-too-long-sbt
scalacOptions ++= Seq("-Xmax-classfile-name","78")

resolvers += Resolver.mavenLocal

resolvers += "Spark Packages Repo" at "http://dl.bintray.com/spark-packages/maven"

// Change this to another test framework if you prefer
libraryDependencies ++= Seq(

  "ml.dmlc" % "xgboost4j"       % "0.7",
  "ml.dmlc" % "xgboost4j-spark" % "0.7",

  "com.esotericsoftware.kryo" % "kryo"       % "2.21", // old version, cfr. XGBoost
  "LLNL"                      % "spark-hdf5" % "0.0.4",

  "com.jsuereth"           %% "scala-arm"          % "2.0",

  "org.apache.spark"       %% "spark-hive"         % "2.0.0"       % "test",
  "org.scalatest"          %% "scalatest"          % "2.2.1"       % "test",
  "com.holdenkarau"        %% "spark-testing-base" % "2.0.0_0.6.0" % "test"

)