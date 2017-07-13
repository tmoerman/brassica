import sbt.Keys._

organization := "org.aertslab"
name         := "GRNBoost"
description  := "A scalable gene regulatory network inference library"

scalaVersion := "2.11.11"
sparkVersion := "2.1.0"
sparkComponents ++= Seq("core", "mllib", "sql", "hive")

javaOptions ++= Seq("-Xms1G", "-Xmx8G", "-XX:MaxPermSize=8G", "-XX:+CMSClassUnloadingEnabled")
parallelExecution in Test := false

// See http://stackoverflow.com/questions/28565837/filename-too-long-sbt
scalacOptions ++= Seq("-Xmax-classfile-name","78")

resolvers += Resolver.mavenLocal
resolvers += Resolver.bintrayRepo("bkirwi", "maven")
resolvers += "Spark Packages Repo" at "http://dl.bintray.com/spark-packages/maven"

libraryDependencies ++= Seq(

  "ml.dmlc"        % "xgboost4j"  % "0.7"   exclude("com.esotericsoftware.kryo", "kryo"),

  "com.jsuereth"  %% "scala-arm"  % "2.0",
  "com.eharmony"   % "spotz-core" % "1.0.0",

  "com.softwaremill.quicklens" %% "quicklens" % "1.4.8",
  "com.github.scopt"           %% "scopt"     % "3.6.0",

  "org.scalactic" %% "scalactic"  % "3.0.1",
  "LLNL"           % "spark-hdf5" % "0.0.4"  % "provided",

  //"com.holdenkarau" %% "spark-testing-base" % "2.0.0_0.6.0" % "test"
  "com.holdenkarau" %% "spark-testing-base" % "2.1.1_0.7.1" % "test"

)

// assembly config
assemblyJarName := "GRNBoost.jar"
test in assembly := {}