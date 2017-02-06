import sbt.Keys._

organization := "org.tmoerman"
name := "brassica"
version := "1.0"

scalaVersion := "2.11.8"
sparkVersion := "2.0.2"
sparkComponents ++= Seq("mllib")

javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled")
parallelExecution in Test := false

// See http://stackoverflow.com/questions/28565837/filename-too-long-sbt
scalacOptions ++= Seq("-Xmax-classfile-name","78")

resolvers += Resolver.mavenLocal

// Change this to another test framework if you prefer
libraryDependencies ++= Seq(

  // built from source and installed with `mvn -DskipTests install`
  "ml.dmlc" % "xgboost4j"       % "0.7",
  "ml.dmlc" % "xgboost4j-spark" % "0.7",

  "org.scalatest"          %% "scalatest"          % "2.2.4"       % "test",
  "com.holdenkarau"        %% "spark-testing-base" % "2.0.0_0.6.0" % "test"

)