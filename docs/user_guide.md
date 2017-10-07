[Home](../README.md) | [Installation Guide](installation.md) | [__User Guide__](user_guide.md) | [Command Line Reference](cli_reference.md) | [Developer Guide](developer_guide.md)

# User Guide

GRNBoost is an [Apache Spark](http://spark.apache.org/) Application library. A Spark application entails a program, bundled as a [.jar file](https://en.wikipedia.org/wiki/JAR_(file_format)), that can be launched by [submitting](http://spark.apache.org/docs/latest/submitting-applications.html#launching-applications-with-spark-submit) it to a Spark instance.

1. [Running in local mode](#1-running-grnboost-in-local-mode)
2. [Running on Amazon Elastic MapReduce](#2-running-grnboost-on-amazon-elastic-mapreduce)

## 1 Running in local mode

Although Spark was designed to run on a multi-node compute cluster, it is also capable to make good use of the resources of a (preferably powerful) computer with one or more physical CPUs, like a single cluster node. In this case we can simply install Spark in a local folder and [submit](http://spark.apache.org/docs/latest/submitting-applications.html#launching-applications-with-spark-submit) GRNBoost as a Spark job to that instance.

1.

## 2 Running on Amazon Elastic MapReduce

Following steps walk through launching GRNBoost on Amazon Elastic MapReduce. Be aware that this can incur a monetary cost, options to
