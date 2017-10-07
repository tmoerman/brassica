# Installation Guide

This page describes how to build and install GRNBoost on different operating
systems

## 1. Download pre-built packages (the easy way)

TODO

## 2. Building from Source (the hard way)

Building GRNBoost from source requires additional software, make sure you have these installed on your system.
1. __[Git](https://git-scm.com/)__ for checking out the code bases from their Github repository.
2. __[SBT](http://www.scala-sbt.org/)__ for building __GRNBoost__ from source.
3. __[Maven](https://maven.apache.org/)__ for building the xgboost Java bindings.
4.


#### Linux (Ubuntu)

On Ubuntu or Debian, using the classic `apt-get` routine is the best option.

* __Git__:  https://www.digitalocean.com/community/tutorials/how-to-install-git-on-ubuntu-14-04

    ```bash
    sudo apt-get update
    sudo apt-get install git
    ```

* __SBT__: http://www.scala-sbt.org/1.x/docs/Installing-sbt-on-Linux.html

    ```bash
    $ echo "deb https://dl.bintray.com/sbt/debian /" | sudo tee -a /etc/apt/sources.list.d/sbt.list
    $ sudo apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 2EE0EA64E40A89B84B2DF73499E82A75642AC823
    $ sudo apt-get update
    $ sudo apt-get install sbt
    ```

* __Maven__: https://www.mkyong.com/maven/how-to-install-maven-in-ubuntu/

    ```bash
    $ sudo apt-get update
    $ sudo apt-get install maven
    ```

#### MacOS

On Mac, we recommend using the [Homebrew](https://brew.sh/) package manager. Other installation options are available, please refer to the software packages' documentation for more info.

* __Git__: `$ brew install git` - https://git-scm.com/book/en/v1/Getting-Started-Installing-Git#Installing-on-Mac
* __SBT__: `$ brew install sbt` - http://www.scala-sbt.org/0.13/docs/Installing-sbt-on-Mac.html
* __Maven__: `$ brew install maven` - http://brewformulas.org/Maven

### 2.1 Building xgboost

GRNBoost depends upon xgboost, so we build this first. Xgboost is essentially a C++ library with Java, Scala, R and Python wrappers. We first compile the C++ part using the gcc compiler, and then build and install the Java components using Maven. The following instructions contains excerpts from the [xgboost build guide](https://xgboost.readthedocs.io/en/latest/build.html).

1. Using git, [clone](https://git-scm.com/docs/git-clone) the xgboost Github repository:

    ```
    $ git clone --recursive https://github.com/dmlc/xgboost
    ```

2. Go the xgboost directory and build the xgboost C++ components:

    * __Linux (Ubuntu)__

    ```
    $ cd xgboost
    $ make -j4
    ```

    * __MacOS__

    ```

    ```

3. Go to the `xgboost/jvm-packages` directory and build the Java components using Maven.

    ```
    $ cd jvm-packages
    $ mvn -DskipTests install   
    ```


### 2.2 Building GRNBoost

Make sure that your Maven repository contains an xgboost instance of version 0.7 or higher.


### 2.3Troubleshooting

Compiling the C++ components can be a tricky affair (especially on MacOS). Some helpful links:
