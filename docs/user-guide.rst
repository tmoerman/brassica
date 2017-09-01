.. _Apache Spark: https://spark.apache.org/
.. _`.jar file`: https://en.wikipedia.org/wiki/JAR_(file_format)
.. _`JVM`: https://en.wikipedia.org/wiki/Java_virtual_machine
.. _submitting: https://spark.apache.org/docs/latest/submitting-applications.html
.. _`Amazon Elastic MapReduce`: https://aws.amazon.com/emr/
.. _`EMR`: https://aws.amazon.com/emr/

User Guide
==========

GRNBoost is an `Apache Spark`_ library. It comes as a `.jar file`_ containing
code that can be executed by a Spark installation, either on your local machine or
on a cluster like `Amazon Elastic MapReduce`_ (EMR). We provide examples of both later.

Using ``spark-submit``
----------------------

Being a Spark application, GRNBoost is executed by submitting_ it to a Spark installation.

.. sidebar:: **Note**

    The backslashes '\\' are used to split the shell command over multiple lines.

.. code-block:: bash

    $SPARK_HOME/bin/spark-submit \
    --class org.aertslab.grnboost.GRNBoost \
    --master local[*] \
    --deploy-mode client \
    --driver-memory 96g \
    --conf spark.network.timeout=10000000 \
    --conf spark.eventLog.enabled=true \
    --jar /path/to/xgboost4j-0.7.jar \
    /path/to/GRNBoost.jar \
    infer \
    -i  /path/to/expression_data.tsv \
    -tf /path/to/transcription_factors.tsv \
    -o  /path/to/grn_output.tsv

The shell command consists of four sections.

1. ``spark-submit`` is the script that launches a Spark applications.

    .. code-block:: bash

        $SPARK_HOME/bin/spark-submit \

2. The launch script is followed by a list of Spark options.

    .. code-block:: bash

        --class org.aertslab.grnboost.GRNBoost \
        --master local[*] \
        --deploy-mode client \
        --driver-memory 96g \
        --conf spark.network.timeout=10000000 \
        --conf spark.eventLog.enabled=true \
        --jar /path/to/xgboost4j-0.7.jar \

3. Next, the path to the Spark application's .jar file is specified.

    .. code-block:: bash

        /path/to/GRNBoost.jar \

4. Finally, arguments interpreted by the Spark application are provided. For more information about GRNBoost's parameters, see the command-line guide.

    .. code-block:: bash

        infer \
        -i  /path/to/expression_data.tsv \
        -tf /path/to/transcription_factors.tsv \
        -o  /path/to/grn_output.tsv



Running locally
------------------------

The most straightforward way to try out GRNBoost_ is to run it on your local machine.
Keep in mind though that this is probably not suitable for large data sets as GRNBoost can be quite memory intensive.

1.



Running on Amazon Elastic MapReduce (EMR)
-----------------------------------------

.. TODO



Running on VSC (Vlaamse Supercomputer)
--------------------------------------

.. TODO
