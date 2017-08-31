.. GRNBoost documentation master file, created by
   sphinx-quickstart on Mon Aug 21 11:12:48 2017.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

.. image:: /logo_banner_grey.png
   :alt: Optunity

==========

GRNboost_ is an `Apache Spark`_ library for inferring gene regulatory networks (GRNs) from next
generation sequencing data. It is written in the Scala_ programming language and `available on Github`_.

GRNBoost_ was inspired by the machine-learning approach proposed by GENIE3_, a popular (200+ citations)
algorithm for the inference of GRNs and winner in two DREAM_ challenges. GENIE3_'s inference strategy is
simple but remarkably effective. It breaks up the construction of a full GRN into a number of regressions,
one for each target gene, in function of a collection of candidate regulating genes (transcription factors).
The regressions are performed using tree-based ensemble learning algorithms (`Random Forest`_ or `ExtraTrees`_).
From each regression model, the input features (genes) with highest importance are selected as candidate
regulators for the target gene. Finally, all regulatory links with sufficient importance are collected
into the final putative regulatory network.

GRNBoost_ adopts GENIE3_'s "multiple regression" approach.



GRNBoost_ improves upon GENIE3_ in terms of performance and

yet rewritten from the ground up
on using `Apache Spark`_ to make it scalable.

to accommodate larger data sets than what can



.. _GRNBoost: https://github.com/aertslab/GRNBoost
.. _GENIE3: http://www.montefiore.ulg.ac.be/~huynh-thu/GENIE3.html
.. _GENIE3 publication: http://journals.plos.org/plosone/article?id=10.1371/journal.pone.0012776
.. _DREAM: http://dreamchallenges.org/
.. _DREAM4:
.. _DREAM5:
.. _Scala: https://www.scala-lang.org/
.. _Apache Spark: https://spark.apache.org/
.. _available on Github: https://github.com/aertslab/GRNBoost
.. _Random Forest: https://en.wikipedia.org/wiki/Random_forest
.. _ExtraTrees: https://en.wikipedia.org/wiki/Random_forest#ExtraTrees

Contents:

.. toctree::
    :maxdepth: 1

    walk-through

.. toctree::
    :maxdepth: 2
    :caption: User Documentation

    overview
    installation
    command-line
    parameters

.. toctree::
    :maxdepth: 2
    :caption: Developer Documentation

    architecture

.. toctree::
    :maxdepth: 2
    :caption: Reference

    reference



Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
