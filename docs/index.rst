.. GRNBoost documentation master file, created by
   sphinx-quickstart on Mon Aug 21 11:12:48 2017.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

.. image:: /logo_banner_grey.png
   :alt: GRNBoost
|

GRNboost_ is an `Apache Spark`_ library for inferring `gene regulatory networks`_ (GRNs) from next
generation sequencing data.

GRNBoost_ was inspired by the machine-learning approach proposed by GENIE3_, a popular (200+ citations)
algorithm for the inference of GRNs and top performer in two DREAM_ challenges. GENIE3_'s inference strategy is
simple but remarkably effective. It breaks up the construction of a full GRN into a number of regressions,
one for each target gene, in function of a collection of candidate regulating genes (transcription factors).
The regressions are performed using tree-based ensemble learning algorithms (`Random Forest`_ or `ExtraTrees`_).
From each regression model, the input features (regulating genes) with highest importance are selected as
candidate regulators for the target gene. Finally, all regulatory links with sufficient importance are collected
into the final putative regulatory network.

GRNBoost_ aims at providing a scalable and computationally performant alternative for GENIE3_. GENIE3_'s
multiple regression approach represents a so-called `embarrassingly parrallel`_ workload. This immediately
suggests the extrapolation from using multiple threads -- one per regression -- on a single machine, to
a distributed_ MapReduce_ approach, where regressions are spread out over multiple compute nodes. GRNBoost_
uses `Apache Spark`_ to coordinate the regressions and aggregate their inference results.
Secondly, motivated by improving the speed of computation of a single regression, GRNBoost_ replaces the
regression algorithm by XGBoost_, a highly performant and currently state-of-the-art `ensemble learning`_ algorithm.
XGBoost_ uses boosting_ to combine weak learners, typically (shallow) decision trees, into a strong learner.

GRNBoost_ is written in the Scala_ programming language and `available on Github`_ under the TODO license.

.. _gene regulatory networks: https://en.wikipedia.org/wiki/Gene_regulatory_network
.. _GRNBoost: https://github.com/aertslab/GRNBoost
.. _GENIE3: http://www.montefiore.ulg.ac.be/~huynh-thu/GENIE3.html
.. _GENIE3 publication: http://journals.plos.org/plosone/article?id=10.1371/journal.pone.0012776
.. _DREAM: http://dreamchallenges.org/
.. _DREAM4:
.. _DREAM5:
.. _embarrassingly parallel: https://en.wikipedia.org/wiki/Embarrassingly_parallel
.. _Scala: https://www.scala-lang.org/
.. _Apache Spark: https://spark.apache.org/
.. _distributed: https://en.wikipedia.org/wiki/Distributed_computing
.. _MapReduce: https://en.wikipedia.org/wiki/MapReduce
.. _available on Github: https://github.com/aertslab/GRNBoost
.. _Random Forest: https://en.wikipedia.org/wiki/Random_forest
.. _ExtraTrees: https://en.wikipedia.org/wiki/Random_forest#ExtraTrees
.. _XGBoost: https://xgboost.readthedocs.io/en/latest/
.. _ensemble learning: https://en.wikipedia.org/wiki/Ensemble_learning
.. _boosting: https://en.wikipedia.org/wiki/Boosting_(machine_learning)
.. _Visual Data Analysis Lab: http://vda-lab.github.io/
.. _Laboratory of Computational Biology: http://gbiomed.kuleuven.be/english/research/50000622/lcb
.. _ESAT: https://www.esat.kuleuven.be/
.. _STADIUS: http://www.esat.kuleuven.be/stadius/
.. _`VIB Center for Brain & Disease Research`: https://cbd.vib.be/

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

Contributors
------------

GRNBoost is a joint effort between the `Visual Data Analysis Lab`_ (ESAT_, STADIUS_)
and the `Laboratory of Computational Biology`_ (`VIB Center for Brain & Disease Research`_).




Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
