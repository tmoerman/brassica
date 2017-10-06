# GRNBoost

TODO travis-ci status goes here

## Introduction

__GRNBoost__ is a library built on top of [Apache Spark](http://spark.apache.org/) that implements a scalable
strategy for gene regulatory network (GRN) inference.

Inferring a gene regulatory network (GRN) from gene expression data is a computationally expensive task, exacerbated by increasing data sizes due to advances in high-throughput gene profiling technology.

__GRNBoost__ was inspired by [GENIE3](http://www.montefiore.ulg.ac.be/~huynh-thu/GENIE3.html), a popular algorithm for GRN inference. GENIE3 breaks up the inference problem into a number of tree-based ([Random Forest](https://en.wikipedia.org/wiki/Random_forest)) regressions, building a predictive model for the expression profile of each gene in the dataset in function of the expression profiles of a collection of candidate regulatory genes (transcription factors). The regression models act as a feature selection mechanism, they yield the most predictive regulators for the target genes as candidate links in the resulting gene regulatory network.

__GRNBoost__ adopts GENIE3's algorithmic blueprint and aims at improving its runtime performance and data size capability. GRNBoost does this by reframing the GENIE3 _multiple regression_ approach into an Apache Spark MapReduce-style pipeline, and by replacing the regression algorithm by the current state-of-the-art among tree-based machine learning algorithms, a [Gradient Boosting](https://en.wikipedia.org/wiki/Gradient_boosting) variant called [xgboost](https://xgboost.readthedocs.io/en/latest/).

## Getting Started

Following pages provide in-depth material to install and use GRNBoost.

* [Installation guide](docs/installation.md)
* [User guide](docs/user_guide.md)
* [Developer Guide](docs/developer_guide.md)

## License

GRNBoost is available via the [3-Clause BSD license](https://opensource.org/licenses/BSD-3-Clause).

## Report a Problem

Please report a problem by filing an [issue](https://github.com/aertslab/GRNBoost/issues/new).

## References

GRNBoost was developed at the ([Stein Aerts](http://www.vib.be/en/research/scientists/Pages/Stein-Aerts-Lab.aspx)) [Laboratory of Computational Biology](https://gbiomed.kuleuven.be/english/research/50000622/lcb) in the context of the [SCENIC](https://gbiomed.kuleuven.be/english/research/50000622/lcb/tools/scenic) project.

__Aibar S, González-Blas CB, Moerman T, Wouters J__, Huynh-Thu VA, __Imrichová H, Kalender Atak Z, Hulselmans G__, Dewaele M, Rambow F, Geurts P, Aerts J, Marine JC, van den Oord J, __Aerts S.__ [SCENIC: Single-Cell Regulatory Network Inference And Clustering](http://biorxiv.org/content/early/2017/05/31/144501). bioRxiv Preprint 2017 May 31. https://doi.org/10.1101/144501
