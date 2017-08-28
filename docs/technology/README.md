# Literature
List of articles, presentations and how-to's relevant for the used technology or data science techniques.

##  Technology
* Spark 2.1.0
  * Mlib
[Data Types - RDD-based API](https://spark.apache.org/docs/latest/mllib-data-types.html#data-types-rdd-based-api)

  * Miscellaneous
[Scalable Sparse Matrix Multiplication in Apache Spark](https://www.balabit.com/blog/scalable-sparse-matrix-multiplication-in-apache-spark/)
* SciSpark

* Breeze
Library used by Spark-Mlib and SciSpark for linear algebra operations. It is part of [ScalaNLP](www.scalanlp.org).

##  Knowledge base
* Matrix Computations
Distributed Matrix Computations, Reza Zadeh, Institute for Computational & Mathematical Engineering at Stanford University - [presentation](https://stanford.edu/~rezab/nips2014workshop/slides/reza.pdf).
Matrix Computations and Optimization in Apache Spark, Reza Zadeh and et al., [article at KDD 2016](https://stanford.edu/~rezab/papers/linalg.pdf). 

* Storing a sparse matrix
Spark uses Compressed sparse column (CSC) to store local matrixes. The following wiki-page explains the different storage approaches for sparse matrixes and their advantages, [storing a sparse matrix](https://en.wikipedia.org/wiki/Sparse_matrix#Storing_a_sparse_matrix).

* Principal component analysis (PCA) and Single value decomposition (SVD). For extreme large matrices, Spark required a
lot memory for calculating PCA. For example, ["*The other day I found this post on the Domino Data Science blog that covers
calculating a PCA of a matrix with 1 million rows and 13,000 columns. This is pretty big as far as PCA usually goes. They
used a Spark cluster on a 16 core machine with 30GB of RAM and it took them 27 hours.*"](http://amedee.me/post/pca-large-matrices/). Hence,
we decided to explore **randomized SVD** methods. There two nice implementations, one from Facebook called [fbpca](https://github.com/facebook/fbpca)
and another one by [scikit-learn](http://scikit-learn.org/stable/modules/generated/sklearn.decomposition.PCA.html).
