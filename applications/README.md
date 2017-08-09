# Applications

The directory contains examples and how-to's so the user can develop applications to interact with the provided platform.

## Notebooks

Web-based notebooks are a nice way for an user to do provenance of his/her work and share it with colleagues. Through Zeppelin- and Jupyter- notebooks users can interact with our platform. Several kernels are made available such as Python 3, PySpark, SparkR and SQL. The provided templates are for the user to be able to load data from HDFS or Minio/S3 and have it as a Resilient Distributed DataSet ([**RDD**](https://spark.apache.org/docs/latest/programming-guide.html#resilient-distributed-datasets-rdds)) to be consumed by user's code or pass it as input to a function from a Spark Extension such as GeoTrellis (i.e., for raster management), SciSpark (i.e., for scientific computations) or SparkML (i.e., for machine learning).

### Jupyter NoteBooks
To run Jupyter Notebooks we use Jupyter Hub. For a compressive tutorial in how to use Jupyter Hub please read its [pdf version](https://github.com/jupyterhub/jupyterhub-tutorial/blob/master/JupyterHub.pdf) and watch its [youtube version](https://youtu.be/gSVvxOchT8Y).

Under the directory [jupyter_notebooks](./notebooks/) several templates can be found for different kernels. They contain examples in how GeoTiffs or HDF5 files are loaded as a **RDD**. They also show how to save results in different formats.

### Zeppelin NoteBooks
[Apache Zeppelin](https://zeppelin.apache.org/) is a web-based notebook that enables data-driven, interactive data analytics and collaborative documents with R, Python, Scala and more. The following [instructions](https://zeppelin.apache.org/docs/0.7.2/interpreter/spark.html) will explain you how to setup Zepplin with the Spark.

## IDEs

For code development it is handy to have an IDE for features such as auto-completion. For this project we have used two different IDEs from [JetBrains](https://www.jetbrains.com/):
* [PyCharm](https://www.jetbrains.com/pycharm/) for python, more details at [ides/python](ides/python)
* [IntelliJ IDEA](https://www.jetbrains.com/idea/) for java and scala, more details at [ides/scala](ides/scala)
