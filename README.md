# Infrastructure
All work related with infrastructure.

## EMMA
Emma is a project where ansible is used to setup a Spark cluster with GeoTrellis and SciSpark modules, and using for storage two flavors of storage, as file-based HDFS and GlusterFS and as object-based Minio (it has the same API as Amazong S3). Give a look into the submodule emma.

## Applications

To develop applications and deploy them at EMMA, the user should follow the steps described in [applications](./applications) to load data into the platform. Under [applications/notebooks](./applications/notebooks) there are examples on how to read GeoTiff files into RDD, run Kmeans from Spark MLlib, store kmeans results, plot/visualize GeoTiffs in Python.
