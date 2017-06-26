# Applications

The diretory contains examples and how-to's so the user can load data into the different storage layers and create applications to interact with the provided platform.

## Data loading

### HDFS

Before The user needs to download the binaries for Hadoop 2.8.0.
```
wget http://apache.hippo.nl/hadoop/common/hadoop-2.8.0/hadoop-2.8.0.tar.gz
tar -xzf hadoop-2.8.0.tar.gz
cd hadoop-2.8.0
```
Copy the Hadoop configuration environment, **core-site.xml** and **hdfs-site.xml**, from one of the virtual machines. Its location at the remote machine is **/etc/hadoop/conf/** and its destination is **etc/hadoop/**.
Once the hadoop configuration is copied it is time to test it and for that let's list the user directories.
```
./bin/hadoop dfs -ls /user

#The outcome should be the following:
Found 4 items
drwxr-xr-x   - hadoop hadoop              0 2017-06-08 10:03 /user/hadoop
drwxrwxrwt   - root   supergroup          0 2017-06-08 10:03 /user/history
drwxr-xr-x   - spark  spark               0 2017-06-08 10:04 /user/spark
drwxr-xr-x   - ubuntu ubuntu              0 2017-06-08 10:04 /user/ubuntu
```

Not it is time to upload some data. The following example shows how to upload the GeoTiffs for the **spring-index**.
```
# Copy the files
./bin/hadoop dfs -copyFromLocal <path_to_spring-index>/spring-index/ /user/hadoop/

# List the uploaded files
./bin/hadoop dfs -ls /user/hadoop/spring-index/
```

The uploaded files can also be listed using the [HDFS web-ui interface](https://github.com/nlesc-sherlock/emma/blob/223f93d91b63399cded51c52faa375ad77601fbd/hadoop.md#hadoop).

### Minio


## Notebooks

Web-based notebooks are a nice way for an user to do provenance of his/her work and share it with colleagues. Through Zeppelin- and Jupyter- notebooks users can interact with our platform. Several kernels are made available such as Python 3, PySpark, SparkR and SQL. The provided templates are for the user to be able to load data from HDFS or Minio/S3 and have it as a Resilient Distributed DataSet ([**RDD**](https://spark.apache.org/docs/latest/programming-guide.html#resilient-distributed-datasets-rdds)) to be consumed by user's code or pass it as input to a function from a Spark Extension such as GeoTrellis (i.e., for raster management), SciSpark (i.e., for scientific computations) or SparkML (i.e., for machine learning).

### Jupyter NoteBooks
To run Jupyter Notebooks we use Jupyter Hub. For a compressive tutorial in how to use Jupyter Hub please read its [pdf version](https://github.com/jupyterhub/jupyterhub-tutorial/blob/master/JupyterHub.pdf) and watch its [youtube version](https://youtu.be/gSVvxOchT8Y).

Under the directory [jupyter_notebooks](./notebooks/) several templates can be found for different kernels. They contain examples in how GeoTiffs or HDF5 files are loaded as a **RDD**. They also show how to save results in different formats.

### Zepplin NoteBooks

