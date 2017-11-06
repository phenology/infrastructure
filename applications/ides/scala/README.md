# Information to get the code working on IntelliJ IDEA.

The following instructions assume you have IntelliJ installed, and its scala and sbt plugins installed. It is also necessary to have Java 1.8.0 installed, we recommend to do it before installing IntelliJ.

## Create project
1) In IntelliJ IDEA in **File** choose **Create Project from Existing Sources** and select the dir **ides/scala**. 

2) In the project layout mark **src** as source directory by right clicking with mouse over it and choosing **Mark directory as**.

3) Before setting up a project dependencies in IntelliJ IDEA, you need to download the following binaries:
```
# Spark 2.1.1 without Hadoop.
wget https://d3kbcqa49mib13.cloudfront.net/spark-2.1.1-bin-without-hadoop.tgz

#Hadoop 2.8.0
wget http://apache.proserve.nl/hadoop/common/hadoop-2.8.0/hadoop-2.8.0.tar.gz
```

### Project Settings

In IntelliJ IDEA in **File** choose **Project Structure** for Project settings and set dependencies. 

* In **Project Settings** set **Project SDK** to 1.8 Java version.

* In **Project Settings** set **Libraries** to the following (click plus):
```
#New project library from Java
1) From Hadoop-2.8.0
  a) add directory share/hadoop/hdfs and select only the jar dir, skip jdiff.
  b) add directory share/hadoop/common/hadoop-common-2.8.0.jar

2) Add Spark-2.1.1-bin-without-hadoop jars

#New project library from Maven
1) Add GeoTrellis dependency from maven (locationtech.geotrellis)
  a) geotrellis-spark_2.11:1.1.1
  b) geotrellis-raster_2.11:1.1.1
  c) geotrellis-proj4_2.11:1.1.1
  d) geotrallis-vector_2.11:1.1.1
  
2) Extras:
  a) Install com.quantifind.charts.Highcharst for plotting in Scala.
    1- Maven com.quantifind:wisp_2.11:0.0.4
  
  b) For Json install spray-json_2.9.1:1.0.1
```

* In **Platform Settings** set **Global Libraries** to scala-sdk-2.11.8 (click plus to add it).

## Add source file

Open a notebook in JupyterHub and then download it as scala into the correct directory, i.e., stable or your testing directory. Then bellow the imports you need to add:
```
object <note_book_name> extends App {
  
  override def main(args: Array[String]): Unit = {
    val appName = this.getClass.getName
    val masterURL = "spark://emma0.emma.nlesc.nl:7077"
    val sc = new SparkContext(new SparkConf().setAppName(appName).setMaster(masterURL))
```

Then close the file with:
```
}}
```

## Application debugging
To debug an application the first steps consists in reconfiguring [Spark to run on debug mode](https://github.com/nlesc-sherlock/emma/blob/master/spark.md#remote-debugging). For performance profiling it is also possible to configure the garbage collector and its monitoring policies. To know more please read [Emma's Spark debug info](https://github.com/nlesc-sherlock/emma/blob/master/spark.md#debug-mode). 

Once Spark is configured to run in *debug mode*, the next step is to configure *IntelliJ IDEA* to attach a debugger to the application driver and a debugger per worker. In *IntelliJ IDEA* the user should click on **Run** and then click **Debug**.

**Then click create/configure Debugger.**
![alt text](https://github.com/phenology/infrastructure/blob/master/applications/ides/scala/images/create_remote_debugger.png "Create remote debugger")

**Create a debugger for the driver.**
![alt text](https://github.com/phenology/infrastructure/blob/master/applications/ides/scala/images/debug_driver.png "Create driver's debugger")

**Create a debugger for a worker.**
![alt text](https://github.com/phenology/infrastructure/blob/master/applications/ides/scala/images/debug_worker.png "Create worker's debugger")


With all debuggers defined, the next step is to run them so they get attached to the remote processes. In case the user wants a process to wait for a debugger be attached before it runs, the user should [*set the variables worker_waiting_on_startup and driver_waiting_on_startup to y (yes), by default they are set to n (no)*](https://github.com/nlesc-sherlock/emma/blob/master/spark.md#remote-debugging).
