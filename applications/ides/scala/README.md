# Information to get the code working on IDE.

Get the binaries:
Spark Without hadoop
Hadoop-2.8.0
Hadoop-common

Add libraries to Intelij IDE.

1) Set Java as Project SDK.

2) Add scala 2.11.8 from maven as SDK into the General libraries.

3) Mark src as source directory

3) From Hadoop-2.8.0
  a) add directory share/hadoop/hdfs and select only the jar dir, skip jdiff.
  b) add directory share/hadoop/common/hadoop-common-2.8.0.jar

4) Add Spark-2.1.1-bin-without-hadoop jars

5) Add GeoTrellis dependency from maven (locationtech.geotrellis)
  a) geotrellis-spark-2.11
  b) geotrellis-raster-2.11
  c) geotrellis-proj4-2.11

6) Download notebooks as scala into the proper directory:
  a) Bellow the imports you need to add:

    object <note_book_name> extends App {

      override def main(args: Array[String]): Unit = {
      val appName = this.getClass.getName
      val masterURL = "spark://emma0.emma.nlesc.nl:7077"
      val sc = new SparkContext(new SparkConf().setAppName(appName).setMaster(masterURL))

  b) Close the file with
    }}


## Extras:

A) Install com.quantifind.charts.Highcharst for plotting in Scala.
  1- Maven com.quantifind:wisp_2.11:0.0.4





