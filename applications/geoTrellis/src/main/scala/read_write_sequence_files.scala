import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}

import geotrellis.proj4.CRS
import geotrellis.spark.io.hadoop._
import geotrellis.vector.{Extent, ProjectedExtent}
import org.apache.hadoop.io.SequenceFile.Writer
import org.apache.hadoop.io._
import org.apache.spark.mllib.clustering.KMeansModel
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object read_write_sequence_files extends App {

  def serialise(value: Any): Array[Byte] = {
    val stream: ByteArrayOutputStream = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(stream)
    oos.writeObject(value)
    oos.close
    stream.toByteArray
  }

  def deserialise(bytes: Array[Byte]): Any = {
    val ois = new ObjectInputStream(new ByteArrayInputStream(bytes))
    val value = ois.readObject
    ois.close
    value
  }

  override def main(args: Array[String]): Unit = {
    val appName = this.getClass.getName
    val masterURL="spark://emma0.emma.nlesc.nl:7077"

    val sc = new SparkContext(new SparkConf().setAppName(appName).setMaster(masterURL))
    var conf = sc.hadoopConfiguration
    var fs = org.apache.hadoop.fs.FileSystem.get(conf)

    var band_NaN_RDD :RDD[Array[Double]] = sc.emptyRDD
    val metadata_path = "hdfs:///user/emma/spring-index/LastFreeze/sequence_file"

    var projected_extent = new ProjectedExtent(new Extent(10,110,10,110), CRS.fromName("EPSG:3857"))
    var num_cols_rows :(Int, Int) = (100, 80)

    //Based on http://hadooptutorial.info/reading-and-writing-sequencefile-example/
    val writer: SequenceFile.Writer = SequenceFile.createWriter(conf,
      Writer.file(metadata_path),
      Writer.keyClass(classOf[IntWritable]),
      Writer.valueClass(classOf[BytesWritable])
    )

    SequenceFile

    val b:ByteArrayOutputStream  = new ByteArrayOutputStream()
    val o:ObjectOutputStream = new ObjectOutputStream(b)
    o.writeObject(projected_extent)

    //writer.append(new IntWritable(1), new BytesWritable(b.toByteArray))
    writer.append(new IntWritable(1), new BytesWritable(serialise(projected_extent)))
    //writer.append(new IntWritable(2), new BytesWritable(new Array[Byte](num_cols_rows._1)))
    writer.append(new IntWritable(2), new BytesWritable(serialise(num_cols_rows._1)))
    //writer.append(new IntWritable(3), new BytesWritable(new Array[Byte](num_cols_rows._2)))
    writer.append(new IntWritable(3), new BytesWritable(serialise(num_cols_rows._2)))
    writer.hflush()
    writer.close()
    //IOUtils.closeStream(writer)

    //Get The sequence File back.
    val metadata_file = sc.sequenceFile(metadata_path, classOf[IntWritable], classOf[BytesWritable]).map(_._2.copyBytes())

    //val result = sc.sequenceFile(metadata_path, classOf[IntWritable], classOf[BytesWritable]).map(x=>(new String((x._2).getBytes),0,((x._2).getLength))).collect()
    val metadata = metadata_file.collect()
    val projected_extentt :ProjectedExtent = deserialise(metadata(0)).asInstanceOf[ProjectedExtent]
    val projected_extent_out :ProjectedExtent = deserialise(metadata(0)._2.copyBytes()).asInstanceOf[ProjectedExtent]
    val num_cols_rows_out = (metadata(1)._2.asInstanceOf[Int], metadata(2)._2.asInstanceOf[Int])

    println(projected_extent)
    println(projected_extent_out)

    println(num_cols_rows)
    println(num_cols_rows_out)

    var wssse_data :List[(Int, Int, Double)] = List.empty

    val test6 = sc.parallelize(wssse_data)
    var test3 = test6.collect().toList
    var wssse_data_tmp :RDD[(Int, Int, Double)] = sc.objectFile("")
    val wssse = wssse_data_tmp.map{case (a,b,c) => Array(a,b,c).mkString(",")}
    wssse.saveAsTextFile()





    val test2 = sc.parallelize(wssse_data)

    wssse_data :+ (1, 2, 2.34)

    var kmeans_models :Array[Int] = Array.empty[Int]
    kmeans_models


    val wssse_dat :List[(Int, Int, Double)] = sc.objectFile("").collect().toList
    var kmeans_models :Array[KMeansModel] = Array.empty[KMeansModel]//.fill(num_kmeans)(new KMeansModel(Array.empty :Array[Vector]))



  }
}
