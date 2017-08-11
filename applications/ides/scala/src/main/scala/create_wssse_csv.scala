import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object create_wssse_csv extends App {

  override def main(args: Array[String]): Unit = {
    val appName = this.getClass.getName
    val masterURL = "spark://emma0.emma.nlesc.nl:7077"
    val sc = new SparkContext(new SparkConf().setAppName(appName).setMaster(masterURL))

    var offline_dir_path = "hdfs:///user/emma/avhrr/"
    //var offline_dir_path = "hdfs:///user/emma/spring-index/"
    var geoTiff_dir = "SOST"
    //var geoTiff_dir = "LeafFinal"
    var wssse_path: String = offline_dir_path + geoTiff_dir + "/75_wssse"
    var wssse_csv_path: String = offline_dir_path + geoTiff_dir + "/75_wssse.csv"

    var conf = sc.hadoopConfiguration
    var fs = org.apache.hadoop.fs.FileSystem.get(conf)

    if (fs.exists(new org.apache.hadoop.fs.Path(wssse_csv_path))) {
      println("The file " + wssse_csv_path + " already exists we will delete it!!!")
      try {
        fs.delete(new org.apache.hadoop.fs.Path(wssse_csv_path), true)
      } catch {
        case _: Throwable => {}
      }
    }

    var wssse_data: RDD[(Int, Int, Double)] = sc.emptyRDD

    //from disk
    if (fs.exists(new org.apache.hadoop.fs.Path(wssse_path))) {
      wssse_data = sc.objectFile(wssse_path)
      println(wssse_data.collect().toList)
    }

    val wssse = wssse_data.repartition(1).sortBy(_._1).map { case (a, b, c) => Array(a, b, c).mkString(",") }
    wssse.saveAsTextFile(wssse_csv_path)
  }
}

