import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object create_csv_file extends App {

  override def main(args: Array[String]): Unit = {
    val appName = this.getClass.getName
    val masterURL="spark://emma0.emma.nlesc.nl:7077"

    val sc = new SparkContext(new SparkConf().setAppName(appName).setMaster(masterURL))
    var offline_dir_path = "hdfs:///user/emma/spring-index/"
    var geoTiff_dir = "BloomFinal"
    var wssse_path :String = offline_dir_path + geoTiff_dir + "/wssse_zoom"
    var wssse_csv_path :String = offline_dir_path + geoTiff_dir + "/wssse_zoom.csv"

    //Check offline modes
    var conf = sc.hadoopConfiguration
    var fs = org.apache.hadoop.fs.FileSystem.get(conf)

    var wssse_data :RDD[(Int, Int, Double)] = sc.emptyRDD

    //from disk
    if (fs.exists(new org.apache.hadoop.fs.Path(wssse_path))) {
      wssse_data = sc.objectFile(wssse_path)
      println(wssse_data.collect().toList)
    }

    wssse_data.sortBy(_._1).repartition(1)
    val wssse = wssse_data.map{case (a,b,c) => Array(a,b,c).mkString(",")}
    wssse.saveAsTextFile(wssse_csv_path)

  }
}
