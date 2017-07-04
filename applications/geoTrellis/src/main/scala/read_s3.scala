import geotrellis.spark.io.s3.S3GeoTiffRDD
import org.apache.hadoop.fs.Path
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Romulo on 5/18/2017.
  */
object read_s3 extends App {
  override def main(args: Array[String]): Unit = {
    val appName = this.getClass.getName
    val masterURL="spark://emma0.emma.nlesc.nl:7077"

    val sc = new SparkContext(new SparkConf().setAppName(appName).setMaster(masterURL))
    val filepath = "hdfs:///user/ubuntu/files"
    val tilesDir = new Path(filepath)
    //We are reading a Multi-band
    val singleBandRDD = S3GeoTiffRDD.spatialMultiband("spring_index", "LastFreeze/1980.tiff", null)
  }
}
