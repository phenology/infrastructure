import geotrellis.raster.Tile
import geotrellis.spark.io.hadoop._
import geotrellis.vector.ProjectedExtent
import org.apache.hadoop.fs.Path
import org.apache.spark.{SparkConf, SparkContext}


/**
  * Created by Romulo on 5/18/2017.
  */
object read_hdfs extends App {
  override def main(args: Array[String]): Unit = {
    val appName = this.getClass.getName
    val masterURL="spark://emma0.emma.nlesc.nl:7077"

    val sc = new SparkContext(new SparkConf().setAppName(appName).setMaster(masterURL))
    val filepath = "hdfs:///user/ubuntu/files"
    val tilesDir = new Path(filepath)
    //We are reading a Multi-band
    val multiBandRDD = sc.hadoopGeoTiffRDD(tilesDir, ".tif")

    val indexKey = multiBandRDD.map{case (k,v) => (v,k)}
    val tile1 :Array[(Tile, ProjectedExtent)] = indexKey.take(1)
    val res = tile1.map{case (t, p) => t}
    val kmeansInput = res.toVector

  }

}
