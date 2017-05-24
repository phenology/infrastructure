import geotrellis.raster.{MultibandTile, Tile}
import geotrellis.spark.io.hadoop._
import geotrellis.vector.ProjectedExtent
import org.apache.hadoop.fs.Path
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

/**
  * Created by Romulo on 5/18/2017.
  */
object read_hdfs extends App {
  override def main(args: Array[String]): Unit = {
    val appName = this.getClass.getName
    val masterURL="spark://emma0.emma.nlesc.nl:7077"

    val sc = new SparkContext(new SparkConf().setAppName(appName).setMaster(masterURL))

    //In Spark 2.1.1 we need a Session
    val ss = SparkSession.builder().getOrCreate()
    val filepath = "hdfs:///user/Romulo/spring_index/LastFreeze/1980.tif"
    val tilesDir = new Path(filepath)

    //We are reading a Single-band
    val multiBandRDD : RDD[(ProjectedExtent, MultibandTile)] = sc.hadoopMultibandGeoTiffRDD(filepath)
    val multiBandTiles = multiBandRDD.values
    val multiBandTileItera = multiBandTiles.toLocalIterator
    val singleBand_1 = multiBandTileItera.next().band(0)
    val singleBand_2 = multiBandTileItera..next().band(0)
    val singleBand_3 = multiBandTileItera.next().band(0)

    val singleBand_1_RDD = sc.parallelize(singleBand_1.toArray())

    val kmeans = new KMeans().setK(2).setSeed(1L)
    val model = kmeans.fit(ss.createDataset(singleBand_1_RDD))

    // Evaluate clustering by computing Within Set Sum of Squared Errors.
    val WSSSE = model.computeCost(singInput)
    println(s"Within Set Sum of Squared Errors = $WSSSE")

    // Shows the result.
    println("Cluster Centers: ")
    model.clusterCenters.foreach(println)

  }

}
