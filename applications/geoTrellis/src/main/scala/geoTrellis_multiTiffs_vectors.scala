import geotrellis.spark.io.hadoop._
import geotrellis.vector.Extent
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object geoTrellis_multiTiffs_vectors extends App {

  override def main(args: Array[String]): Unit = {
    val appName = this.getClass.getName
    val masterURL="spark://emma0.emma.nlesc.nl:7077"

    val sc = new SparkContext(new SparkConf().setAppName(appName).setMaster(masterURL))

    //The following example shows how to get the number of bands, however, it is not known
    //val band_count = geotrellis.raster.io.geotiff.reader.TiffTagsReader.read(filepath).bandCount;

    val band_count = 1;
    var band_RDD :RDD[Array[Double]] = sc.emptyRDD
    val pattern: String = "tif"
    var filepath :String = ""
    var extent_USA = new Extent(-126.30312894720473, 14.29219617034159, -56.162671563152486, 49.25462702827337)
    var bands_USA :(Double, Double) = (0, 0)
    if (band_count == 1) {
      //Single band GeoTiff
      filepath = "hdfs:///user/hadoop/spring-index/LastFreeze/"
    } else {
      //Multi band GeoTiff
      filepath = "hdfs:///user/hadoop/spring-index/BloomFinal/"
    }

    if (band_count == 1) {
      //Lets load a Singleband GeoTiff and return RDD just with the tiles.
      //Since it is a single GeoTiff, it will be a RDD with a tile.
      val bands_RDD = sc.hadoopGeoTiffRDD(filepath, pattern).values
      val extents_withIndex = sc.hadoopGeoTiffRDD(filepath, pattern).keys.zipWithIndex().map{case (e,v) => (v,e)}
      extent_USA = extents_withIndex.lookup(0).apply(0).extent

      val bands_withIndex = bands_RDD.zipWithIndex().map{case (e,v) => (v,e)}
      bands_USA = (bands_withIndex.lookup(0).apply(0).cols, bands_withIndex.lookup(0).apply(0).rows)

      //Lets create an ArrayDouble and filter out NaN
      band_RDD = bands_RDD.map(m => m.toArrayDouble().filter(!_.isNaN))

      //Lets create a Vector and filter out NaN
      bands_RDD.foreach(m => println("cols %d and rows %d".format(m.cols,m.rows)))
    } else {
      //Lets load a Multiband GeoTiff and return RDD just with the tiles.
      //Since it is a single GeoTiff, it will be a RDD with a tile.
      val bands_RDD = sc.hadoopMultibandGeoTiffRDD(filepath, pattern).values
      val extents_withIndex = sc.hadoopGeoTiffRDD(filepath, pattern).keys.zipWithIndex().map{case (e,v) => (v,e)}
      extent_USA = extents_withIndex.lookup(0).apply(0).extent

      val bands_withIndex = bands_RDD.zipWithIndex().map{case (e,v) => (v,e)}
      bands_USA = (bands_withIndex.lookup(0).apply(0).cols, bands_withIndex.lookup(0).apply(0).rows)

      //Extract the 4th band and filter out NaN
      band_RDD = bands_RDD.map(m => m.band(3).toArrayDouble().filter(v => !v.isNaN).take(1000000))
    }

    println(bands_USA)

    println(extent_USA)

    // Create a Vector without NaN values
    val band_vec = band_RDD.map(s => Vectors.dense(s)).cache()

    val numClusters = 3
    val numIterations = 20
    val clusters = {
      KMeans.train(band_vec,numClusters,numIterations)
    }

    // Evaluate clustering by computing Within Set Sum of Squared Errors
    val WSSSE = clusters.computeCost(band_vec)
    println("Within Set Sum of Squared Errors = " + WSSSE)

    //Un-persist the model
    band_vec.unpersist()

    // Shows the result.
    println("Cluster Centers: ")
    clusters.clusterCenters.foreach(println)

    //Clusters save the model
    if (band_count == 1) {
      clusters.save(sc, "hdfs:///user/emma/spring_index/LastFreeze/1908_kmeans_model")
    } else {
      clusters.save(sc, "hdfs:///user/emma/spring_index/BloomFinal/1908_kmeans_model")
    }
  }
}
