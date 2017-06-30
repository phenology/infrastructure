import geotrellis.spark.io.hadoop._
import geotrellis.vector.ProjectedExtent
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object geoTrellis_multiTiffs_kmeans extends App {

  override def main(args: Array[String]): Unit = {
    val appName = this.getClass.getName
    val masterURL="spark://emma0.emma.nlesc.nl:7077"

    val sc = new SparkContext(new SparkConf().setAppName(appName).setMaster(masterURL))
    //val band_count = geotrellis.raster.io.geotiff.reader.TiffTagsReader.read(filepath).bandCount;
    val band_count = 4;
    var band_RDD :RDD[(ProjectedExtent, Array[Double])] = sc.emptyRDD
    val pattern: String = "tif"
    var filepath :String = ""
    if (band_count == 1)
      filepath = "hdfs:///user/hadoop/spring-index/LastFreeze/"
    else
      filepath = "hdfs:///user/hadoop/spring-index/BloomFinal/"

    if (band_count == 1) {
      val bands_RDD = sc.hadoopGeoTiffRDD(filepath, pattern)
      band_RDD = bands_RDD.mapValues(m => m.toArrayDouble().filter(!_.isNaN))
    } else {
      val bands_RDD = sc.hadoopMultibandGeoTiffRDD(filepath, pattern)
      band_RDD = bands_RDD.mapValues(m => m.band(3).toArrayDouble().filter(v => !v.isNaN).take(1000000))
    }

    val band_vec = band_RDD.values.map(s => Vectors.dense(s)).cache()

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

    println(clusters.clusterCenters.foreach(m => m.asML.size))

    //Clusters save the model
    if (band_count == 1) {
      clusters.save(sc, "hdfs:///user/emma/spring_index/LastFreeze/1908_kmeans_model")
    } else {
      clusters.save(sc, "hdfs:///user/emma/spring_index/BloomFinal/1908_kmeans_model")
    }
  }
}
