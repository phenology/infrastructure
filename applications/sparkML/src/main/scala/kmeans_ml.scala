import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.sql.SparkSession


/**
  * Created by Romulo on 5/18/2017.
  */
object kmeans_ml extends App {
  override def main(args: Array[String]): Unit = {
    val appName = this.getClass.getName
    val masterURL="spark://emma0.emma.nlesc.nl:7077"

    //If you are in Spark 2.0, use SparkSession
    /*First option*/
    //val sc = new SparkSession(new SparkConf().setAppName(appName).setMaster(masterURL))

    /*Second Option*/
    val sc = SparkSession.builder().appName(appName).master(masterURL).getOrCreate()

    val dataset = sc.read.format("libsvm").load("hdfs:///user/ubuntu/files/sample_kmeans_data.txt")

    // Trains a k-means model.
    val kmeans = new KMeans().setK(2).setSeed(1L)
    val model = kmeans.fit(dataset)

    // Evaluate clustering by computing Within Set Sum of Squared Errors.
    val WSSSE = model.computeCost(dataset)
    println(s"Within Set Sum of Squared Errors = $WSSSE")

    // Shows the result.
    println("Cluster Centers: ")
    model.clusterCenters.foreach(println)
  }
}
