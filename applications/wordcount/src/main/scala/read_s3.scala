import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Romulo on 5/18/2017.
  */
object read_s3 extends App{
  override def main(args: Array[String]): Unit = {
    val appName = this.getClass.getName
    val masterURL="spark://emma0.emma.nlesc.nl:7077"

    val sc = new SparkContext(new SparkConf().setAppName(appName).setMaster(masterURL))
    val filepath = "s3a://files/sonnets.txt"
    val sonnets = sc.textFile(filepath)
    val counts = sonnets.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey(_ + _)

    counts.saveAsTextFile("hdfs:///user/ubuntu/files/output")

    //At the moment it is not possible to save output to S3
    //counts.saveAsTextFile("s3a://files/output")
  }
}
