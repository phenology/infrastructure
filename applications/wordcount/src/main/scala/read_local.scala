import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Romulo on 5/18/2017.
  */
object read_local extends App{
  override def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "E:\\windows\\Projects\\Spark\\binaries\\hadoop-common-2.2.0-bin-master")
    val appName = this.getClass.getName
    val masterURL="local[*]"

    val sc = new SparkContext(new SparkConf().setAppName(appName).setMaster(masterURL))
    val filepath = "hdfs:///user/ubuntu/sonnets.txt"

    val sonnets = sc.textFile(filepath)
    val counts = sonnets.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey(_ + _)
    System.out.println(counts.collect().mkString(", "))
    //counts.saveAsTextFile("hdfs://emma0.emma.nlesc.nl:9000/files/output")

    // read in text file and split each document into words
    val tokenized = sc.textFile(filepath).flatMap(_.split(" "))
    val threshold = 12

    // count the occurrence of each word
    val wordCounts = tokenized.map((_, 1)).reduceByKey(_ + _)

    // filter out words with fewer than threshold occurrences
    val filtered = wordCounts.filter(_._2 >= threshold)

    // count characters
    val charCounts = filtered.flatMap(_._1.toCharArray).map((_, 1)).reduceByKey(_ + _)

    System.out.println(charCounts.collect().mkString(", "))
  }
}
