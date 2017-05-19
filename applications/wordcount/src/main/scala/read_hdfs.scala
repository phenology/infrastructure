import org.apache.spark.{SparkConf, SparkContext}

object read_hdfs extends App {
  override def main(args: Array[String]) {
    val appName = this.getClass.getName
    val masterURL="spark://emma0.emma.nlesc.nl:7077"

    val sc = new SparkContext(new SparkConf().setAppName(appName).setMaster(masterURL))
    val filepath = "hdfs://emma0.emma.nlesc.nl:9000/user/ubuntu/sonnets.txt"

    val sonnets = sc.textFile(filepath)
    val counts = sonnets.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey(_ + _)
    counts.saveAsTextFile("hdfs://emma0.emma.nlesc.nl:9000/files/output")

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
