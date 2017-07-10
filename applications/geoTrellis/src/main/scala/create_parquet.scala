import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object create_parquet extends App {

  override def main(args: Array[String]): Unit = {
    val appName = this.getClass.getName
    val masterURL="spark://emma0.emma.nlesc.nl:7077"

    val spark_conf = new SparkConf().setAppName(appName).setMaster(masterURL)
    val sc = new SparkContext(spark_conf)
    var conf = sc.hadoopConfiguration
    var fs = org.apache.hadoop.fs.FileSystem.get(conf)
    var spark = SparkSession.builder().config(spark_conf).getOrCreate()

    var dir_path = "hdfs:///user/hadoop/spring-index/"
    var offline_dir_path = "hdfs:///user/emma/spring-index/"
    var geoTiff_dir = "BloomFinal"
    val toBeMasked = true

    var mask_str = ""
    if (toBeMasked)
      mask_str = "_mask"
    var grids_matrix_path = offline_dir_path + geoTiff_dir + "/grids_matrix" + mask_str
    var grids_parquet_path = offline_dir_path + geoTiff_dir + "/grids_matrix" + mask_str + ".parquet"

    var grids_matrix: RDD[Vector] = sc.emptyRDD
    grids_matrix = sc.objectFile(grids_matrix_path)
    grids_matrix.count()

    val start_year = 1980
    val end_year = 2016
    var cols :Array[String] = new Array[String](start_year-end_year)
    for (f <- start_year to end_year) {
      cols(f-start_year) = f.toString
    }

    val schema = new StructType(cols.map( m => StructField(m, DoubleType, nullable = true)))
    val rowRDD = grids_matrix.map( m => Row.fromSeq(m.toArray))
    val matrixDF = spark.createDataFrame(rowRDD, schema)

    matrixDF.c
    matrixDF.write.parquet(grids_parquet_path)

  }
}
