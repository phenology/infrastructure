import geotrellis.proj4.CRS
import geotrellis.raster.{DoubleArrayTile, Tile}
import geotrellis.raster.io.geotiff._
import geotrellis.raster.io.geotiff.writer.GeoTiffWriter
import geotrellis.raster.io.geotiff.{GeoTiff, SinglebandGeoTiff}
import geotrellis.spark.io.hadoop._
import geotrellis.vector.{Extent, ProjectedExtent}
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object geoTrellis_multiTiffs_save_GeoTiff extends App {

  override def main(args: Array[String]): Unit = {
    val appName = this.getClass.getName
    val masterURL = "spark://emma0.emma.nlesc.nl:7077"

    val sc = new SparkContext(new SparkConf().setAppName(appName).setMaster(masterURL))
    //val band_count = geotrellis.raster.io.geotiff.reader.TiffTagsReader.read(filepath).bandCount;
    val band_count = 1;
    val in_memory = 2;
    val sample = 1000;
    var projected_extent = new ProjectedExtent(new Extent(0,0,0,0), CRS.fromName("EPSG:3857"))
    var num_cols_rows :(Int, Int) = (0, 0)
    var band_RDD: RDD[Array[Double]] = sc.emptyRDD
    var band_vec: RDD[Vector] = sc.emptyRDD
    var band0: RDD[(Long, Double)] = sc.emptyRDD
    var band0_index: Array[Int] = Array.emptyIntArray
    val pattern: String = "tif"
    var filepath: String = ""
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
      val tiles_RDD = sc.hadoopGeoTiffRDD(filepath, pattern).values
      val bands_RDD = tiles_RDD.map(m => m.toArrayDouble())

      val extents_withIndex = sc.hadoopGeoTiffRDD(filepath, pattern).keys.zipWithIndex().map{case (e,v) => (v,e)}
      projected_extent = extents_withIndex.lookup(0).apply(0)

      val tiles_withIndex = tiles_RDD.zipWithIndex().map{case (e,v) => (v,e)}
      num_cols_rows = (tiles_withIndex.lookup(0).apply(0).cols, tiles_withIndex.lookup(0).apply(0).rows)

      //Get Index for Cells
      val bands_withIndex = bands_RDD.zipWithIndex().map { case (e, v) => (v, e) }
      //band0_index = bands_withIndex.lookup(0).apply(0).zipWithIndex.filter{ case (v, i) => !v.isNaN }.map { case (v, i) => (i) }
      val band0_index = bands_withIndex.lookup(0).apply(0).zipWithIndex.filter(m => !m._1.isNaN).take(sample).map { case (v, i) => (i) }

      //Get Array[Double] of a Title to later store the cluster ids.
      band0 = sc.parallelize(bands_withIndex.lookup(0).take(1)).flatMap( m => m).zipWithIndex.map{case (v,i) => (i,v)}

      //Lets filter out NaN
      band_RDD = bands_RDD.map(m => m.filter(!_.isNaN).take(sample))
    } else {
      //Lets load a Multiband GeoTiff and return RDD just with the tiles.
      //Since it is a multi-band GeoTiff, we will take band 4
      val tiles_RDD = sc.hadoopMultibandGeoTiffRDD(filepath, pattern).values
      val bands_RDD = tiles_RDD.map(m => m.band(3).toArrayDouble())

      val extents_withIndex = sc.hadoopGeoTiffRDD(filepath, pattern).keys.zipWithIndex().map{case (e,v) => (v,e)}
      projected_extent = extents_withIndex.lookup(0).apply(0)

      val tiles_withIndex = tiles_RDD.zipWithIndex().map{case (e,v) => (v,e)}
      num_cols_rows = (tiles_withIndex.lookup(0).apply(0).cols, tiles_withIndex.lookup(0).apply(0).rows)

      //Get Index for Cells
      val bands_withIndex = bands_RDD.zipWithIndex().map { case (e, v) => (v, e) }
      band0_index = bands_withIndex.lookup(0).apply(0).zipWithIndex.filter { case (v, i) => !v.isNaN }.take(sample).map { case (v, i) => (i) }

      //Get Array[Double] of a Title to later store the cluster ids.
      band0 = sc.parallelize(bands_withIndex.lookup(0).take(1)).flatMap( m => m).zipWithIndex.map{case (v,i) => (i,v)}

      //Let's filter out NaN
      band_RDD = bands_RDD.map(m => m.filter(v => !v.isNaN).take(sample))
    }

    /*
    We need to do a Matrix transpose to have clusters per cell
    and not per year. If we do:

    val band_vec = band_RDD.map(s => Vectors.dense(s)).cache()

    The vectors are rows and therefore the matrix will look like this:
    Vectors.dense(0.0, 1.0, 2.0),
    Vectors.dense(3.0, 4.0, 5.0),
    Vectors.dense(6.0, 7.0, 8.0),
    Vectors.dense(9.0, 0.0, 1.0)

    Inspired in:
    http://jacob119.blogspot.nl/2015/11/how-to-convert-matrix-to-rddvector-in.html
    and
    https://stackoverflow.com/questions/29390717/how-to-transpose-an-rdd-in-spark
    */

    if (in_memory == 1) {
      //A) For small memory footprint RDDs we can simply bring it to memory and transpose it
      //First transpose and then parallelize otherwise you get:
      //error: polymorphic expression cannot be instantiated to expected type;
      val band_vec_T = band_RDD.collect().transpose
      band_vec = sc.parallelize(band_vec_T).map(m => Vectors.dense(m)).cache()
    } else {
      //B) For large memory footpring RDDs we need to run in distributed mode

      // Split the matrix into one number per line.
      val byColumnAndRow = band_RDD.zipWithIndex.flatMap {
        case (row, rowIndex) => row.zipWithIndex.map {
          case (number, columnIndex) => columnIndex -> (rowIndex, number)
        }
      }

      // Build up the transposed matrix. Group and sort by column index first.
      val byColumn = byColumnAndRow.groupByKey.sortByKey().values

      // Then sort by row index.
      val transposed = byColumn.map {
        indexedRow => indexedRow.toSeq.sortBy(_._1).map(_._2)
      }

      band_vec = transposed.map(m => Vectors.dense(m.toArray)).cache()
    }

    /*
     Here we will collect some info to see if the transpose worked correctly
    */

    val band_vec_col = band_vec.collect()

    //Number of Columns, i.e., years
    println(band_vec_col.size)

    //Number of cells after filtering our NaN and a take()
    println(band_vec_col(0).size)

    //Values for a cell over the years.
    println(band_vec_col(0))


    /*
     Here we will train kmeans
    */

    val numClusters = 3
    val numIterations = 5
    val clusters = {
      KMeans.train(band_vec, numClusters, numIterations)
    }

    // Evaluate clustering by computing Within Set Sum of Squared Errors
    val WSSSE = clusters.computeCost(band_vec)
    println("Within Set Sum of Squared Errors = " + WSSSE)

    //Un-persist the model
    band_vec.unpersist()

    /*
     Cluster model's result management
    */

    // Lets show the result.
    println("Cluster Centers: ")
    clusters.clusterCenters.foreach(println)

    //Lets save the model into HDFS. If the file already exists it will abort and report error.
    /*
    if (band_count == 1) {
        clusters.save(sc, "hdfs:///user/emma/spring_index/LastFreeze/all_kmeans_model")
    } else {
        clusters.save(sc, "hdfs:///user/emma/spring_index/BloomFinal/all_kmeans_model")
    }
    */

    /*
     Run Kmeans and obtain the clusters per each cell and collect first 50 results.
    */

    //Cache the model
    band_vec.cache()

    val res = clusters.predict(band_vec)
    res.repartition(1)getNumPartitions

    //Un-persist the model
    band_vec.unpersist()

    //Collect first 50
    val res_out = res.collect().take(50)

    /*
     Show the cluster ID for the first 50 cells
    */

    //res_out.foreach(println)
    println(res_out.size)
    println(band0_index.size)

    /*
     Save the result as GeoTiff. However, it is not straightforward.
     We need to get the clusterCenter which is a RDD[Vectors]
     It contains a vector per year. However, the vector indices
     are the ones from the ArrayOfDoubles with the NaN values.
    */

    //Merge two RDDs
    val cluster_cell_pos = res.repartition(1).zip(sc.parallelize(band0_index, 1))
    //val cluster_cell_pos = res.zip(sc.parallelize(band0_index))
    cluster_cell_pos.collect()//.take(50)

    /*
     Join the RDD with clusters with the Grid of cells from GeoTiff.
     Inspired in:
      https://stackoverflow.com/questions/31257077/how-do-you-perform-basic-joins-of-two-rdd-tables-in-spark-using-python
    */
    val band0_0_1010 = band0.filterByRange(0,1010)
    val grid_clusters = band0_0_1010.repartition(100).leftOuterJoin(cluster_cell_pos.map{ case (c,i) => (i.toLong, c)})
    //val grid_clusters_res = grid_clusters.sortByKey(true).map{case (k, (v, c)) => if (c == None) (k, Double.NaN) else (k, c.get)}//.take(50).foreach(println)
    val grid_clusters_res = grid_clusters.sortByKey(true).map{case (k, (v, c)) => if (c == None) (k, -1.0) else (k, c.get.toDouble)}//.take(50).foreach(println)


    /*
     Create a GeoTiff and save to HDFS.
    */

    val cluster_cells :Array[Double] = grid_clusters_res.values.map(m => m.toDouble).collect()
    val cluster_tile = DoubleArrayTile(cluster_cells, num_cols_rows._1, num_cols_rows._2)
    //val cluster_tile = DoubleArrayTile(cluster_cells, 101, 10)
    val geoTiff = SinglebandGeoTiff(cluster_tile, projected_extent.extent, projected_extent.crs, Tags.empty, GeoTiffOptions.DEFAULT)

    sc.parallelize(geoTiff.toByteArray).saveAsObjectFile("hdfs:///users/emma/spring-index/BloomFinal/cluster.tif")

    //Write to the local file system
    //val path = "~/clusters.tif"
    //GeoTiffWriter.write(geoTiff, path)

  }
}