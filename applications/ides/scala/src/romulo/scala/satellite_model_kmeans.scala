package romulo.scala

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}

import geotrellis.proj4.CRS
import geotrellis.raster.io.geotiff.writer.GeoTiffWriter
import geotrellis.raster.io.geotiff.{SinglebandGeoTiff, _}
import geotrellis.raster.{CellType, DoubleArrayTile, MultibandTile, Tile, UByteCellType}
import geotrellis.spark.io.hadoop._
import geotrellis.vector.{Extent, ProjectedExtent}
import org.apache.hadoop.io.SequenceFile.Writer
import org.apache.hadoop.io.{SequenceFile, _}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.sys.process._

//Spire is a numeric library for Scala which is intended to be generic, fast, and precise.
import spire.syntax.cfor._

object satellite_model_kmeans extends App {

  override def main(args: Array[String]): Unit = {
    val appName = this.getClass.getName
    val masterURL = "spark://emma0.emma.nlesc.nl:7077"
    val sc = new SparkContext(new SparkConf().setAppName(appName).setMaster(masterURL))

    //OPERATION MODE
    var inpB_rdd_offline_mode = true
    var inpA_rdd_offline_mode = true
    var matrix_offline_mode = true
    var kmeans_offline_mode = true

    //Using spring-index inpB
    var inpB_path = "hdfs:///user/hadoop/spring-index/"
    var inpB_dir = "BloomFinal"

    //Using AVHRR Satellite data
    var inpA_path = "hdfs:///user/hadoop/spring-index/"
    var inpA_dir = "LeafFinal"

    var out_path = "hdfs:///user/pheno/kmeans_" + inpB_dir + "_" + inpA_dir + "CentroidS/"
    var band_num = 3

    //Satellite years between (inclusive) 1989 - 2014
    //Model years between (inclusive) 1980 - 2015

    val timeseries = (1980, 2015)
    var first_year = 1980
    var last_year = 2015

    //Mask
    val toBeMasked = true
    val mask_path = "hdfs:///user/hadoop/usa_mask.tif"

    //Kmeans number of iterations and clusters
    var numIterations = 75
    var minClusters = 70
    var maxClusters = 70
    var stepClusters = 10
    var save_kmeans_inpB = false
    val save_rdds = false
    val save_matrix = false




    //OPERATION VALIDATION//
    var conf = sc.hadoopConfiguration
    var fs = org.apache.hadoop.fs.FileSystem.get(conf)

    //Paths to store data structures for Offline runs
    var mask_str = ""
    if (toBeMasked)
      mask_str = "_mask"

    var inpB_grid_path = out_path + inpB_dir + "_grid"
    var inpA_grid_path = out_path + inpA_dir + "_grid"
    var grid0_path = out_path + inpB_dir + "_grid0"
    var grid0_index_path = out_path + inpB_dir + "_grid0_index"
    var matrix_path = out_path + inpB_dir + "_matrix" + "_" + first_year + "_" + last_year
    var metadata_path = out_path + inpB_dir + "_metadata"

    val inpB_rdd_offline_exists = fs.exists(new org.apache.hadoop.fs.Path(inpB_grid_path))
    val inpA_rdd_offline_exists = fs.exists(new org.apache.hadoop.fs.Path(inpA_grid_path))
    val matrix_offline_exists = fs.exists(new org.apache.hadoop.fs.Path(matrix_path))

    if (minClusters > maxClusters) {
      maxClusters = minClusters
      stepClusters = 1
    }
    if (stepClusters < 1) {
      stepClusters = 1
    }

    if (inpB_rdd_offline_mode != inpB_rdd_offline_exists) {
      println("\"Load GeoTiffs\" offline mode is not set properly, i.e., either it was set to false and the required file does not exist or vice-versa. We will reset it to " + inpB_rdd_offline_exists.toString())
      inpB_rdd_offline_mode = inpB_rdd_offline_exists
    }

    if (inpA_rdd_offline_mode != inpA_rdd_offline_exists) {
      println("\"Load GeoTiffs\" offline mode is not set properly, i.e., either it was set to false and the required file does not exist or vice-versa. We will reset it to " + inpA_rdd_offline_exists.toString())
      inpA_rdd_offline_mode = inpA_rdd_offline_exists
    }

    if (matrix_offline_mode != matrix_offline_exists) {
      println("\"Matrix\" offline mode is not set properly, i.e., either it was set to false and the required file does not exist or vice-versa. We will reset it to " + matrix_offline_exists.toString())
      matrix_offline_mode = matrix_offline_exists
    }

    //Years
    //Years
    val years = timeseries._1 to timeseries._2

    if (!years.contains(first_year) || !(years.contains(last_year))) {
      println("Invalid range of years for " + inpB_dir + ". I should be between " + first_year + " and " + last_year)
      System.exit(0)
    }

    var years_range = (years.indexOf(first_year), years.indexOf(last_year))

    var num_kmeans: Int = 1
    if (minClusters != maxClusters) {
      num_kmeans = ((maxClusters - minClusters) / stepClusters) + 1
    }
    println(num_kmeans)
    var kmeans_inpB_paths: Array[String] = Array.fill[String](num_kmeans)("")
    var wssse_path: String = out_path + "/" + numIterations + "_wssse" + "_" + first_year + "_" + last_year
    var geotiff_hdfs_paths: Array[String] = Array.fill[String](num_kmeans)("")
    var geotiff_tmp_paths: Array[String] = Array.fill[String](num_kmeans)("")
    var numClusters_id = 0

    if (num_kmeans > 1) {
      numClusters_id = 0
      cfor(minClusters)(_ <= maxClusters, _ + stepClusters) { numClusters =>
        kmeans_inpB_paths(numClusters_id) = out_path + "/kmeans_inpB_" + band_num + "_" + numClusters + "_" + numIterations + "_" + first_year + "_" + last_year

        //Check if the file exists
        val kmeans_exist = fs.exists(new org.apache.hadoop.fs.Path(kmeans_inpB_paths(numClusters_id)))
        if (kmeans_exist && !kmeans_offline_mode) {
          println("The kmeans inpB path " + kmeans_inpB_paths(numClusters_id) + " exists, please remove it.")
        } else if (!kmeans_exist && kmeans_offline_mode) {
          kmeans_offline_mode = false
        }

        geotiff_hdfs_paths(numClusters_id) = out_path + "/clusters_" + band_num + "_" + numClusters + "_" + numIterations + "_" + first_year + "_" + last_year
        geotiff_tmp_paths(numClusters_id) = "/tmp/clusters_" + band_num + "_" + numClusters + "_" + numIterations
        numClusters_id += 1
      }
      kmeans_offline_mode = false
    } else {
      kmeans_inpB_paths(0) = out_path + "/kmeans_inpB_" + band_num + "_" + minClusters + "_" + numIterations + "_" + first_year + "_" + last_year
      val kmeans_offline_exists = fs.exists(new org.apache.hadoop.fs.Path(kmeans_inpB_paths(0)))
      if (kmeans_offline_mode != kmeans_offline_exists) {
        println("\"Kmeans\" offline mode is not set properly, i.e., either it was set to false and the required file does not exist or vice-versa. We will reset it to " + kmeans_offline_exists.toString())
        kmeans_offline_mode = kmeans_offline_exists
      }
      geotiff_hdfs_paths(0) = out_path + "/clusters_" + band_num + "_" + minClusters + "_" + numIterations + "_" + first_year + "_" + last_year
      geotiff_tmp_paths(0) = "/tmp/clusters_" + band_num + "_" + minClusters + "_" + numIterations
    }

    //Global variables
    var inpA_grids_RDD: RDD[(Long,Array[Double])] = sc.emptyRDD
    var inpA_grids: RDD[(Long,Array[Double])] = sc.emptyRDD
    var inpB_grids_RDD: RDD[(Long, Array[Double])] = sc.emptyRDD
    var inpB_grids: RDD[(Long, Array[Double])] = sc.emptyRDD
    var projected_extent = new ProjectedExtent(new Extent(0, 0, 0, 0), CRS.fromName("EPSG:3857"))
    var grid0: RDD[(Long, Double)] = sc.emptyRDD
    var grid0_index: RDD[Long] = sc.emptyRDD
    var num_cols_rows: (Int, Int) = (0, 0)
    var cellT: CellType = UByteCellType
    var mask_tile0: Tile = new SinglebandGeoTiff(geotrellis.raster.ArrayTile.empty(cellT, num_cols_rows._1, num_cols_rows._2), projected_extent.extent, projected_extent.crs, Tags.empty, GeoTiffOptions.DEFAULT).tile
    //We are comparing 2 data sets only
    var cells_size: Long = 2
    var t0: Long = 0
    var t1: Long = 0




    //FUNCTIONS to (DE)SERIALIZE ANY STRUCTURE
    def serialize(value: Any): Array[Byte] = {
      val out_stream: ByteArrayOutputStream = new ByteArrayOutputStream()
      val obj_out_stream = new ObjectOutputStream(out_stream)
      obj_out_stream.writeObject(value)
      obj_out_stream.close
      out_stream.toByteArray
    }

    def deserialize(bytes: Array[Byte]): Any = {
      val obj_in_stream = new ObjectInputStream(new ByteArrayInputStream(bytes))
      val value = obj_in_stream.readObject
      obj_in_stream.close
      value
    }




    //LOAD GEOTIFFS
    //----//
    def hadoopGeoTiffRDD(inpA_filepath :String, pattern :String): RDD[(Int, (ProjectedExtent, Tile))] = {
      val listFiles = sc.binaryFiles(inpA_filepath + "/" + pattern).sortBy(_._1).keys.collect()
      var prevRDD :RDD[(Int,(ProjectedExtent, Tile))] = sc.emptyRDD

      cfor(0)(_ < listFiles.length, _ + 1) { k =>
        val filePath :String = listFiles(k)
        val kB = sc.broadcast(k)
        val currRDD = sc.hadoopGeoTiffRDD(filePath).map(m => (kB.value, m))
        prevRDD = currRDD.union(prevRDD)
        //kB.destroy()
      }
      prevRDD.sortBy(_._1)
    }

    //---//
    def hadoopMultibandGeoTiffRDD(inpA_filepath :String, pattern :String): RDD[(Int, (ProjectedExtent, MultibandTile))] = {
      val listFiles = sc.binaryFiles(inpA_filepath + "/" + pattern).sortBy(_._1).keys.collect()
      var prevRDD :RDD[(Int,(ProjectedExtent, MultibandTile))] = sc.emptyRDD

      cfor(0)(_ < listFiles.length, _ + 1) { k =>
        val filePath :String = listFiles(k)
        val kB = sc.broadcast(k)
        val currRDD = sc.hadoopMultibandGeoTiffRDD(filePath).map(m => (kB.value,m))
        prevRDD = currRDD.union(prevRDD)
        //kB.destroy()
      }
      prevRDD.sortBy(_._1)
    }



    //GEOTIFFS A//
    t0 = System.nanoTime()
    //Load Mask
    if (toBeMasked) {
      val mask_tiles_RDD = sc.hadoopGeoTiffRDD(mask_path).values
      val mask_tiles_withIndex = mask_tiles_RDD.zipWithIndex().map { case (e, v) => (v, e) }
      mask_tile0 = (mask_tiles_withIndex.filter(m => m._1 == 0).filter(m => !m._1.isNaN).values.collect()) (0)
    }

    //Local variables
    val pattern: String = "*.tif"
    val inpA_filepath: String = inpA_path + inpA_dir
    val inpB_filepath: String = inpB_path + "/" + inpB_dir

    t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0) + "ns")

    t0 = System.nanoTime()
    if (inpA_rdd_offline_mode) {
      inpA_grids_RDD = sc.objectFile(inpA_grid_path)
    } else {
      val inpA_geos_RDD = hadoopMultibandGeoTiffRDD(inpA_filepath, pattern)
      val inpA_tiles_RDD = inpA_geos_RDD.map{ case (i,(p,t)) => (i,t)}

      val band_numB :Broadcast[Int] = sc.broadcast(band_num)
      if (toBeMasked) {
        val mask_tile_broad: Broadcast[Tile] = sc.broadcast(mask_tile0)
        inpA_grids_RDD = inpA_tiles_RDD.map{case (i,m) => (i,m.band(band_numB.value).localInverseMask(mask_tile_broad.value, 1, -1000).toArrayDouble().filter(_ != -1000).filter(!_.isNaN))}
      } else {
        inpA_grids_RDD = inpA_tiles_RDD.map{case (i,m) => (i,m.band(band_numB.value).toArrayDouble().filter(!_.isNaN))}
      }

      //Store in HDFS
      if (save_rdds) {
        inpA_grids_RDD.saveAsObjectFile(inpA_grid_path)
      }
    }
    val inpA_grids_withIndex = inpA_grids_RDD//.zipWithIndex().map { case (e, v) => (v, e) }

    //Filter out the range of years:
    val inpA_year_diff = first_year-timeseries._1
    val inputA_year_diffB = sc.broadcast(inpA_year_diff)
    inpA_grids = inpA_grids_withIndex.filterByRange(years_range._1, years_range._2).map{ case(i,a) => (i-(inputA_year_diffB.value),a)}//.values

    var inpA_grid0_index: RDD[Double] = inpA_grids_withIndex.filter(m => m._1 == 0).values.flatMap(m => m)

    t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0) + "ns")




    //GEOTIFFS B//
    t0 = System.nanoTime()
    if (inpB_rdd_offline_mode) {
      inpB_grids_RDD = sc.objectFile(inpB_grid_path)
      grid0 = sc.objectFile(grid0_path)
      grid0_index = sc.objectFile(grid0_index_path)

      val metadata = sc.sequenceFile(metadata_path, classOf[IntWritable], classOf[BytesWritable]).map(_._2.copyBytes()).collect()
      projected_extent = deserialize(metadata(0)).asInstanceOf[ProjectedExtent]
      num_cols_rows = (deserialize(metadata(1)).asInstanceOf[Int], deserialize(metadata(2)).asInstanceOf[Int])
      cellT = deserialize(metadata(3)).asInstanceOf[CellType]
    } else {
      val inpB_geos_RDD = hadoopMultibandGeoTiffRDD(inpB_filepath, pattern)
      val inpB_tiles_RDD = inpB_geos_RDD.map{case (i,(p,t)) => (i,t)}

      //Retrieve the number of cols and rows of the Tile's grid
      val tiles_withIndex = inpB_tiles_RDD//.zipWithIndex().map { case (v, i) => (i, v) }
      val tile0 = (tiles_withIndex.filter(m => m._1 == 0).values.collect()) (0)

      num_cols_rows = (tile0.cols, tile0.rows)
      cellT = tile0.cellType

      //Retrieve the ProjectExtent which contains metadata such as CRS and bounding box
      val projected_extents_withIndex = inpB_geos_RDD.map{case (i,(p,t)) => (i,p)}
      projected_extent = (projected_extents_withIndex.filter(m => m._1 == 0).values.collect()) (0)

      val band_numB: Broadcast[Int] = sc.broadcast(band_num)
      if (toBeMasked) {
        val mask_tile_broad: Broadcast[Tile] = sc.broadcast(mask_tile0)
        inpB_grids_RDD = inpB_tiles_RDD.map{ case (i, m) => (i,m.band(band_numB.value).localInverseMask(mask_tile_broad.value, 1, -1000).toArrayDouble())}
      } else {
        inpB_grids_RDD = inpB_tiles_RDD.map{ case (i, m) => (i, m.band(band_numB.value).toArrayDouble())}
      }

      //Get Index for each Cell
      val grids_withIndex = inpB_grids_RDD//.zipWithIndex().map { case (e, v) => (v, e) }
      if (toBeMasked) {
        grid0_index = grids_withIndex.filter(m => m._1 == 0).values.flatMap(m => m).zipWithIndex.filter(m => m._1 != -1000.0).filter(m => !m._1.isNaN).map { case (v, i) => (i) }
      } else {
        grid0_index = grids_withIndex.filter(m => m._1 == 0).values.flatMap(m => m).filter(m => !m.isNaN).zipWithIndex.map { case (v, i) => (i) }
      }

      //Get the Tile's grid
      grid0 = grids_withIndex.filter(m => m._1 == 0).values.flatMap(m => m).zipWithIndex.map { case (v, i) => (i, v) }

      //Lets filter out NaN
      if (toBeMasked) {
        inpB_grids_RDD = inpB_grids_RDD.map{ case (i,m) => (i,m.filter(m => m != -1000.0).filter(m => !m.isNaN))}
      } else {
        inpB_grids_RDD = inpB_grids_RDD.map{ case (i,m) => (i, m.filter(!_.isNaN))}
      }

      //Store data in HDFS
      if (save_rdds) {
        grid0.saveAsObjectFile(grid0_path)
        grid0_index.saveAsObjectFile(grid0_index_path)
        inpB_grids_RDD.saveAsObjectFile(inpB_grid_path)

        val writer: SequenceFile.Writer = SequenceFile.createWriter(conf,
          Writer.file(metadata_path),
          Writer.keyClass(classOf[IntWritable]),
          Writer.valueClass(classOf[BytesWritable])
        )

        writer.append(new IntWritable(1), new BytesWritable(serialize(projected_extent)))
        writer.append(new IntWritable(2), new BytesWritable(serialize(num_cols_rows._1)))
        writer.append(new IntWritable(3), new BytesWritable(serialize(num_cols_rows._2)))
        writer.append(new IntWritable(4), new BytesWritable(serialize(cellT)))
        writer.hflush()
        writer.close()
      }
    }
    val inpB_grids_withIndex = inpB_grids_RDD.zipWithIndex().map { case (e, v) => (v, e) }

    //Filter out the range of years:
    inpB_grids = inpB_grids_withIndex.filterByRange(years_range._1, years_range._2).values

    t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0) + "ns")


    grid0.cache()
    val nans_index = grid0.filter(_._2 != -1000).map(_._2).zipWithIndex().filter(_._1.isNaN).map(_._2).collect()
    val nans_indexB = sc.broadcast(nans_index)
    val inpA_grids_noNaNs = inpA_grids.map{ case (i,v) => (i,v.zipWithIndex.filter{ case (v,i) => nans_indexB.value.contains(i)}.map{ case (v,i) => v})}


    //MATRIX//
    t0 = System.nanoTime()
    var grids_matrix: RDD[Vector] = sc.emptyRDD

    if (matrix_offline_mode) {
      grids_matrix = sc.objectFile(matrix_path)
    } else {
      val inp_grids :RDD[Array[Double]] = inpA_grids_noNaNs.flatMap{ case (i,m) => m}.zipWithIndex().map{ case (v,i) => (i,v)}.join(inpB_grids.flatMap{case (i,m) => m}.zipWithIndex().map{case (v,i) => (i,v)}).sortByKey(true).map{case (i, (a1,a2)) => Array(a1, a2)}
      val cells_sizeB = sc.broadcast(cells_size)
      grids_matrix = inp_grids.map(m => m.zipWithIndex).map(m => m.filter(!_._1.isNaN)).map(m => Vectors.sparse(cells_sizeB.value.toInt, m.map(v => v._2), m.map(v => v._1)))
      if (save_matrix)
        grids_matrix.saveAsObjectFile(matrix_path)
    }
    t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0) + "ns")

    inpA_grids_noNaNs.cache()
    inpB_grids.cache()



    //KMEANS TRAINING//
    t0 = System.nanoTime()
    //Global variables
    var kmeans_inpBs :Array[KMeansModel] = new Array[KMeansModel](num_kmeans)
    var wssse_data :List[(Int, Int, Double)] = List.empty

    if (kmeans_offline_mode) {
      numClusters_id = 0
      cfor(minClusters)(_ <= maxClusters, _ + stepClusters) { numClusters =>
        if (!fs.exists(new org.apache.hadoop.fs.Path(kmeans_inpB_paths(numClusters_id)))) {
          println("One of the files does not exist, we will abort!!!")
          System.exit(0)
        } else {
          kmeans_inpBs(numClusters_id) = KMeansModel.load(sc, kmeans_inpB_paths(numClusters_id))
        }
        numClusters_id += 1
      }
      val wssse_data_RDD :RDD[(Int, Int, Double)]  = sc.objectFile(wssse_path)
      wssse_data  = wssse_data_RDD.collect().toList
    } else {
      numClusters_id = 0
      if (fs.exists(new org.apache.hadoop.fs.Path(wssse_path))) {
        val wssse_data_RDD :RDD[(Int, Int, Double)]  = sc.objectFile(wssse_path)
        wssse_data  = wssse_data_RDD.collect().toList
      }
      grids_matrix.cache()
      cfor(minClusters)(_ <= maxClusters, _ + stepClusters) { numClusters =>
        println(numClusters)
        kmeans_inpBs(numClusters_id) = {
          KMeans.train(grids_matrix, numClusters, numIterations)
        }

        // Evaluate clustering by computing Within Set Sum of Squared Errors
        val WSSSE = kmeans_inpBs(numClusters_id).computeCost(grids_matrix)
        println("Within Set Sum of Squared Errors = " + WSSSE)

        wssse_data = wssse_data :+ (numClusters, numIterations, WSSSE)

        //Save kmeans inpB
        if (save_kmeans_inpB) {
          if (!fs.exists(new org.apache.hadoop.fs.Path(kmeans_inpB_paths(numClusters_id)))) {
            kmeans_inpBs(numClusters_id).save(sc, kmeans_inpB_paths(numClusters_id))
          }
        }
        numClusters_id += 1

        if (fs.exists(new org.apache.hadoop.fs.Path(wssse_path))) {
          println("We will delete the wssse file")
          try { fs.delete(new org.apache.hadoop.fs.Path(wssse_path), true) } catch { case _ : Throwable => { } }
        }

        println("Lets create it with the new data")
        sc.parallelize(wssse_data, 1).saveAsObjectFile(wssse_path)
      }

      //Un-persist it to save memory
      grids_matrix.unpersist()

    }
    t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0) + "ns")




    //INSPECT WSSSE//
    t0 = System.nanoTime()
    //current
    println(wssse_data)

    //from disk
    if (fs.exists(new org.apache.hadoop.fs.Path(wssse_path))) {
      var wssse_data_tmp :RDD[(Int, Int, Double)] = sc.objectFile(wssse_path)//.collect()//.toList
      println(wssse_data_tmp.collect().toList)
    }
    t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0) + "ns")




    //KMEANS CLUSTERING//
    t0 = System.nanoTime()
    //Cache it so kmeans is more efficient
    grids_matrix.cache()

    var kmeans_res: Array[RDD[Int]] = Array.fill(num_kmeans)(sc.emptyRDD)
    var kmeans_centroids: Array[Array[Double]] = Array.fill(num_kmeans)(Array.emptyDoubleArray)
    numClusters_id = 0
    cfor(minClusters)(_ <= maxClusters, _ + stepClusters) { numClusters =>
      kmeans_res(numClusters_id) = kmeans_inpBs(numClusters_id).predict(grids_matrix)
      kmeans_centroids(numClusters_id) = kmeans_inpBs(numClusters_id).clusterCenters.map(m => m(0))
      numClusters_id += 1
    }

    //Un-persist it to save memory
    grids_matrix.unpersist()
    t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0) + "ns")

    //-----//
    t0 = System.nanoTime()
    val kmeans_res_out = kmeans_res(0).take(150)
    kmeans_res_out.foreach(print)

    println(kmeans_res_out.size)
    t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0) + "ns")


    //CREATE GeoTiffs
    t0 = System.nanoTime()
    numClusters_id = 0
    grid0_index.cache()
    grid0.cache()
    val grid0_index_I = grid0_index.zipWithIndex().map{ case (v,i) => (i,v)}
    grid0_index_I.cache()
    grid0_index.unpersist()
    kmeans_res(0).cache()
    var num_cells = kmeans_res(0).count().toInt
    kmeans_res(0).unpersist()
    var cells_per_year = num_cells / ((last_year-first_year)+1)
    println(cells_per_year)
    println(num_cells)
    val cells_per_yearB = sc.broadcast(cells_per_year)

    cfor(minClusters)(_ <= maxClusters, _ + stepClusters) { numClusters =>
      //Merge two RDDs, one containing the clusters_ID indices and the other one the indices of a Tile's grid cells
      var year :Int = 0
      val kmeans_res_zip = kmeans_res(numClusters_id).zipWithIndex()
      kmeans_res_zip.cache()
      cfor(0) (_ < num_cells, _ + cells_per_year) { cellID =>
        println("Saving GeoTiff for numClustersID: " + numClusters_id + " year: " + years(year))
        val cellIDB = sc.broadcast(cellID)
        val kmeans_res_sing = kmeans_res(numClusters_id)
        val cluster_cell_pos = ((kmeans_res_zip.map{ case (v,i) => (i,v)}.filterByRange(cellIDB.value, (cellIDB.value+cells_per_yearB.value-1)).map{case (i,v) => (i-cellIDB.value, v)}.join(grid0_index_I)).map{ case (k,(v,i)) => (v,i)})

        //Associate a Cluster_IDs to respective Grid_cell
        val grid_clusters :RDD[ (Long, (Double, Option[Int]))] = grid0.leftOuterJoin(cluster_cell_pos.map{ case (c,i) => (i.toLong, c)})

        //Convert all None to NaN
        val grid_clusters_res = grid_clusters.sortByKey(true).map{case (k, (v, c)) => if (c == None) (k, Int.MaxValue) else (k, c.get)}

        //Define a Tile
        val cluster_cellsID :Array[Int] = grid_clusters_res.values.collect()
        var cluster_cells :Array[Double] = Array.fill(cluster_cellsID.length)(Double.NaN)
        cfor(0)(_ < cluster_cellsID.size, _ + 1) { cellID =>
          if (cluster_cellsID(cellID) != Int.MaxValue) {
            cluster_cells(cellID) = kmeans_centroids(numClusters_id)(cluster_cellsID(cellID))
          }
        }
        val cluster_cellsD = DoubleArrayTile(cluster_cells, num_cols_rows._1, num_cols_rows._2)
        val geoTif = new SinglebandGeoTiff(cluster_cellsD, projected_extent.extent, projected_extent.crs, Tags.empty, GeoTiffOptions(compression.DeflateCompression))

        //Save to /tmp/
        GeoTiffWriter.write(geoTif, geotiff_tmp_paths(numClusters_id) + "_" + years(year) + ".tif")

        //Upload to HDFS
        var cmd = "hadoop dfs -copyFromLocal -f " + geotiff_tmp_paths(numClusters_id) + "_" + years(year) + ".tif" + " " + geotiff_hdfs_paths(numClusters_id) + "_" + years(year) + ".tif"
        Process(cmd)!

        //Remove from /tmp/
        cmd = "rm -fr " + geotiff_tmp_paths(numClusters_id) + "_" + years(year) + ".tif"
        Process(cmd)!

        cellIDB.destroy()
        year += 1
      }
      kmeans_res_zip.unpersist()
      numClusters_id += 1
    }
    cells_per_yearB.destroy()
    grid0_index_I.unpersist()
    grid0.unpersist()
    t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0) + "ns")

  }
}

