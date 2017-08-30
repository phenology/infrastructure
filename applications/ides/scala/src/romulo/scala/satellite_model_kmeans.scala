package romulo.scala

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}

import geotrellis.proj4.CRS
import geotrellis.raster.io.geotiff.writer.GeoTiffWriter
import geotrellis.raster.io.geotiff.{SinglebandGeoTiff, _}
import geotrellis.raster.{CellType, DoubleArrayTile, Tile, UByteCellType}
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

    //Using spring-index model
    var inpB_path = "hdfs:///user/hadoop/spring-index/"
    var inpB_dir = "BloomFinal"

    //Using AVHRR Satellite data
    var inpA_path = "hdfs:///user/hadoop/spring-index/"
    var inpA_dir = "LeafFinal"

    var out_path = "hdfs:///user/pheno/kmeans_" + inpB_dir + "_" + inpA_dir + "/"
    var band_num = 3

    //Satellite years between (inclusive) 1989 - 2014
    //Model years between (inclusive) 1980 - 2015

    var first_year = 1980
    var last_year = 2015

    //Mask
    val toBeMasked = true
    val mask_path = "hdfs:///user/hadoop/usa_mask.tif"

    //Kmeans number of iterations and clusters
    var numIterations = 75
    var minClusters = 210
    var maxClusters = 500
    var stepClusters = 10
    var save_kmeans_model = false
    val save_rdds = true
    val save_matrix = true


    // OPERATION VALIDATION
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
    val years = 1980 to 2015
    if (!years.contains(first_year) || !(years.contains(last_year))) {
      println("Invalid range of years for " + inpA_dir + ". I should be between " + first_year + " and " + last_year)
      System.exit(0)
    }
    var years_range = (years.indexOf(first_year), years.indexOf(last_year))

    var num_kmeans: Int = 1
    if (minClusters != maxClusters) {
      num_kmeans = ((maxClusters - minClusters) / stepClusters) + 1
    }
    println(num_kmeans)
    var kmeans_model_paths: Array[String] = Array.fill[String](num_kmeans)("")
    var wssse_path: String = out_path + "/" + numIterations + "_wssse" + "_" + first_year + "_" + last_year
    var geotiff_hdfs_paths: Array[String] = Array.fill[String](num_kmeans)("")
    var geotiff_tmp_paths: Array[String] = Array.fill[String](num_kmeans)("")
    var numClusters_id = 0

    if (num_kmeans > 1) {
      numClusters_id = 0
      cfor(minClusters)(_ <= maxClusters, _ + stepClusters) { numClusters =>
        kmeans_model_paths(numClusters_id) = out_path + "/kmeans_model_" + band_num + "_" + numClusters + "_" + numIterations + "_" + first_year + "_" + last_year

        //Check if the file exists
        val kmeans_exist = fs.exists(new org.apache.hadoop.fs.Path(kmeans_model_paths(numClusters_id)))
        if (kmeans_exist && !kmeans_offline_mode) {
          println("The kmeans model path " + kmeans_model_paths(numClusters_id) + " exists, please remove it.")
        } else if (!kmeans_exist && kmeans_offline_mode) {
          kmeans_offline_mode = false
        }

        geotiff_hdfs_paths(numClusters_id) = out_path + "/clusters_" + band_num + "_" + numClusters + "_" + numIterations + "_" + first_year + "_" + last_year
        geotiff_tmp_paths(numClusters_id) = "/tmp/clusters_" + band_num + "_" + numClusters + "_" + numIterations
        numClusters_id += 1
      }
      kmeans_offline_mode = false
    } else {
      kmeans_model_paths(0) = out_path + "/kmeans_model_" + band_num + "_" + minClusters + "_" + numIterations + "_" + first_year + "_" + last_year
      val kmeans_offline_exists = fs.exists(new org.apache.hadoop.fs.Path(kmeans_model_paths(0)))
      if (kmeans_offline_mode != kmeans_offline_exists) {
        println("\"Kmeans\" offline mode is not set properly, i.e., either it was set to false and the required file does not exist or vice-versa. We will reset it to " + kmeans_offline_exists.toString())
        kmeans_offline_mode = kmeans_offline_exists
      }
      geotiff_hdfs_paths(0) = out_path + "/clusters_" + band_num + "_" + minClusters + "_" + numIterations + "_" + first_year + "_" + last_year
      geotiff_tmp_paths(0) = "/tmp/clusters_" + band_num + "_" + minClusters + "_" + numIterations
    }

    //Global variables
    var inpA_grids_RDD: RDD[Array[Double]] = sc.emptyRDD
    var inpA_grids: RDD[Array[Double]] = sc.emptyRDD
    var inpB_grids_RDD: RDD[Array[Double]] = sc.emptyRDD
    var inpB_grids: RDD[Array[Double]] = sc.emptyRDD
    var projected_extent = new ProjectedExtent(new Extent(0, 0, 0, 0), CRS.fromName("EPSG:3857"))
    var grid0: RDD[(Long, Double)] = sc.emptyRDD
    var grid0_index: RDD[Long] = sc.emptyRDD
    var num_cols_rows: (Int, Int) = (0, 0)
    var cellT: CellType = UByteCellType
    var mask_tile0: Tile = new SinglebandGeoTiff(geotrellis.raster.ArrayTile.empty(cellT, num_cols_rows._1, num_cols_rows._2), projected_extent.extent, projected_extent.crs, Tags.empty, GeoTiffOptions.DEFAULT).tile
    var cells_size: Long = 0
    var inpA_cells_size: Long = 0
    var inpB_cells_size: Long = 0
    var t0: Long = 0
    var t1: Long = 0

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

    //LOAD GeoTiffs

    // GeoTiffs A
    t0 = System.nanoTime()
    //Load Mask
    if (toBeMasked) {
      val mask_tiles_RDD = sc.hadoopGeoTiffRDD(mask_path).values
      val mask_tiles_withIndex = mask_tiles_RDD.zipWithIndex().map { case (e, v) => (v, e) }
      mask_tile0 = (mask_tiles_withIndex.filter(m => m._1 == 0).filter(m => !m._1.isNaN).values.collect()) (0)
    }

    //Local variables
    val pattern: String = "tif"
    val inpA_filepath: String = inpA_path + inpA_dir
    val inpB_filepath: String = inpB_path + "/" + inpB_dir

    t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0) + "ns")

    t0 = System.nanoTime()
    if (inpA_rdd_offline_mode) {
      inpA_grids_RDD = sc.objectFile(inpA_grid_path)
    } else {
      //Lets load MODIS Singleband GeoTiffs and return RDD just with the tiles.
      val inpA_geos_RDD = sc.hadoopGeoTiffRDD(inpA_filepath, pattern)
      val inpA_tiles_RDD = inpA_geos_RDD.values

      if (toBeMasked) {
        val mask_tile_broad: Broadcast[Tile] = sc.broadcast(mask_tile0)
        inpA_grids_RDD = inpA_tiles_RDD.map(m => m.localInverseMask(mask_tile_broad.value, 1, -1000).toArrayDouble().filter(_ != -1000))
      } else {
        inpA_grids_RDD = inpA_tiles_RDD.map(m => m.toArrayDouble())
      }

      //Store in HDFS
      if (save_rdds) {
        inpA_grids_RDD.saveAsObjectFile(inpA_grid_path)
      }
    }
    val inpA_grids_withIndex = inpA_grids_RDD.zipWithIndex().map { case (e, v) => (v, e) }

    //Filter out the range of years:
    inpA_grids = inpA_grids_withIndex.filterByRange(years_range._1, years_range._2).values

    var inpA_grid0_index: RDD[Double] = inpA_grids_withIndex.filter(m => m._1 == 0).values.flatMap(m => m)
    inpA_cells_size = inpA_grid0_index.count().toInt
    println("Number of cells is: " + inpA_cells_size)

    t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0) + "ns")

    //GeoTiffs B
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
      val inpB_geos_RDD = sc.hadoopMultibandGeoTiffRDD(inpB_filepath, pattern)
      val inpB_tiles_RDD = inpB_geos_RDD.values

      //Retrieve the number of cols and rows of the Tile's grid
      val tiles_withIndex = inpB_tiles_RDD.zipWithIndex().map { case (v, i) => (i, v) }
      val tile0 = (tiles_withIndex.filter(m => m._1 == 0).values.collect()) (0)

      num_cols_rows = (tile0.cols, tile0.rows)
      cellT = tile0.cellType

      //Retrieve the ProjectExtent which contains metadata such as CRS and bounding box
      val projected_extents_withIndex = inpB_geos_RDD.keys.zipWithIndex().map { case (e, v) => (v, e) }
      projected_extent = (projected_extents_withIndex.filter(m => m._1 == 0).values.collect()) (0)

      val band_numB: Broadcast[Int] = sc.broadcast(band_num)
      if (toBeMasked) {
        val mask_tile_broad: Broadcast[Tile] = sc.broadcast(mask_tile0)
        inpB_grids_RDD = inpB_tiles_RDD.map(m => m.band(band_numB.value).localInverseMask(mask_tile_broad.value, 1, -1000).toArrayDouble())
      } else {
        inpB_grids_RDD = inpB_tiles_RDD.map(m => m.band(band_numB.value).toArrayDouble())
      }

      //Get Index for each Cell
      val grids_withIndex = inpB_grids_RDD.zipWithIndex().map { case (e, v) => (v, e) }
      if (toBeMasked) {
        grid0_index = grids_withIndex.filter(m => m._1 == 0).values.flatMap(m => m).zipWithIndex.filter(m => m._1 != -1000.0).map { case (v, i) => (i) }
      } else {
        grid0_index = grids_withIndex.filter(m => m._1 == 0).values.flatMap(m => m).zipWithIndex.map { case (v, i) => (i) }
      }

      //Get the Tile's grid
      grid0 = grids_withIndex.filter(m => m._1 == 0).values.flatMap(m => m).zipWithIndex.map { case (v, i) => (i, v) }

      //Lets filter out NaN
      if (toBeMasked) {
        inpB_grids_RDD = inpB_grids_RDD.map(m => m.filter(m => m != -1000.0))
      } else {
        inpB_grids_RDD = inpB_grids_RDD
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

    var inpB_tile0_index: RDD[Double] = inpB_grids_withIndex.filter(m => m._1 == 0).values.flatMap(m => m)
    inpB_cells_size = inpB_tile0_index.count().toInt
    println("Number of cells is: " + inpB_cells_size)

    if (inpA_cells_size != inpB_cells_size){
      println("Cells size differs: " + inpA_cells_size + " and " + inpB_cells_size)
      System.exit(0)
    } else {
      cells_size = inpA_cells_size
    }

    t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0) + "ns")

    //MATRIX
    t0 = System.nanoTime()
    var grids_matrix: RDD[Vector] = sc.emptyRDD
    val inp_grids :RDD[Array[Double]] = inpA_grids.flatMap(m => m).zipWithUniqueId().map{ case (v,i) => (i,v)}.join(inpB_grids.flatMap(m => m).zipWithUniqueId().map{case (v,i) => (i,v)}).map{case (i, (a1,a2)) => Array(a1, a2)}

    if (matrix_offline_mode) {
      grids_matrix = sc.objectFile(matrix_path)
    } else {
      //Dense Vector
      //grids_matrix = inp_grids.map(m => Vectors.dense(m))
      //Sparse Vector
      val cells_sizeB = sc.broadcast(cells_size)
      grids_matrix = inp_grids.map(m => m.zipWithIndex).map(m => m.filter(!_._1.isNaN)).map(m => Vectors.sparse(cells_sizeB.value.toInt, m.map(v => v._2), m.map(v => v._1)))
      grids_matrix.saveAsObjectFile(matrix_path)
    }
    t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0) + "ns")

    //KMEANS TRAINING
    t0 = System.nanoTime()
    //Global variables
    var kmeans_models :Array[KMeansModel] = new Array[KMeansModel](num_kmeans)
    var wssse_data :List[(Int, Int, Double)] = List.empty

    if (kmeans_offline_mode) {
      numClusters_id = 0
      cfor(minClusters)(_ <= maxClusters, _ + stepClusters) { numClusters =>
        if (!fs.exists(new org.apache.hadoop.fs.Path(kmeans_model_paths(numClusters_id)))) {
          println("One of the files does not exist, we will abort!!!")
          System.exit(0)
        } else {
          kmeans_models(numClusters_id) = KMeansModel.load(sc, kmeans_model_paths(numClusters_id))
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
        kmeans_models(numClusters_id) = {
          KMeans.train(grids_matrix, numClusters, numIterations)
        }

        // Evaluate clustering by computing Within Set Sum of Squared Errors
        val WSSSE = kmeans_models(numClusters_id).computeCost(grids_matrix)
        println("Within Set Sum of Squared Errors = " + WSSSE)

        wssse_data = wssse_data :+ (numClusters, numIterations, WSSSE)

        //Save kmeans model
        if (save_kmeans_model) {
          if (!fs.exists(new org.apache.hadoop.fs.Path(kmeans_model_paths(numClusters_id)))) {
            kmeans_models(numClusters_id).save(sc, kmeans_model_paths(numClusters_id))
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

    // KMEANS CLUSTERING
    t0 = System.nanoTime()
    //Cache it so kmeans is more efficient
    grids_matrix.cache()

    var kmeans_res: Array[RDD[Int]] = Array.fill(num_kmeans)(sc.emptyRDD)
    numClusters_id = 0
    cfor(minClusters)(_ <= maxClusters, _ + stepClusters) { numClusters =>
      kmeans_res(numClusters_id) = kmeans_models(numClusters_id).predict(grids_matrix)
      numClusters_id += 1
    }

    //Un-persist it to save memory
    grids_matrix.unpersist()
    t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0) + "ns")

    //CREATE GeoTiffs
    t0 = System.nanoTime()
    numClusters_id = 0
    val grid0_index_I = grid0_index.zipWithIndex().map{ case (v,i) => (i,v)}
    grid0_index_I.cache()
    var num_cells = kmeans_res(0).count().toInt
    var cells_per_year = num_cells / ((last_year-first_year)+1)
    println(cells_per_year)
    println(num_cells)
    val cells_per_yearB = sc.broadcast(cells_per_year)

    cfor(minClusters)(_ <= maxClusters, _ + stepClusters) { numClusters =>
      //Merge two RDDs, one containing the clusters_ID indices and the other one the indices of a Tile's grid cells
      var year :Int = 0
      cfor(0) (_ < num_cells, _ + cells_per_year) { cellID =>
        println("Saving GeoTiff for numClustersID: " + numClusters_id + " year: " + year)
        val cellIDB = sc.broadcast(cellID)
        val kmeans_res_sing = kmeans_res(numClusters_id)
        val cluster_cell_pos = ((kmeans_res(numClusters_id).zipWithIndex().map{ case (v,i) => (i,v)}.filterByRange(cellIDB.value, (cellIDB.value+cells_per_yearB.value-1)).map{case (i,v) => (i-cellIDB.value, v)}.join(grid0_index_I)).map{ case (k,(v,i)) => (v,i)})

        //Associate a Cluster_IDs to respective Grid_cell
        val grid_clusters :RDD[ (Long, (Double, Option[Int]))] = grid0.leftOuterJoin(cluster_cell_pos.map{ case (c,i) => (i.toLong, c)})

        //Convert all None to NaN
        val grid_clusters_res = grid_clusters.sortByKey(true).map{case (k, (v, c)) => if (c == None) (k, Double.NaN) else (k, c.get.toDouble)}

        //Define a Tile
        val cluster_cells :Array[Double] = grid_clusters_res.values.collect()
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
      numClusters_id += 1
    }
    cells_per_yearB.destroy()
    grid0_index_I.unpersist()
    t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0) + "ns")

  }
}

