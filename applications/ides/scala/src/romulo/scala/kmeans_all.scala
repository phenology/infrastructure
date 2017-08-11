
import sys.process._

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}


import geotrellis.proj4.CRS
import geotrellis.raster.{CellType, DoubleArrayTile, Tile, UByteCellType}
import geotrellis.raster.io.geotiff._
import geotrellis.raster.io.geotiff.writer.GeoTiffWriter
import geotrellis.raster.io.geotiff.SinglebandGeoTiff
import geotrellis.spark.io.hadoop._
import org.apache.hadoop.io._
import geotrellis.vector.{Extent, ProjectedExtent}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry, RowMatrix}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import org.apache.hadoop.io.{SequenceFile}
import org.apache.hadoop.io.SequenceFile.Writer

//Spire is a numeric library for Scala which is intended to be generic, fast, and precise.
import spire.syntax.cfor._

object kmeans_all extends App {

  override def main(args: Array[String]): Unit = {
    val appName = this.getClass.getName
    val masterURL = "spark://emma0.emma.nlesc.nl:7077"
    val sc = new SparkContext(new SparkConf().setAppName(appName).setMaster(masterURL))

    //Operation mode
    var rdd_offline_mode = true
    var matrix_offline_mode = true
    var kmeans_offline_mode = true

    //GeoTiffs to be read from "hdfs:///user/hadoop/spring-index/"
    var dir_path = "hdfs:///user/hadoop/spring-index/"
    var offline_dir_path = "hdfs:///user/emma/spring-index/"
    var geoTiff_dir = "LeafFinal"
    var geoTiff_2_dir = "BloomFinal"
    var band_num = 3

    //Years between (inclusive) 1980 - 2015
    var model_first_year = 1989
    var model_last_year = 2014

    //Mask
    val toBeMasked = true
    val mask_path = "hdfs:///user/hadoop/usa_mask.tif"

    //Kmeans number of iterations and clusters
    var numIterations = 75
    var minClusters = 210
    var maxClusters = 500
    var stepClusters = 10
    var save_kmeans_model = false

    //Validation, do not modify these lines.
    var single_band = false
    if (geoTiff_dir == "BloomFinal" || geoTiff_dir == "LeafFinal") {
      single_band = false
    } else if (geoTiff_dir == "LastFreeze" || geoTiff_dir == "DamageIndex") {
      single_band = true
      if (band_num > 0) {
        println("Since LastFreezze and DamageIndex are single band, we will use band 0!!!")
        band_num = 0
      }
    } else {
      println("Directory unknown, please set either BloomFinal, LeafFinal, LastFreeze or DamageIndex!!!")
    }

    if (minClusters > maxClusters) {
      maxClusters = minClusters
      stepClusters = 1
    }
    if (stepClusters < 1) {
      stepClusters = 1
    }

    //Paths to store data structures for Offline runs
    var mask_str = ""
    if (toBeMasked)
      mask_str = "_mask"
    var grid0_path = offline_dir_path + geoTiff_dir + "/grid0" + mask_str
    var grid0_index_path = offline_dir_path + geoTiff_dir + "/grid0_index" + mask_str
    var grids_noNaN_path = offline_dir_path + geoTiff_dir + "/grids_noNaN" + mask_str
    var metadata_path = offline_dir_path + geoTiff_dir + "/metadata" + mask_str
    var grids_matrix_path = offline_dir_path + geoTiff_dir + "/grids_matrix" + mask_str

    //Check offline modes
    var conf = sc.hadoopConfiguration
    var fs = org.apache.hadoop.fs.FileSystem.get(conf)

    val rdd_offline_exists = fs.exists(new org.apache.hadoop.fs.Path(grid0_path))
    val matrix_offline_exists = fs.exists(new org.apache.hadoop.fs.Path(grids_matrix_path))

    if (rdd_offline_mode != rdd_offline_exists) {
      println("\"Load GeoTiffs\" offline mode is not set properly, i.e., either it was set to false and the required file does not exist or vice-versa. We will reset it to " + rdd_offline_exists.toString())
      rdd_offline_mode = rdd_offline_exists
    }
    if (matrix_offline_mode != matrix_offline_exists) {
      println("\"Matrix\" offline mode is not set properly, i.e., either it was set to false and the required file does not exist or vice-versa. We will reset it to " + matrix_offline_exists.toString())
      matrix_offline_mode = matrix_offline_exists
    }

    if (!fs.exists(new org.apache.hadoop.fs.Path(mask_path))) {
      println("The mask path: " + mask_path + " is invalid!!!")
    }

    //Years
    val model_years = 1980 to 2015

    if (!model_years.contains(model_first_year) || !(model_years.contains(model_last_year))) {
      println("Invalid range of years for " + geoTiff_dir + ". I should be between " + model_first_year + " and " + model_last_year)
      System.exit(0)
    }

    var model_years_range = (model_years.indexOf(model_first_year), model_years.indexOf(model_last_year))

    var num_kmeans: Int = 1
    if (minClusters != maxClusters) {
      num_kmeans = ((maxClusters - minClusters) / stepClusters) + 1
    }
    println(num_kmeans)
    var kmeans_model_paths: Array[String] = Array.fill[String](num_kmeans)("")
    var wssse_path: String = offline_dir_path + geoTiff_dir + "/" + numIterations + "_wssse"
    var geotiff_hdfs_paths: Array[String] = Array.fill[String](num_kmeans)("")
    var geotiff_tmp_paths: Array[String] = Array.fill[String](num_kmeans)("")

    if (num_kmeans > 1) {
      var numClusters_id = 0
      cfor(minClusters)(_ <= maxClusters, _ + stepClusters) { numClusters =>
        kmeans_model_paths(numClusters_id) = offline_dir_path + geoTiff_dir + "/kmeans_model_" + numClusters + "_" + numIterations

        //Check if the file exists
        val kmeans_exist = fs.exists(new org.apache.hadoop.fs.Path(kmeans_model_paths(numClusters_id)))
        if (kmeans_exist && !kmeans_offline_mode) {
          println("The kmeans model path " + kmeans_model_paths(numClusters_id) + " exists, please remove it.")
        } else if (!kmeans_exist && kmeans_offline_mode) {
          kmeans_offline_mode = false
        }

        geotiff_hdfs_paths(numClusters_id) = offline_dir_path + geoTiff_dir + "/clusters_" + numClusters + "_" + numIterations + ".tif"
        geotiff_tmp_paths(numClusters_id) = "/tmp/clusters_" + geoTiff_dir + "_" + numClusters + "_" + numIterations + ".tif"
        if (fs.exists(new org.apache.hadoop.fs.Path(geotiff_hdfs_paths(numClusters_id)))) {
          println("There is already a GeoTiff with the path: " + geotiff_hdfs_paths(numClusters_id) + ". Please make either a copy or move it to another location, otherwise, it will be over-written.")
        }
        numClusters_id += 1
      }
      kmeans_offline_mode = false
    } else {
      kmeans_model_paths(0) = offline_dir_path + geoTiff_dir + "/kmeans_model_" + minClusters + "_" + numIterations
      val kmeans_offline_exists = fs.exists(new org.apache.hadoop.fs.Path(kmeans_model_paths(0)))
      if (kmeans_offline_mode != kmeans_offline_exists) {
        println("\"Kmeans\" offline mode is not set properly, i.e., either it was set to false and the required file does not exist or vice-versa. We will reset it to " + kmeans_offline_exists.toString())
        kmeans_offline_mode = kmeans_offline_exists
      }
      geotiff_hdfs_paths(0) = offline_dir_path + geoTiff_dir + "/clusters_" + minClusters + "_" + numIterations + ".tif"
      geotiff_tmp_paths(0) = "/tmp/clusters_" + geoTiff_dir + "_" + minClusters + "_" + numIterations + ".tif"
      if (fs.exists(new org.apache.hadoop.fs.Path(geotiff_hdfs_paths(0)))) {
        println("There is already a GeoTiff with the path: " + geotiff_hdfs_paths(0) + ". Please make either a copy or move it to another location, otherwise, it will be over-written.")
      }
    }

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

    var t0 = System.nanoTime()
    //Global variables
    var projected_extent = new ProjectedExtent(new Extent(0, 0, 0, 0), CRS.fromName("EPSG:3857"))
    var grid0: RDD[(Long, Double)] = sc.emptyRDD
    var grid0_index: RDD[Long] = sc.emptyRDD
    var grids_noNaN_RDD: RDD[Array[Double]] = sc.emptyRDD
    var num_cols_rows: (Int, Int) = (0, 0)
    var cellT: CellType = UByteCellType
    var grids_RDD: RDD[Array[Double]] = sc.emptyRDD
    var mask_tile0: Tile = new SinglebandGeoTiff(geotrellis.raster.ArrayTile.empty(cellT, num_cols_rows._1, num_cols_rows._2), projected_extent.extent, projected_extent.crs, Tags.empty, GeoTiffOptions.DEFAULT).tile

    //Load Mask
    if (toBeMasked) {
      val mask_tiles_RDD = sc.hadoopGeoTiffRDD(mask_path).values
      val mask_tiles_withIndex = mask_tiles_RDD.zipWithIndex().map { case (e, v) => (v, e) }
      mask_tile0 = (mask_tiles_withIndex.filter(m => m._1 == 0).values.collect()) (0)
    }

    //Local variables
    val pattern: String = "tif"
    val filepath: String = dir_path + geoTiff_dir
    val filepath_2: String = dir_path + geoTiff_2_dir


    if (rdd_offline_mode) {
      grids_noNaN_RDD = sc.objectFile(grids_noNaN_path)
      grid0 = sc.objectFile(grid0_path)
      grid0_index = sc.objectFile(grid0_index_path)

      val metadata = sc.sequenceFile(metadata_path, classOf[IntWritable], classOf[BytesWritable]).map(_._2.copyBytes()).collect()
      projected_extent = deserialize(metadata(0)).asInstanceOf[ProjectedExtent]
      num_cols_rows = (deserialize(metadata(1)).asInstanceOf[Int], deserialize(metadata(2)).asInstanceOf[Int])
    } else {
      if (single_band) {
        //Lets load a Singleband GeoTiffs and return RDD just with the tiles.
        var tiles_1_RDD: RDD[Tile] = sc.hadoopGeoTiffRDD(filepath, pattern).values
        var tiles_2_RDD: RDD[Tile] = sc.hadoopGeoTiffRDD(filepath_2, pattern).values

        //Retrive the numbre of cols and rows of the Tile's grid
        val tiles_withIndex = tiles_1_RDD.zipWithIndex().map { case (e, v) => (v, e) }
        val tile0 = (tiles_withIndex.filter(m => m._1 == 0).values.collect()) (0)
        num_cols_rows = (tile0.cols, tile0.rows)
        cellT = tile0.cellType

        val tiles_RDD = sc.union([tiles_1_RDD, tiles_2_RDD])

        if (toBeMasked) {
          val mask_tile_broad: Broadcast[Tile] = sc.broadcast(mask_tile0)
          grids_RDD = tiles_RDD.map(m => m.localInverseMask(mask_tile_broad.value, 1, 0).toArrayDouble())
        } else {
          grids_RDD = tiles_RDD.map(m => m.toArrayDouble())
        }
      } else {
        //Lets load Multiband GeoTiffs and return RDD just with the tiles.
        val tiles_1_RDD = sc.hadoopMultibandGeoTiffRDD(filepath, pattern).values
        val tiles_2_RDD = sc.hadoopMultibandGeoTiffRDD(filepath_2, pattern).values

        //Retrive the numbre of cols and rows of the Tile's grid
        val tiles_withIndex = tiles_1_RDD.zipWithIndex().map { case (e, v) => (v, e) }
        val tile0 = (tiles_withIndex.filter(m => m._1 == 0).values.collect()) (0)
        num_cols_rows = (tile0.cols, tile0.rows)
        cellT = tile0.cellType

        val tiles_RDD = sc.union([tiles_1_RDD, tiles_2_RDD])

        //Lets read the average of the Spring-Index which is stored in the 4th band
        val band_numB: Broadcast[Int] = sc.broadcast(band_num)
        if (toBeMasked) {
          val mask_tile_broad: Broadcast[Tile] = sc.broadcast(mask_tile0)
          grids_RDD = tiles_RDD.map(m => m.band(band_numB.value).localInverseMask(mask_tile_broad.value, 1, 0).toArrayDouble().map(m => if (m == 0.0) Double.NaN else m))
        } else {
          grids_RDD = tiles_RDD.map(m => m.band(band_numB.value).toArrayDouble())
        }
      }

      //Retrieve the ProjectExtent which contains metadata such as CRS and bounding box
      val projected_extents_withIndex = sc.hadoopGeoTiffRDD(filepath, pattern).keys.zipWithIndex().map { case (e, v) => (v, e) }
      projected_extent = (projected_extents_withIndex.filter(m => m._1 == 0).values.collect()) (0)

      //Get Index for each Cell
      val grids_withIndex = grids_RDD.zipWithIndex().map { case (e, v) => (v, e) }
      grid0_index = grids_withIndex.filter(m => m._1 == 0).values.flatMap(m => m).zipWithIndex.filter(m => !m._1.isNaN).map { case (v, i) => (i) }

      //Get the Tile's grid
      grid0 = grids_withIndex.filter(m => m._1 == 0).values.flatMap(m => m).zipWithIndex.map { case (v, i) => (i, v) }

      //Lets filter out NaN
      grids_noNaN_RDD = grids_RDD.map(m => m.filter(!_.isNaN))

      //Store data in HDFS
      grid0.saveAsObjectFile(grid0_path)
      grid0_index.saveAsObjectFile(grid0_index_path)
      grids_noNaN_RDD.saveAsObjectFile(grids_noNaN_path)

      val grids_noNaN_RDD_withIndex = grids_noNaN_RDD.zipWithIndex().map { case (e, v) => (v, e) }
      grids_noNaN_RDD = grids_noNaN_RDD_withIndex.filterByRange(model_years_range._1, model_years_range._2).values

      val writer: SequenceFile.Writer = SequenceFile.createWriter(conf,
        Writer.file(metadata_path),
        Writer.keyClass(classOf[IntWritable]),
        Writer.valueClass(classOf[BytesWritable])
      )

      writer.append(new IntWritable(1), new BytesWritable(serialize(projected_extent)))
      writer.append(new IntWritable(2), new BytesWritable(serialize(num_cols_rows._1)))
      writer.append(new IntWritable(3), new BytesWritable(serialize(num_cols_rows._2)))
      writer.hflush()
      writer.close()
    }
    var t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0) + "ns")

    t0 = System.nanoTime()
    //Global variables
    var grids_matrix: RDD[Vector] = sc.emptyRDD

    if (matrix_offline_mode) {
      grids_matrix = sc.objectFile(grids_matrix_path)
    } else {
      val mat: RowMatrix = new RowMatrix(grids_noNaN_RDD.map(m => Vectors.dense(m)))

      // Split the matrix into one number per line.
      val byColumnAndRow = mat.rows.zipWithIndex.map {
        case (row, rowIndex) => row.toArray.zipWithIndex.map {
          case (number, columnIndex) => new MatrixEntry(rowIndex, columnIndex, number)
        }
      }.flatMap(x => x)

      val matt: CoordinateMatrix = new CoordinateMatrix(byColumnAndRow)
      val matt_T = matt.transpose()
      //grids_matrix = matt_T.toRowMatrix().rows
      grids_matrix = matt_T.toIndexedRowMatrix().rows.sortBy(_.index).map(_.vector)
      grids_matrix.saveAsObjectFile(grids_matrix_path)
    }
    t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0) + "ns")

    t0 = System.nanoTime()
    //Global variables
    var kmeans_models: Array[KMeansModel] = new Array[KMeansModel](num_kmeans)
    var wssse_data: List[(Int, Int, Double)] = List.empty

    if (kmeans_offline_mode) {
      var numClusters_id = 0
      cfor(minClusters)(_ <= maxClusters, _ + stepClusters) { numClusters =>
        if (!fs.exists(new org.apache.hadoop.fs.Path(kmeans_model_paths(numClusters_id)))) {
          println("One of the files does not exist, we will abort!!!")
          System.exit(0)
        } else {
          kmeans_models(numClusters_id) = KMeansModel.load(sc, kmeans_model_paths(numClusters_id))
        }
        numClusters_id += 1
      }
      val wssse_data_RDD: RDD[(Int, Int, Double)] = sc.objectFile(wssse_path)
      wssse_data = wssse_data_RDD.collect().toList
    } else {
      var numClusters_id = 0
      if (fs.exists(new org.apache.hadoop.fs.Path(wssse_path))) {
        val wssse_data_RDD: RDD[(Int, Int, Double)] = sc.objectFile(wssse_path)
        wssse_data = wssse_data_RDD.collect().toList
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
      }

      //Un-persist it to save memory
      grids_matrix.unpersist()

      if (fs.exists(new org.apache.hadoop.fs.Path(wssse_path))) {
        println("We will delete the wssse file")
        try {
          fs.delete(new org.apache.hadoop.fs.Path(wssse_path), true)
        } catch {
          case _: Throwable => {}
        }
      }

      println("Lets create it with the new data")
      sc.parallelize(wssse_data, 1).saveAsObjectFile(wssse_path)
    }
    t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0) + "ns")

    t0 = System.nanoTime()
    //current
    println(wssse_data)

    //from disk
    if (fs.exists(new org.apache.hadoop.fs.Path(wssse_path))) {
      var wssse_data_tmp: RDD[(Int, Int, Double)] = sc.objectFile(wssse_path) //.collect()//.toList
      println(wssse_data_tmp.collect().toList)
    }
    t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0) + "ns")

    t0 = System.nanoTime()
    //Cache it so kmeans is more efficient
    grids_matrix.cache()

    var kmeans_res: Array[RDD[Int]] = Array.fill(num_kmeans)(sc.emptyRDD)
    var numClusters_id = 0
    cfor(minClusters)(_ <= maxClusters, _ + stepClusters) { numClusters =>
      kmeans_res(numClusters_id) = kmeans_models(numClusters_id).predict(grids_matrix)
      numClusters_id += 1
    }

    //Un-persist it to save memory
    grids_matrix.unpersist()
    t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0) + "ns")

    t0 = System.nanoTime()
    val kmeans_res_out = kmeans_res(0).take(150)
    kmeans_res_out.foreach(print)

    println(kmeans_res_out.size)
    t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0) + "ns")

    t0 = System.nanoTime()
    numClusters_id = 0

    cfor(minClusters)(_ <= maxClusters, _ + stepClusters) { numClusters =>
      //Merge two RDDs, one containing the clusters_ID indices and the other one the indices of a Tile's grid cells
      val cluster_cell_pos = ((kmeans_res(numClusters_id).zipWithIndex().map { case (v, i) => (i, v) }).join(grid0_index.zipWithIndex().map { case (v, i) => (i, v) })).map { case (k, (v, i)) => (v, i) }

      //Associate a Cluster_IDs to respective Grid_cell
      val grid_clusters :RDD[ (Long, Option[Int])] = grid0.leftOuterJoin(cluster_cell_pos.map { case (c, i) => (i.toLong, c) })

      //Convert all None to NaN
      val grid_clusters_res = grid_clusters.sortByKey(true).map { case (k, (v, c)) => if (c == None) (k, Double.NaN) else (k, c.get.toDouble) }

      //Define a Tile
      val cluster_cells: Array[Double] = grid_clusters_res.values.collect()
      val cluster_cellsD = DoubleArrayTile(cluster_cells, num_cols_rows._1, num_cols_rows._2)
      val cluster_tile = geotrellis.raster.DoubleArrayTile.empty(num_cols_rows._1, num_cols_rows._2)
      cfor(0)(_ < num_cols_rows._1, _ + 1) { col =>
        cfor(0)(_ < num_cols_rows._2, _ + 1) { row =>
          val v = cluster_cellsD.getDouble(col, row)
          if (v != Double.NaN)
            cluster_tile.setDouble(col, row, v)
        }
      }

      val geoTif = new SinglebandGeoTiff(cluster_tile, projected_extent.extent, projected_extent.crs, Tags.empty, GeoTiffOptions(compression.DeflateCompression))

      //Save to /tmp/
      GeoTiffWriter.write(geoTif, geotiff_tmp_paths(numClusters_id))

      //Upload to HDFS
      var cmd = "hadoop dfs -copyFromLocal -f " + geotiff_tmp_paths(numClusters_id) + " " + geotiff_hdfs_paths(numClusters_id)
      Process(cmd) !

      //Remove from /tmp/
      cmd = "rm -fr " + geotiff_tmp_paths(numClusters_id)
      Process(cmd) !

      numClusters_id += 1
    }
    t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0) + "ns")
  }
}
