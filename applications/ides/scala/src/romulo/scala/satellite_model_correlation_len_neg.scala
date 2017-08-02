import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}

import breeze.linalg.sum
import breeze.stats._
import geotrellis.proj4.CRS
import geotrellis.raster.io.geotiff.writer.GeoTiffWriter
import geotrellis.raster.io.geotiff.{SinglebandGeoTiff, _}
import geotrellis.raster.{CellType, DoubleArrayTile, Tile, UByteCellType}
import geotrellis.spark.io.hadoop._
import geotrellis.vector.{Extent, ProjectedExtent}
import org.apache.hadoop.io.SequenceFile.Writer
import org.apache.hadoop.io.{SequenceFile, _}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry, RowMatrix}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext, mllib}
import com.quantifind.charts.Highcharts._

import scala.sys.process._

object satellite_model_correlation_len_neg extends App {

  override def main(args: Array[String]): Unit = {
    val appName = this.getClass.getName
    val masterURL = "spark://emma0.emma.nlesc.nl:7077"
    val sc = new SparkContext(new SparkConf().setAppName(appName).setMaster(masterURL))

    var model_rdd_offline_mode = true
    var model_matrix_offline_mode = true
    var satellite_rdd_offline_mode = true
    var satellite_matrix_offline_mode = true

    //Using spring-index model
    var model_path = "hdfs:///user/hadoop/spring-index/"
    var model_dir = "BloomFinal"

    //Using AVHRR Satellite data
    var satellite_path = "hdfs:///user/hadoop/spring-index/"
    var satellite_dir = "LeafFinal"

    var out_path = "hdfs:///user/pheno/correlation/"
    var band_num = 3

    //Years between (inclusive) 1989 - 2014
    var satellite_first_year = 1989
    var satellite_last_year = 2014

    //Years between (inclusive) 1980 - 2015
    var model_first_year = 1988
    var model_last_year = 2014

    //Mask
    val toBeMasked = true
    val mask_path = "hdfs:///user/hadoop/usa_mask.tif"

    val save_rdds = true
    val save_matrix = true

    //Check offline modes
    var conf = sc.hadoopConfiguration
    var fs = org.apache.hadoop.fs.FileSystem.get(conf)

    //Paths to store data structures for Offline runs
    var mask_str = ""
    if (toBeMasked)
      mask_str = "_mask"
    var model_grid0_path = out_path + model_dir + "_grid0"
    var model_grid0_index_path = out_path + model_dir + "_grid0_index"

    var model_grid_path = out_path + model_dir + "_grid"
    var satellite_grid_path = out_path + satellite_dir + "_grid"
    var model_matrix_path = out_path + model_dir + "_matrix"
    var satellite_matrix_path = out_path + satellite_dir + "_matrix"
    var metadata_path = out_path + model_dir + "_metadata"

    val model_rdd_offline_exists = fs.exists(new org.apache.hadoop.fs.Path(model_grid_path))
    val model_matrix_offline_exists = fs.exists(new org.apache.hadoop.fs.Path(model_matrix_path))
    val satellite_rdd_offline_exists = fs.exists(new org.apache.hadoop.fs.Path(satellite_grid_path))
    val satellite_matrix_offline_exists = fs.exists(new org.apache.hadoop.fs.Path(satellite_matrix_path))

    if (model_rdd_offline_mode != model_rdd_offline_exists) {
      println("\"Load GeoTiffs\" offline mode is not set properly, i.e., either it was set to false and the required file does not exist or vice-versa. We will reset it to " + model_rdd_offline_exists.toString())
      model_rdd_offline_mode = model_rdd_offline_exists
    }

    if (model_matrix_offline_mode != model_matrix_offline_exists) {
      println("\"Matrix\" offline mode is not set properly, i.e., either it was set to false and the required file does not exist or vice-versa. We will reset it to " + model_matrix_offline_exists.toString())
      model_matrix_offline_mode = model_matrix_offline_exists
    }

    var model_skip_rdd = false
    if (model_matrix_offline_exists) {
      println("Since we have a matrix, the load of the grids RDD will be skipped!!!")
      model_skip_rdd = true
    }

    if (satellite_rdd_offline_mode != satellite_rdd_offline_exists) {
      println("\"Load GeoTiffs\" offline mode is not set properly, i.e., either it was set to false and the required file does not exist or vice-versa. We will reset it to " + satellite_rdd_offline_exists.toString())
      satellite_rdd_offline_mode = satellite_rdd_offline_exists
    }

    if (satellite_matrix_offline_mode != satellite_matrix_offline_exists) {
      println("\"Matrix\" offline mode is not set properly, i.e., either it was set to false and the required file does not exist or vice-versa. We will reset it to " + satellite_matrix_offline_exists.toString())
      satellite_matrix_offline_mode = satellite_matrix_offline_exists
    }

    var satellite_skip_rdd = false
    if (satellite_matrix_offline_exists) {
      println("Since we have a matrix, the load of the grids RDD will be skipped!!!")
      satellite_skip_rdd = true
    }

    var corr_tif = out_path + satellite_dir + "_" + model_dir + ".tif"
    var corr_tif_tmp = "/tmp/correlation_" + satellite_dir + "_" + model_dir + ".tif"

    //Years
    val satellite_years = 1989 to 2014
    val model_years = 1980 to 2015

    if (!satellite_years.contains(satellite_first_year) || !(satellite_years.contains(satellite_last_year))) {
      println("Invalid range of years for " + satellite_dir + ". I should be between " + satellite_first_year + " and " + satellite_last_year)
      System.exit(0)
    }

    if (!model_years.contains(model_first_year) || !(model_years.contains(model_last_year))) {
      println("Invalid range of years for " + model_dir + ". I should be between " + model_first_year + " and " + model_last_year)
      System.exit(0)
    }

    if (((satellite_last_year - model_first_year) > (model_last_year - model_first_year)) || ((satellite_last_year - model_first_year) > (model_last_year - model_first_year))) {
      println("The range of years for each data set should be of the same length.");
      System.exit(0)
    }

    var model_years_range = (model_years.indexOf(model_first_year), model_years.indexOf(model_last_year))
    var satellite_years_range = (satellite_years.indexOf(satellite_first_year), satellite_years.indexOf(satellite_last_year))

    //Global variables
    var projected_extent = new ProjectedExtent(new Extent(0, 0, 0, 0), CRS.fromName("EPSG:3857"))
    var model_grid0: RDD[(Long, Double)] = sc.emptyRDD
    var model_grid0_index: RDD[Long] = sc.emptyRDD
    var grids_RDD: RDD[Array[Double]] = sc.emptyRDD
    var model_grids_RDD: RDD[Array[Double]] = sc.emptyRDD
    var model_grids: RDD[Array[Double]] = sc.emptyRDD
    var satellite_grids_RDD: RDD[Array[Double]] = sc.emptyRDD
    var satellite_grids: RDD[Array[Double]] = sc.emptyRDD
    var num_cols_rows: (Int, Int) = (0, 0)
    var cellT: CellType = UByteCellType
    var mask_tile0: Tile = new SinglebandGeoTiff(geotrellis.raster.ArrayTile.empty(cellT, num_cols_rows._1, num_cols_rows._2), projected_extent.extent, projected_extent.crs, Tags.empty, GeoTiffOptions.DEFAULT).tile
    var satellite_cells_size: Long = 0
    var model_cells_size: Long = 0
    var t0: Long = 0
    var t1: Long = 0

    satellite_matrix_path

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

    t0 = System.nanoTime()

    //Load Mask
    if (!(model_skip_rdd && satellite_skip_rdd) && toBeMasked) {
      val mask_tiles_RDD = sc.hadoopGeoTiffRDD(mask_path).values
      val mask_tiles_withIndex = mask_tiles_RDD.zipWithIndex().map { case (e, v) => (v, e) }
      mask_tile0 = (mask_tiles_withIndex.filter(m => m._1 == 0).filter(m => !m._1.isNaN).values.collect()) (0)
    }

    //Local variables
    val pattern: String = "tif"
    val satellite_filepath: String = satellite_path + satellite_dir
    val model_filepath: String = model_path + "/" + model_dir

    t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0) + "ns")

    mask_tile0.size

    t0 = System.nanoTime()
    if (!satellite_skip_rdd) {
      if (satellite_rdd_offline_mode) {
        satellite_grids_RDD = sc.objectFile(satellite_grid_path)
      } else {
        //Lets load MODIS Singleband GeoTiffs and return RDD just with the tiles.
        val satellite_geos_RDD = sc.hadoopGeoTiffRDD(satellite_filepath, pattern)
        val satellite_tiles_RDD = satellite_geos_RDD.values

        if (toBeMasked) {
          val mask_tile_broad: Broadcast[Tile] = sc.broadcast(mask_tile0)
          satellite_grids_RDD = satellite_tiles_RDD.map(m => m.localInverseMask(mask_tile_broad.value, 1, -1000).toArrayDouble().filter(_ != -1000))
        } else {
          satellite_grids_RDD = satellite_tiles_RDD.map(m => m.toArrayDouble())
        }

        //Store in HDFS
        if (save_rdds) {
          satellite_grids_RDD.saveAsObjectFile(satellite_grid_path)
        }
      }
      val satellite_grids_withIndex = satellite_grids_RDD.zipWithIndex().map { case (e, v) => (v, e) }

      //Filter out the range of years:
      satellite_grids = satellite_grids_withIndex.filterByRange(satellite_years_range._1, satellite_years_range._2).values

      var satellite_grid0_index: RDD[Double] = satellite_grids_withIndex.filter(m => m._1 == 0).values.flatMap(m => m)
      satellite_cells_size = satellite_grid0_index.count().toInt
      println("Number of cells is: " + satellite_cells_size)
    }
    t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0) + "ns")

    mask_tile0.size

    t0 = System.nanoTime()
    if (model_rdd_offline_mode) {
      model_grids_RDD = sc.objectFile(model_grid_path)
      model_grid0 = sc.objectFile(model_grid0_path)
      model_grid0_index = sc.objectFile(model_grid0_index_path)

      val metadata = sc.sequenceFile(metadata_path, classOf[IntWritable], classOf[BytesWritable]).map(_._2.copyBytes()).collect()
      projected_extent = deserialize(metadata(0)).asInstanceOf[ProjectedExtent]
      num_cols_rows = (deserialize(metadata(1)).asInstanceOf[Int], deserialize(metadata(2)).asInstanceOf[Int])
      cellT = deserialize(metadata(3)).asInstanceOf[CellType]
    } else {
      val model_geos_RDD = sc.hadoopMultibandGeoTiffRDD(model_filepath, pattern)
      val model_tiles_RDD = model_geos_RDD.values

      //Retrieve the number of cols and rows of the Tile's grid
      val tiles_withIndex = model_tiles_RDD.zipWithIndex().map { case (v, i) => (i, v) }
      val tile0 = (tiles_withIndex.filter(m => m._1 == 0).values.collect()) (0)

      num_cols_rows = (tile0.cols, tile0.rows)
      cellT = tile0.cellType

      //Retrieve the ProjectExtent which contains metadata such as CRS and bounding box
      val projected_extents_withIndex = model_geos_RDD.keys.zipWithIndex().map { case (e, v) => (v, e) }
      projected_extent = (projected_extents_withIndex.filter(m => m._1 == 0).values.collect()) (0)

      val band_numB: Broadcast[Int] = sc.broadcast(band_num)
      if (toBeMasked) {
        val mask_tile_broad: Broadcast[Tile] = sc.broadcast(mask_tile0)
        grids_RDD = model_tiles_RDD.map(m => m.band(band_numB.value).localInverseMask(mask_tile_broad.value, 1, -1000).toArrayDouble())
      } else {
        grids_RDD = model_tiles_RDD.map(m => m.band(band_numB.value).toArrayDouble())
      }

      //Get Index for each Cell
      val grids_withIndex = grids_RDD.zipWithIndex().map { case (e, v) => (v, e) }
      if (toBeMasked) {
        model_grid0_index = grids_withIndex.filter(m => m._1 == 0).values.flatMap(m => m).zipWithIndex.filter(m => m._1 != -1000.0).map { case (v, i) => (i) }
      } else {
        model_grid0_index = grids_withIndex.filter(m => m._1 == 0).values.flatMap(m => m).zipWithIndex.map { case (v, i) => (i) }
      }

      //Get the Tile's grid
      model_grid0 = grids_withIndex.filter(m => m._1 == 0).values.flatMap(m => m).zipWithIndex.map { case (v, i) => (i, v) }

      //Lets filter out NaN
      if (toBeMasked) {
        model_grids_RDD = grids_RDD.map(m => m.filter(m => m != -1000.0))
      } else {
        model_grids_RDD = grids_RDD
      }

      //Store data in HDFS
      if (save_rdds) {
        model_grid0.saveAsObjectFile(model_grid0_path)
        model_grid0_index.saveAsObjectFile(model_grid0_index_path)
        model_grids_RDD.saveAsObjectFile(model_grid_path)

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
    val model_grids_withIndex = model_grids_RDD.zipWithIndex().map { case (e, v) => (v, e) }

    //Filter out the range of years:
    model_grids = model_grids_withIndex.filterByRange(model_years_range._1, model_years_range._2).values

    var model_tile0_index: RDD[Double] = model_grids_withIndex.filter(m => m._1 == 0).values.flatMap(m => m)
    model_cells_size = model_tile0_index.count().toInt
    println("Number of cells is: " + model_cells_size)

    t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0) + "ns")

    t0 = System.nanoTime()
    //Global variables
    var satellite_matrix: RDD[mllib.linalg.Vector] = sc.emptyRDD
    val satellite_cells_sizeB = sc.broadcast(satellite_cells_size)
    if (satellite_matrix_offline_mode) {
      satellite_matrix = sc.objectFile(satellite_matrix_path)
    } else {
      val mat: RowMatrix = new RowMatrix(satellite_grids_RDD.map(m => m.zipWithIndex).map(m => m.filter(!_._1.isNaN)).map(m => Vectors.sparse(satellite_cells_sizeB.value.toInt, m.map(v => v._2), m.map(v => v._1))))
      // Split the matrix into one number per line.
      val byColumnAndRow = mat.rows.zipWithIndex.map {
        case (row, rowIndex) => row.toArray.zipWithIndex.map {
          case (number, columnIndex) => new MatrixEntry(rowIndex, columnIndex, number)
        }
      }.flatMap(x => x)

      val matt: CoordinateMatrix = new CoordinateMatrix(byColumnAndRow)
      val matt_T = matt.transpose()

      satellite_matrix = matt_T.toIndexedRowMatrix().rows.sortBy(_.index).map(_.vector)
      if (save_matrix) {
        satellite_matrix.saveAsObjectFile(satellite_matrix_path)
      }
    }

    t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0) + "ns")

    t0 = System.nanoTime()

    //Global variables
    var model_matrix: RDD[mllib.linalg.Vector] = sc.emptyRDD

    val model_cells_sizeB = sc.broadcast(model_cells_size)
    if (model_matrix_offline_mode) {
      model_matrix = sc.objectFile(model_matrix_path)
    } else {
      val mat: RowMatrix = new RowMatrix(model_grids.map(m => m.zipWithIndex).map(m => m.filter(!_._1.isNaN)).map(m => Vectors.sparse(model_cells_sizeB.value.toInt, m.map(v => v._2), m.map(v => v._1))))
      // Split the matrix into one number per line.
      val byColumnAndRow = mat.rows.zipWithIndex.map {
        case (row, rowIndex) => row.toArray.zipWithIndex.map {
          case (number, columnIndex) => new MatrixEntry(rowIndex, columnIndex, number)
        }
      }.flatMap(x => x)

      val matt: CoordinateMatrix = new CoordinateMatrix(byColumnAndRow)
      val matt_T = matt.transpose()

      model_matrix = matt_T.toIndexedRowMatrix().rows.sortBy(_.index).map(_.vector)

      if (save_matrix) {
        model_matrix.saveAsObjectFile(model_matrix_path)
      }
    }

    t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0) + "ns")

    var model_matrix_rows = model_matrix.count()
    var satellite_matrix_rows = satellite_matrix.count()

    if (model_matrix_rows != satellite_matrix_rows) {
      println("For correlation it is necessary to have a matrix with same number of rows and columns!!!")
      println("Model matrix has " + model_matrix_rows + " rows while satellite matrix has " + satellite_matrix_rows + " rows!!!")
      //System.exit(0)
    }

    val satellite: RDD[(Long, Array[Double])] = satellite_matrix.zipWithIndex().map { case (v, i) => (i, v.toArray) }
    val model: RDD[(Long, Array[Double])] = model_matrix.zipWithIndex().map { case (v, i) => (i, v.toArray) }
    val satellite_model = satellite.join(model)
    val satellite_model_V = satellite_model.map { case (a, (b, c)) => (a, (new breeze.linalg.DenseVector[Double](b), new breeze.linalg.DenseVector[Double](c))) }


    var satellite_yearsB = sc.broadcast(satellite_years)
    val satellite_model_corr = satellite_model_V.map { case (i, (b, a)) => (i, {
      val isLeapYear = (year: Int) => (((year % 4) == 0) && !(
        ((year % 100) == 0) &&
          !((year % 400) == 0))
        )
      //a is model, b is satellite (model has an extra day)
      var n = a.length
      val aC: Array[Double] = new Array[Double](n * 2)
      val bC = new Array[Double](n * 2)

      for (i <- 0 until (n * 2)) {
        aC(i) = Double.NaN
        bC(i) = Double.NaN
      }

      var k = 0
      for (i <- 0 until (n)) {
        aC(k) = a(i)
        aC(k + 1) = a(i)
        if (i > 0)
          bC(k) = b(i - 1)
        k += 2
      }

      //Let's shift the negative days
      var neg_cnt = 0
      for (i <- 1 until (n)) {
        if (bC(i * 2) < 0) {
          neg_cnt += 1
          if (isLeapYear(satellite_yearsB.value(i - 1))) {
            bC((i * 2) - 1) = 367 - bC(i * 2)
          } else {
            bC((i * 2) - 1) = 366 - bC(i * 2)
          }
          bC(i * 2) = Double.NaN
        } //else { //American system starts countin at 0 days
        //  bC(i*2) = bC(i*2) + 1
        //}
      }

      for (i <- 0 until (n * 2)) {
        if (bC(i).isNaN)
          aC(i) = Double.NaN
      }

      val aC_NoNAN = new breeze.linalg.DenseVector[Double](aC.filter(!_.isNaN))
      val bC_NoNAN = new breeze.linalg.DenseVector[Double](bC.filter(!_.isNaN))

      if (aC_NoNAN.length > bC_NoNAN.length) {
        assert(false)
      }

      //Correlation
      n = aC_NoNAN.length
      val ameanavar = meanAndVariance(aC_NoNAN)
      val amean = ameanavar.mean
      val avar = ameanavar.variance
      val bmeanbvar = meanAndVariance(bC_NoNAN)
      val bmean = bmeanbvar.mean
      val bvar = bmeanbvar.variance
      val astddev = math.sqrt(avar)
      val bstddev = math.sqrt(bvar)

      ((1.0 / (n - 1.0) * sum(((aC_NoNAN - amean) / astddev) :* ((bC_NoNAN - bmean) / bstddev))), n, neg_cnt)
    })
    }.sortByKey()

    val corr_res_b = satellite_model_corr.map { case (a, (b, c, d)) => (a, b) }
    //Merge two RDDs, one containing the clusters_ID indices and the other one the indices of a Tile's grid cells
    var corr_cell_pos = corr_res_b.join(model_grid0_index.zipWithIndex().map { case (v, i) => (i, v) }).map { case (k, (v, i)) => (v, i) }

    //Associate a Cluster_IDs to respective Grid_cell
    var grid_corr :RDD[ (Long, (Double, Option[Double]))] = model_grid0.map { case (i, v) => if (v == -1000) (i, Double.NaN) else (i, v) }.leftOuterJoin(corr_cell_pos.map { case (c, i) => (i.toLong, c) })

    //Convert all None to NaN
    var grid_corr_res = grid_corr.sortByKey(true).map { case (k, (v, c)) => if (c == None) (k, Double.NaN) else (k, c.get) }

    //Define a Tile
    var corr_cells: Array[Double] = grid_corr_res.values.collect()
    var corr_cellsD = DoubleArrayTile(corr_cells, num_cols_rows._1, num_cols_rows._2)

    var geoTif = new SinglebandGeoTiff(corr_cellsD, projected_extent.extent, projected_extent.crs, Tags.empty, GeoTiffOptions(compression.DeflateCompression))

    //Save to /tmp/
    GeoTiffWriter.write(geoTif, corr_tif_tmp)

    //Upload to HDFS

    corr_tif = out_path + satellite_dir + "_" + model_dir + ".tif"
    var cmd = "hadoop dfs -copyFromLocal -f " + corr_tif_tmp + " " + corr_tif
    Process(cmd) !

    //Remove from /tmp/
    cmd = "rm -fr " + corr_tif_tmp
    Process(cmd) !

    val corr_res_c = satellite_model_corr.map { case (a, (b, c, d)) => (a, c) }
    //Merge two RDDs, one containing the clusters_ID indices and the other one the indices of a Tile's grid cells
    corr_cell_pos = corr_res_c.join(model_grid0_index.zipWithIndex().map { case (v, i) => (i, v) }).map { case (k, (v, i)) => (v, i) }

    //Associate a Cluster_IDs to respective Grid_cell
    grid_corr = model_grid0.map { case (i, v) => if (v == -1000) (i, Double.NaN) else (i, v) }.leftOuterJoin(corr_cell_pos.map { case (c, i) => (i.toLong, c) })

    //Convert all None to NaN
    grid_corr_res = grid_corr.sortByKey(true).map { case (k, (v, c)) => if (c == None) (k, Double.NaN) else (k, c.get) }

    //Define a Tile
    corr_cells = grid_corr_res.values.collect()
    corr_cellsD = DoubleArrayTile(corr_cells, num_cols_rows._1, num_cols_rows._2)

    geoTif = new SinglebandGeoTiff(corr_cellsD, projected_extent.extent, projected_extent.crs, Tags.empty, GeoTiffOptions(compression.DeflateCompression))

    //Save to /tmp/
    GeoTiffWriter.write(geoTif, corr_tif_tmp)

    //Upload to HDFS
    corr_tif = out_path + "len_" + satellite_dir + "_" + model_dir + ".tif"
    cmd = "hadoop dfs -copyFromLocal -f " + corr_tif_tmp + " " + corr_tif
    Process(cmd) !

    //Remove from /tmp/
    cmd = "rm -fr " + corr_tif_tmp
    Process(cmd) !

    val corr_res_d = satellite_model_corr.map { case (a, (b, c, d)) => (a, d) }
    //Merge two RDDs, one containing the clusters_ID indices and the other one the indices of a Tile's grid cells
    corr_cell_pos = corr_res_d.join(model_grid0_index.zipWithIndex().map { case (v, i) => (i, v) }).map { case (k, (v, i)) => (v, i) }

    //Associate a Cluster_IDs to respective Grid_cell
    grid_corr = model_grid0.map { case (i, v) => if (v == -1000) (i, Double.NaN) else (i, v) }.leftOuterJoin(corr_cell_pos.map { case (c, i) => (i.toLong, c) })

    //Convert all None to NaN
    grid_corr_res = grid_corr.sortByKey(true).map { case (k, (v, c)) => if (c == None) (k, Double.NaN) else (k, c.get.toDouble) }

    //Define a Tile
    corr_cells = grid_corr_res.values.collect()
    corr_cellsD = DoubleArrayTile(corr_cells, num_cols_rows._1, num_cols_rows._2)

    geoTif = new SinglebandGeoTiff(corr_cellsD, projected_extent.extent, projected_extent.crs, Tags.empty, GeoTiffOptions(compression.DeflateCompression))

    //Save to /tmp/
    corr_tif = out_path + "neg_" + satellite_dir + "_" + model_dir + ".tif"
    GeoTiffWriter.write(geoTif, corr_tif_tmp)

    //Upload to HDFS
    cmd = "hadoop dfs -copyFromLocal -f " + corr_tif_tmp + " " + corr_tif
    Process(cmd) !

    //Remove from /tmp/
    cmd = "rm -fr " + corr_tif_tmp
    Process(cmd) !

    satellite_yearsB = sc.broadcast(satellite_years)
    val satellite_model_arrays = satellite_model_V.map { case (i, (b, a)) => (i, {
      val isLeapYear = (year: Int) => (((year % 4) == 0) && !(
        ((year % 100) == 0) &&
          !((year % 400) == 0))
        )
      //a is model, b is satellite (model has an extra day)
      var n = a.length
      val aC: Array[Double] = new Array[Double](n * 2)
      val bC = new Array[Double](n * 2)

      for (i <- 0 until (n * 2)) {
        aC(i) = Double.NaN
        bC(i) = Double.NaN
      }

      var k = 0
      for (i <- 0 until (n)) {
        aC(k) = a(i)
        aC(k + 1) = a(i)
        if (i > 0)
          bC(k) = b(i - 1)
        k += 2
      }

      //Let's shift the negative days
      var neg_cnt = 0
      for (i <- 1 until (n)) {
        if (bC(i * 2) < 0) {
          neg_cnt += 1
          if (isLeapYear(satellite_yearsB.value(i - 1))) {
            bC((i * 2) - 1) = 367 - bC(i * 2)
          } else {
            bC((i * 2) - 1) = 366 - bC(i * 2)
          }
          bC(i * 2) = Double.NaN
        } else {
          bC(i * 2) = bC(i * 2) + 1
        }
      }

      for (i <- 0 until (n * 2)) {
        if (bC(i).isNaN)
          aC(i) = Double.NaN
      }

      val aC_NoNAN = new breeze.linalg.DenseVector[Double](aC.filter(!_.isNaN))
      val bC_NoNAN = new breeze.linalg.DenseVector[Double](bC.filter(!_.isNaN))

      (bC_NoNAN, aC_NoNAN)
    })
    }.sortByKey()

    val index_id_gridID = model_grid0_index.zipWithIndex().map { case (v, i) => (i, v) }

    var cell_ID: Int = 0
    var id_cell: Long = 0
    var xs_ys: (Seq[Double], Seq[Double]) = (Seq.empty, Seq.empty)

    cell_ID = 1994166
    //cell_ID = 6789976

    val cell_IDB = sc.broadcast(cell_ID)
    id_cell = index_id_gridID.filter(_._2 == cell_IDB.value).map { case (i, v) => i }.collect()(0)
    val id_cellB = sc.broadcast(id_cell)
    xs_ys = satellite_model_arrays.filter(_._1 == id_cellB.value).map { case (i, (b, a)) => (b.toArray.toSeq, a.toArray.toSeq) }.collect()(0)

    //cell_ID = 1994166
    //cell_ID = 11150671
    cell_ID = 3203272
    val cell_IDB1 = sc.broadcast(cell_ID)
    id_cell = index_id_gridID.filter(_._2 == cell_IDB1.value).map { case (i, v) => i }.collect()(0)
    val id_cellB1 = sc.broadcast(id_cell)
    xs_ys = satellite_model_arrays.filter(_._1 == id_cellB1.value).map { case (i, (b, a)) => (b.toArray.toSeq, a.toArray.toSeq) }.collect()(0)

    startServer
    deleteAll()
    scatter(xs_ys._1, xs_ys._2)

    deleteAll()
    stopWispServer
    stopServer
  }
}

