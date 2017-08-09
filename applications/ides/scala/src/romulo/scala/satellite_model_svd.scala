
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
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.linalg.distributed._
import org.apache.spark.mllib.stat.{MultivariateStatisticalSummary, Statistics}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

import scala.sys.process._

object satellite_model_svd extends App {

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
    var satellite_path = "hdfs:///user/hadoop/avhrr/"
    var satellite_dir = "SOST"

    var out_path = "hdfs:///user/pheno/svd/"
    var band_num = 3

    //Years between (inclusive) 1989 - 2014
    var satellite_first_year = 1989
    var satellite_last_year = 2014

    //Years between (inclusive) 1980 - 2015
    var model_first_year = 1989
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

    var sc_path = out_path + model_dir + "_sc"
    var mc_path = out_path + model_dir + "_mc"

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

    if (satellite_rdd_offline_mode != satellite_rdd_offline_exists) {
      println("\"Load GeoTiffs\" offline mode is not set properly, i.e., either it was set to false and the required file does not exist or vice-versa. We will reset it to " + satellite_rdd_offline_exists.toString())
      satellite_rdd_offline_mode = satellite_rdd_offline_exists
    }

    if (satellite_matrix_offline_mode != satellite_matrix_offline_exists) {
      println("\"Matrix\" offline mode is not set properly, i.e., either it was set to false and the required file does not exist or vice-versa. We will reset it to " + satellite_matrix_offline_exists.toString())
      satellite_matrix_offline_mode = satellite_matrix_offline_exists
    }

    var corr_tif = out_path + "_" + satellite_dir + "_" + model_dir + ".tif"
    var corr_tif_tmp = "/tmp/svd_" + satellite_dir + "_" + model_dir + ".tif"

    //Years
    val model_years = 1980 to 2015
    val satellite_years = 1989 to 2014

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
    val rows = sc.parallelize(Array[Double]()).map(m => Vectors.dense(m))
    var model_summary: MultivariateStatisticalSummary = new RowMatrix(rows).computeColumnSummaryStatistics()
    var model_std: Array[Double] = new Array[Double](0)

    var satellite_grids_RDD: RDD[Array[Double]] = sc.emptyRDD
    var satellite_grids: RDD[Array[Double]] = sc.emptyRDD
    var satellite_summary: MultivariateStatisticalSummary = new RowMatrix(rows).computeColumnSummaryStatistics()
    var satellite_std: Array[Double] = new Array[Double](0)

    var num_cols_rows: (Int, Int) = (0, 0)
    var cellT: CellType = UByteCellType
    var mask_tile0: Tile = new SinglebandGeoTiff(geotrellis.raster.ArrayTile.empty(cellT, num_cols_rows._1, num_cols_rows._2), projected_extent.extent, projected_extent.crs, Tags.empty, GeoTiffOptions.DEFAULT).tile
    var satellite_cells_size: Long = 0
    var model_cells_size: Long = 0
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

    t0 = System.nanoTime()

    //Load Mask
    if (toBeMasked) {
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

    t0 = System.nanoTime()
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
    satellite_grids.persist(StorageLevel.DISK_ONLY)

    //Collect Stats:
    satellite_summary = Statistics.colStats(satellite_grids.map(m => Vectors.dense(m)))
    satellite_std = satellite_summary.variance.toArray.map(m => scala.math.sqrt(m))

    var satellite_grid0_index: RDD[Double] = satellite_grids_withIndex.filter(m => m._1 == 0).values.flatMap(m => m)
    satellite_cells_size = satellite_grid0_index.count().toInt
    println("Number of cells is: " + satellite_cells_size)
    t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0) + "ns")

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
      model_grids_RDD.saveAsObjectFile(model_grid_path)
      model_grid0.saveAsObjectFile(model_grid0_path)
      model_grid0_index.saveAsObjectFile(model_grid0_index_path)

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
    val model_grids_withIndex = model_grids_RDD.zipWithIndex().map { case (e, v) => (v, e) }

    //Filter out the range of years:
    model_grids = model_grids_withIndex.filterByRange(model_years_range._1, model_years_range._2).values
    model_grids.persist()

    //Collect Stats:
    model_summary = Statistics.colStats(model_grids.map(m => Vectors.dense(m)))
    //model_std = model_summary.variance.toArray.map(m => scala.math.sqrt(m))

    var model_tile0_index: RDD[Double] = model_grids_withIndex.filter(m => m._1 == 0).values.flatMap(m => m)
    model_cells_size = model_tile0_index.count().toInt

    t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0) + "ns")

    //Satellite
    val satellite_cells_sizeB = sc.broadcast(num_cols_rows._2)
    val satellite_mat: RowMatrix = new RowMatrix(satellite_grids.map(m => m.zipWithIndex).map(m => m.filter(!_._1.isNaN)).map(m => Vectors.sparse(satellite_cells_sizeB.value.toInt, m.map(v => v._2), m.map(v => v._1))))

    // Split the matrix into one number per line.
    val sat_byColumnAndRow = satellite_mat.rows.zipWithIndex.map {
      case (row, rowIndex) => row.toArray.zipWithIndex.map {
        case (number, columnIndex) => new MatrixEntry(rowIndex, columnIndex, number)
      }
    }.flatMap(x => x)
    val satellite_blockMatrix: BlockMatrix = new CoordinateMatrix(sat_byColumnAndRow).toBlockMatrix()

    //SC
    val sc_exists = fs.exists(new org.apache.hadoop.fs.Path(sc_path))
    var Sc :BlockMatrix = null
    if (sc_exists) {
      val rdd_indexed_rows :RDD[IndexedRow]= sc.objectFile(sc_path)
      Sc = new IndexedRowMatrix(rdd_indexed_rows).toBlockMatrix()
    } else {
      val satellite_M_1_Gc = sc.parallelize(Array[Vector](satellite_summary.mean)).map(m => Vectors.dense(m.toArray))
      val satellite_M_1_Gc_RowM: RowMatrix = new RowMatrix(satellite_M_1_Gc)
      val sat_M_1_Gc_byColumnAndRow = satellite_M_1_Gc_RowM.rows.zipWithIndex.map {
        case (row, rowIndex) => row.toArray.zipWithIndex.map {
          case (number, columnIndex) => new MatrixEntry(rowIndex, columnIndex, number)
        }
      }.flatMap(x => x)
      val satellite_M_1_Gc_blockMatrix = new CoordinateMatrix(sat_M_1_Gc_byColumnAndRow).toBlockMatrix()

      val sat_matrix_Nt_1 = new Array[Double](satellite_grids.count().toInt)
      satellite_grids.unpersist(false)
      for (i <- 0 until sat_matrix_Nt_1.length)
        sat_matrix_Nt_1(i) = 1
      val satellite_M_Nt_1 = sc.parallelize(sat_matrix_Nt_1).map(m => Vectors.dense(m))
      val satellite_M_Nt_1_RowM: RowMatrix = new RowMatrix(satellite_M_Nt_1)
      val sat_M_Nt_1_byColumnAndRow = satellite_M_Nt_1_RowM.rows.zipWithIndex.map {
        case (row, rowIndex) => row.toArray.zipWithIndex.map {
          case (number, columnIndex) => new MatrixEntry(rowIndex, columnIndex, number)
        }
      }.flatMap(x => x)
      val satellite_M_Nt_1_blockMatrix = new CoordinateMatrix(sat_M_Nt_1_byColumnAndRow).toBlockMatrix()
      val satellite_M_Nt_Gc_blockMatrix = satellite_M_Nt_1_blockMatrix.multiply(satellite_M_1_Gc_blockMatrix)

      Sc = satellite_blockMatrix.subtract(satellite_M_Nt_Gc_blockMatrix)

      //save to disk
      Sc.toIndexedRowMatrix().rows.saveAsObjectFile(sc_path)
    }
    Sc.persist(StorageLevel.MEMORY_AND_DISK)


    //Model
    val model_cells_sizeB = sc.broadcast(model_cells_size)
    val model_mat: RowMatrix = new RowMatrix(model_grids.map(m => m.zipWithIndex).map(m => m.filter(!_._1.isNaN)).map(m => Vectors.sparse(model_cells_sizeB.value.toInt, m.map(v => v._2), m.map(v => v._1))))

    // Split the matrix into one number per line.
    val mod_byColumnAndRow = model_mat.rows.zipWithIndex.map {
      case (row, rowIndex) => row.toArray.zipWithIndex.map {
        case (number, columnIndex) => new MatrixEntry(rowIndex, columnIndex, number)
      }
    }.flatMap(x => x)
    val model_blockMatrix: BlockMatrix = new CoordinateMatrix(mod_byColumnAndRow).transpose().toBlockMatrix()

    //MC
    val mc_exists = fs.exists(new org.apache.hadoop.fs.Path(mc_path))
    var Mc :BlockMatrix = null
    if (mc_exists) {
      val rdd_indexed_rows :RDD[IndexedRow]= sc.objectFile(mc_path)
      Mc = new IndexedRowMatrix(rdd_indexed_rows).toBlockMatrix()
    } else {
      val model_M_1_Gc = sc.parallelize(Array[Vector](model_summary.mean)).map(m => Vectors.dense(m.toArray))
      val model_M_1_Gc_RowM: RowMatrix = new RowMatrix(model_M_1_Gc)
      val mod_M_1_Gc_byColumnAndRow = model_M_1_Gc_RowM.rows.zipWithIndex.map {
        case (row, rowIndex) => row.toArray.zipWithIndex.map {
          case (number, columnIndex) => new MatrixEntry(rowIndex, columnIndex, number)
        }
      }.flatMap(x => x)
      val model_M_1_Gc_blockMatrix = new CoordinateMatrix(mod_M_1_Gc_byColumnAndRow).toBlockMatrix()

      val model_matrix_Nt_1 = new Array[Double](model_grids.count().toInt)
      model_grids.unpersist(false)

      for (i <- 0 until model_matrix_Nt_1.length)
        model_matrix_Nt_1(i) = 1
      val model_M_Nt_1 = sc.parallelize(model_matrix_Nt_1).map(m => Vectors.dense(m))
      val model_M_Nt_1_RowM: RowMatrix = new RowMatrix(model_M_Nt_1)
      val mod_M_Nt_1_byColumnAndRow = model_M_Nt_1_RowM.rows.zipWithIndex.map {
        case (row, rowIndex) => row.toArray.zipWithIndex.map {
          case (number, columnIndex) => new MatrixEntry(rowIndex, columnIndex, number)
        }
      }.flatMap(x => x)
      val model_M_Nt_1_blockMatrix = new CoordinateMatrix(mod_M_Nt_1_byColumnAndRow).toBlockMatrix()
      val model_M_Nt_Gc_blockMatrix = model_M_Nt_1_blockMatrix.multiply(model_M_1_Gc_blockMatrix)
      val model_M_Gc_Nt_blockMatrix = model_M_Nt_Gc_blockMatrix.transpose
      Mc = model_blockMatrix.subtract(model_M_Gc_Nt_blockMatrix)

      //save to disk
      Mc.toIndexedRowMatrix().rows.saveAsObjectFile(mc_path)
    }
    Mc.persist(StorageLevel.MEMORY_AND_DISK)

    //Matrix Multiplication
    //val matrix_mul = model_blockMatrix.multiply(satellite_blockMatrix)
    val matrix_mul = Mc.multiply(Sc)
    val resRowMatrix: RowMatrix = new RowMatrix(matrix_mul.toIndexedRowMatrix().rows.sortBy(_.index).map(_.vector))
    matrix_mul.persist(StorageLevel.MEMORY_AND_DISK)

    //SVD
    val svd: SingularValueDecomposition[RowMatrix, Matrix] = resRowMatrix.computeSVD(10, true)

    val U: RowMatrix = svd.U // The U factor is a RowMatrix.
    val s: Vector = svd.s // The singular values are stored in a local dense vector.
    val V: Matrix = svd.V // The V factor is a local dense matrix.


    //BUILD GEOTIFFS
    //Merge two RDDs, one containing the clusters_ID indices and the other one the indices of a Tile's grid cells
    val multi_res = sc.parallelize(s.toArray).zipWithIndex().map { case (v, i) => (i, v) }
    val multi_cell_pos = multi_res.join(model_grid0_index.zipWithIndex().map { case (v, i) => (i, v) }).map { case (k, (v, i)) => (v, i) }

    //Associate a Cluster_IDs to respective Grid_cell
    val grid_multi = model_grid0.map { case (i, v) => if (v == -1000) (i, Double.NaN) else (i, v) }.leftOuterJoin(multi_cell_pos.map { case (c, i) => (i.toLong, c) })

    //Convert all None to NaN
    val grid_multi_res = grid_multi.sortByKey(true).map { case (k, (v, c)) => if (c == None) (k, Double.NaN) else (k, c.get.toDouble) }

    //Define a Tile
    val corr_cells: Array[Double] = grid_multi_res.values.collect()

    val corr_cellsD = DoubleArrayTile(corr_cells, num_cols_rows._1, num_cols_rows._2)

    val geoTif = new SinglebandGeoTiff(corr_cellsD, projected_extent.extent, projected_extent.crs, Tags.empty, GeoTiffOptions(compression.DeflateCompression))

    //Save to /tmp/
    GeoTiffWriter.write(geoTif, corr_tif_tmp)

    //Upload to HDFS
    var cmd = "hadoop dfs -copyFromLocal -f " + corr_tif_tmp + " " + corr_tif
    Process(cmd) !

    //Remove from /tmp/
    cmd = "rm -fr " + corr_tif_tmp
    Process(cmd) !

  }
}
