package main.scala


import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}

import geotrellis.proj4.CRS
import geotrellis.raster.io.geotiff.{SinglebandGeoTiff, _}
import geotrellis.raster.{CellType, Tile, UByteCellType}
import geotrellis.spark.io.hadoop._
import geotrellis.vector.{Extent, ProjectedExtent}
import org.apache.hadoop.io.SequenceFile.Writer
import org.apache.hadoop.io.{SequenceFile, _}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry, RowMatrix}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//Spire is a numeric library for Scala which is intended to be generic, fast, and precise.

object co_clustering_model extends App {

  override def main(args: Array[String]): Unit = {
    val appName = this.getClass.getName
    val masterURL = "spark://emma0.emma.nlesc.nl:7077"
    val sc = new SparkContext(new SparkConf().setAppName(appName).setMaster(masterURL))

    //Operation mode
    var rdd_offline_mode = true
    var matrix_offline_mode = true

    //GeoTiffs to be read from "hdfs:///user/hadoop/spring-index/"
    var dir_path = "hdfs:///user/hadoop/spring-index/"
    var offline_dir_path = "hdfs:///user/emma/spring-index/"
    var geoTiff_dir = "LeafFinal"
    var band_num = 3

    //Years between (inclusive) 1989 - 2014
    var model_first_year = 1989
    var model_last_year = 2014

    //Mask
    val toBeMasked = true
    val mask_path = "hdfs:///user/hadoop/usa_mask.tif"

    //Kmeans number of iterations and clusters
    var numIterations = 75
    var minClusters = 100
    var maxClusters = 120
    var stepClusters = 10

    //Validation, do not modify these lines.
    var single_band = false
    if (geoTiff_dir == "BloomFinal" || geoTiff_dir == "LeafFinal") {
      single_band = false
    } else if (geoTiff_dir == "LastFreeze" || geoTiff_dir == "DamageIndex") {
      single_band = true
      if (band_num > 0) {
        println("Since LastFreezze and DamageIndex are single band, we will use band 0!!!")
        band_num  = 0
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
    var grid0_path = offline_dir_path + geoTiff_dir + "/grid0" + "_" + band_num + mask_str
    var grid0_index_path = offline_dir_path + geoTiff_dir + "/grid0_index" + "_" + band_num + mask_str
    var grids_noNaN_path = offline_dir_path + geoTiff_dir + "/grids_noNaN" + "_" + band_num + mask_str
    var metadata_path = offline_dir_path + geoTiff_dir + "/metadata" + "_" + band_num + mask_str
    var grids_matrix_path = offline_dir_path + geoTiff_dir + "/grids_matrix" + "_" + band_num + mask_str

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
    var grid_cells_size: Long = 0

    //Load Mask
    if (toBeMasked) {
      val mask_tiles_RDD = sc.hadoopGeoTiffRDD(mask_path).values
      val mask_tiles_withIndex = mask_tiles_RDD.zipWithIndex().map { case (e, v) => (v, e) }
      mask_tile0 = (mask_tiles_withIndex.filter(m => m._1 == 0).values.collect()) (0)
    }

    //Local variables
    val pattern: String = "tif"
    val filepath: String = dir_path + geoTiff_dir

    if (rdd_offline_mode) {
      grids_noNaN_RDD = sc.objectFile(grids_noNaN_path)
      grid0 = sc.objectFile(grid0_path)
      grid0_index = sc.objectFile(grid0_index_path)

      val metadata = sc.sequenceFile(metadata_path, classOf[IntWritable], classOf[BytesWritable]).map(_._2.copyBytes()).collect()
      projected_extent = deserialize(metadata(0)).asInstanceOf[ProjectedExtent]
      num_cols_rows = (deserialize(metadata(1)).asInstanceOf[Int], deserialize(metadata(2)).asInstanceOf[Int])
      cellT = deserialize(metadata(3)).asInstanceOf[CellType]
    } else {
      if (single_band) {
        //Lets load a Singleband GeoTiffs and return RDD just with the tiles.
        var tiles_RDD: RDD[Tile] = sc.hadoopGeoTiffRDD(filepath, pattern).values

        //Retrive the numbre of cols and rows of the Tile's grid
        val tiles_withIndex = tiles_RDD.zipWithIndex().map { case (e, v) => (v, e) }
        val tile0 = (tiles_withIndex.filter(m => m._1 == 0).values.collect()) (0)
        num_cols_rows = (tile0.cols, tile0.rows)
        cellT = tile0.cellType

        if (toBeMasked) {
          val mask_tile_broad: Broadcast[Tile] = sc.broadcast(mask_tile0)
          grids_RDD = tiles_RDD.map(m => m.localInverseMask(mask_tile_broad.value, 1, -1000).toArrayDouble())
        } else {
          grids_RDD = tiles_RDD.map(m => m.toArrayDouble())
        }
      } else {
        //Lets load Multiband GeoTiffs and return RDD just with the tiles.
        val tiles_RDD = sc.hadoopMultibandGeoTiffRDD(filepath, pattern).values

        //Retrive the numbre of cols and rows of the Tile's grid
        val tiles_withIndex = tiles_RDD.zipWithIndex().map { case (e, v) => (v, e) }
        val tile0 = (tiles_withIndex.filter(m => m._1 == 0).values.collect()) (0)
        num_cols_rows = (tile0.cols, tile0.rows)
        cellT = tile0.cellType

        //Lets read the average of the Spring-Index which is stored in the 4th band
        val band_numB: Broadcast[Int] = sc.broadcast(band_num)
        if (toBeMasked) {
          val mask_tile_broad: Broadcast[Tile] = sc.broadcast(mask_tile0)
          grids_RDD = tiles_RDD.map(m => m.band(band_numB.value).localInverseMask(mask_tile_broad.value, 1, -1000).toArrayDouble())
        } else {
          grids_RDD = tiles_RDD.map(m => m.band(band_numB.value).toArrayDouble())
        }
      }

      //Retrieve the ProjectExtent which contains metadata such as CRS and bounding box
      val projected_extents_withIndex = sc.hadoopGeoTiffRDD(filepath, pattern).keys.zipWithIndex().map { case (e, v) => (v, e) }
      projected_extent = (projected_extents_withIndex.filter(m => m._1 == 0).values.collect()) (0)

      //Get Index for each Cell
      val grids_withIndex = grids_RDD.zipWithIndex().map { case (e, v) => (v, e) }
      if (toBeMasked) {
        grid0_index = grids_withIndex.filter(m => m._1 == 0).values.flatMap(m => m).zipWithIndex.filter(m => m._1 != -1000.0).map { case (v, i) => (i) }
      } else {
        grid0_index = grids_withIndex.filter(m => m._1 == 0).values.flatMap(m => m).zipWithIndex.map { case (v, i) => (i) }

      }
      //Get the Tile's grid
      grid0 = grids_withIndex.filter(m => m._1 == 0).values.flatMap(m => m).zipWithIndex.map { case (v, i) => (i, v) }

      //Lets filter out NaN
      if (toBeMasked) {
        grids_noNaN_RDD = grids_RDD.map(m => m.filter(m => m != -1000.0))
      } else {
        grids_noNaN_RDD = grids_RDD
      }
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
      writer.append(new IntWritable(4), new BytesWritable(serialize(cellT)))
      writer.hflush()
      writer.close()
    }
    grid_cells_size = grid0_index.count().toInt
    var t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0) + "ns")

    t0 = System.nanoTime()
    //Global variables
    var grids_matrix: RDD[Vector] = sc.emptyRDD
    val grid_cells_sizeB = sc.broadcast(grid_cells_size)

    if (matrix_offline_mode) {
      grids_matrix = sc.objectFile(grids_matrix_path)
    } else {
      //Dense Vector
      //val mat: RowMatrix = new RowMatrix(grids_noNaN_RDD.map(m => Vectors.dense(m)))
      //Sparse Vector
      val mat: RowMatrix = new RowMatrix(grids_noNaN_RDD.map(m => m.zipWithIndex).map(m => m.filter(!_._1.isNaN)).map(m => Vectors.sparse(grid_cells_sizeB.value.toInt, m.map(v => v._2), m.map(v => v._1)))) // Split the matrix into one number per line.
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

    //SUPPORT FUNCTIONS

    //CALCUALTE AVERAGE
    /*
    calculate_average <- function(Left, matrixDF, Right, W, epsilon) {
      if (is.null(W)) {
        #A 2D array, i.e., a Matrix for which each cell is set with the value 1
        W <- array(1, dim(matrixDF))
      } else {
        # Element-wise multiplication
          matrixDF <- W * matrixDF
      }
      # t(Left) is Matrix transpose
      # %*% means matrix mutiplication
      # mean(matrix) gives a single value which is the mean of all values in the matrix
      # y=mean(x,'r') (or, equivalently, y=mean(x,1)) is the rowwise mean.
      # y=mean(x,'c') (or, equivalently, y=mean(x,2)) is the columnwise mean.
      # Right + means(matrix) is a element-wise addition
        numerator <- t(Left) %*% matrixDF %*% Right + mean(matrixDF) * epsilon

      denominator <- t(Left) %*% W %*% Right + epsilon
      return(numerator/denominator)
    }
    */

    def calculate_average (Left :CoordinateMatrix, matrixDF :CoordinateMatrix, Right :CoordinateMatrix, W :CoordinateMatrix, epsilon :Double): CoordinateMatrix = {
      var _W :CoordinateMatrix = null
      var _matrixDF : CoordinateMatrix = null

      if (W == null) {
        val byColumnAndRow = matrixDF.toRowMatrix().rows.zipWithIndex.map {
          case (row, rowIndex) => row.toArray.zipWithIndex.map {
            case (number, columnIndex) => new MatrixEntry(rowIndex, columnIndex, 1)
          }
        }.flatMap(x => x)

        _W = new CoordinateMatrix(byColumnAndRow)
      } else {
        //We assume that both rows fit in memory
        val joined_mat :RDD[ (Long, (Array[Double], Array[Double]))] = W.toRowMatrix().rows.map(_.toArray).zipWithUniqueId().map{case (v,i) => (i,v)}.join(matrixDF.toRowMatrix().rows.map(_.toArray).zipWithUniqueId().map{case (v,i) => (i,v)})
        _matrixDF = new CoordinateMatrix(joined_mat.map {case (row_index, (a,b)) => a.zip(b).map(m => m._1*m._2).zipWithIndex.map{ case (v,col_index) => new MatrixEntry(row_index, col_index,v)}}.flatMap(m => m))
      }

      val leftT = Left.transpose
      val leftT_matrixDF_right = leftT.toBlockMatrix().multiply(_matrixDF.toBlockMatrix().multiply(Right.toBlockMatrix()))
      val mean_matrixDF_epsilon = _matrixDF.toRowMatrix().rows.map(m => m.toArray.sum/m.size).reduce( (a,b) => a+b)/_matrixDF.numRows().toDouble * epsilon
      val mean_matrixDF_epsilonB = sc.broadcast(mean_matrixDF_epsilon)
      val numerator = leftT_matrixDF_right.toIndexedRowMatrix().rows.map( m => m.vector.toArray.map(m => m+mean_matrixDF_epsilonB.value))
      mean_matrixDF_epsilonB.destroy()

      val leftT_w_right = leftT.toBlockMatrix().multiply(W.toBlockMatrix().multiply(Right.toBlockMatrix()))
      val epsilonB = sc.broadcast(epsilon)
      val denominator = leftT_w_right.toIndexedRowMatrix().rows.map( m => m.vector.toArray.map(m => m+epsilonB.value))

      //We assume the two rows fit in memory
      val numerator_denominator :RDD[ (Long, (Array[Double], Array[Double]))] = numerator.zipWithUniqueId().map{ case (v,i) => (i,v)}.join(denominator.zipWithUniqueId().map{case (v,i) => (i,v)})
      new CoordinateMatrix(numerator_denominator.map{ case (row_index,(a,b)) => a.zip(b).map(m => m._1 / m._2).zipWithIndex.map{ case (v,col_index) => new MatrixEntry(row_index, col_index,v)}}.flatMap(m => m))
    }

    /*
    coCavg <- function(dist, row_col, R, matrixDF, C, W, epsilon) {
      CoCavg <- calculate_average(R, matrixDF, C, W, epsilon)
      if (row_col=="row") {
        #Creates a list and names the elements. Such names can then be used to access them in an easy way.
        return(list(Zrowc = array(dist, dim(matrixDF)), Zrowv = CoCavg %*% t(C)))
      } else if (row_col=="col") {
        return(list(Zcolc = array(dist, dim(matrixDF)), Zcolv = R %*% CoCavg))
      }
    }
    */

    def coCavg (dist :Double, row_col :String, Right: CoordinateMatrix, matrixDF: CoordinateMatrix, C: CoordinateMatrix, W: CoordinateMatrix, epsilon: Double) :(CoordinateMatrix,CoordinateMatrix) = {
      val CoCavg = calculate_average(Right, matrixDF, C, W, epsilon)
      val byColumnAndRow = matrixDF.toRowMatrix().rows.zipWithIndex.map {
        case (row, rowIndex) => row.toArray.zipWithIndex.map {
          case (number, columnIndex) => new MatrixEntry(rowIndex, columnIndex, dist)
        }
      }.flatMap(x => x)
      val a = new CoordinateMatrix(byColumnAndRow)
      if (row_col.equals("row")) {
        (a, CoCavg.toBlockMatrix().multiply(C.toBlockMatrix().transpose).toCoordinateMatrix())
      } else {
        (a, Right.toBlockMatrix().multiply(CoCavg.toBlockMatrix().transpose).toCoordinateMatrix())
      }
    }

    /*
    similarity_measure <- function(dist, matrixDF, X, Y, W, epsilon) {
      if (is.null(W))
      W <- array(1, dim(matrixDF))
      if (dist==0) {
        # rowSums sum values of Raster objects.
        # rep vector several times, but with each we repeat the values, in this case has many as the matrixDF rows
          #> rep(1:4, 2)
        #  [1] 1 2 3 4 1 2 3 4
        # > rep(1:4, each = 2)       # not the same.
        #  [1] 1 1 2 2 3 3 4 4
        euc <- function(i) rowSums(W * (matrixDF - X - rep(Y[i,], each = dim(matrixDF)[1]))^2)
        return(sapply(1:dim(Y)[1], euc))
      } else if (dist==1) {
        # log(t(Y + epsilon)): sum epsilon to all elements of Y, transpose it and do the log to each element of the transpose matrix
        return((W * X) %*% t(Y + epsilon) - (W * matrixDF) %*% log(t(Y + epsilon)))
      }
    }
    */

    def euc (i: Long, matrixDF_X: CoordinateMatrix, Y: CoordinateMatrix, W: CoordinateMatrix, each :Int) :RDD[MatrixEntry] = {
      val Y_row_i = Y.toRowMatrix().rows.zipWithIndex().filter(_._2 == i)

      val eachB = sc.broadcast(each)
      val rep_Y_row_i = Y_row_i.map{ case (v, i) => v.toArray.map( m => Array.fill(eachB.value)(m))}.flatMap(m => m)
      eachB.destroy()

      val matrixDF_X_rep_Y_joined_mat :RDD[ (Long, (Array[Double], Array[Double]))] = matrixDF_X.toRowMatrix().rows.map(_.toArray).zipWithUniqueId().map{case (v,i) => (i,v)}.join(rep_Y_row_i.zipWithUniqueId().map{ case (v,i) => (i,v)})
      val matrixDF_X_rep_Y = new CoordinateMatrix(matrixDF_X_rep_Y_joined_mat.map {case (row_index, (a,b)) => a.zip(b).map(m => math.pow(m._1-m._2,2)).zipWithIndex.map{ case (v,col_index) => new MatrixEntry(row_index, col_index,v)}}.flatMap(m => m))

      val joined_mat :RDD[ (Long, (Array[Double], Array[Double]))] = W.toRowMatrix().rows.map(_.toArray).zipWithUniqueId().map{case (v,i) => (i,v)}.join(matrixDF_X_rep_Y.toRowMatrix().rows.map(_.toArray).zipWithUniqueId().map{case (v,i) => (i,v)})
      val iB = sc.broadcast(i)
      val res = joined_mat.map {case (row_index, (a,b)) => a.zip(b).map(m => m._1-m._2)}.map( m => (iB.value,m.sum)).zipWithIndex.map{ case ((row_index, v),col_index) => new MatrixEntry(row_index, col_index,v)}
      iB.destroy()
      res
    }

    def similarity_measure(dist :Double, matrixDF : CoordinateMatrix, X: CoordinateMatrix, Y: CoordinateMatrix, W: CoordinateMatrix, epsilon :Double) :CoordinateMatrix = {
      var _W :CoordinateMatrix = null
      var _matrixDF : CoordinateMatrix = null

      if (W == null) {
        val byColumnAndRow = matrixDF.toRowMatrix().rows.zipWithIndex.map {
          case (row, rowIndex) => row.toArray.zipWithIndex.map {
            case (number, columnIndex) => new MatrixEntry(rowIndex, columnIndex, 1)
          }
        }.flatMap(x => x)

        _W = new CoordinateMatrix(byColumnAndRow)
      }
      if (dist == 0) {
        val matrixDF_X_joined_mat :RDD[ (Long, (Array[Double], Array[Double]))] = matrixDF.toRowMatrix().rows.map(_.toArray).zipWithUniqueId().map{case (v,i) => (i,v)}.join(X.toRowMatrix().rows.map(_.toArray).zipWithUniqueId().map{case (v,i) => (i,v)})
        val X_matrixDF = new CoordinateMatrix(matrixDF_X_joined_mat.map {case (row_index, (a,b)) => a.zip(b).map(m => m._1-m._2).zipWithIndex.map{ case (v,col_index) => new MatrixEntry(row_index, col_index,v)}}.flatMap(m => m))
        val matrixDF_rows = matrixDF.numRows().toInt

        var resRDD :RDD[MatrixEntry] = sc.emptyRDD

        for (i <- 0 until (Y.numRows())) {
          if (resRDD.isEmpty()) {
            resRDD = euc(i, X_matrixDF, Y, W, matrixDF_rows)
          } else {
            resRDD = resRDD.union(euc(i, X_matrixDF, Y, W, matrixDF_rows))
          }
        }
        new CoordinateMatrix(resRDD)
      } else {
        val W_X_joined_mat :RDD[ (Long, (Array[Double], Array[Double]))] = W.toRowMatrix().rows.map(_.toArray).zipWithUniqueId().map{case (v,i) => (i,v)}.join(X.toRowMatrix().rows.map(_.toArray).zipWithUniqueId().map{case (v,i) => (i,v)})
        val W_X = new CoordinateMatrix(W_X_joined_mat.map {case (row_index, (a,b)) => a.zip(b).map(m => m._1*m._2).zipWithIndex.map{ case (v,col_index) => new MatrixEntry(row_index, col_index,v)}}.flatMap(m => m))

        val epsilonB = sc.broadcast(epsilon)
        val Y_epsilonT = new CoordinateMatrix(Y.toIndexedRowMatrix().rows.map(m => m.vector.toArray.map(m => m+epsilonB.value)).zipWithIndex().map{ case (a, row_index) => a.zipWithIndex.map{ case (v, col_index) => new MatrixEntry(row_index, col_index, v)}}.flatMap(m => m)).transpose()

        val W_matrixDF_joined_mat :RDD[ (Long, (Array[Double], Array[Double]))] = W.toRowMatrix().rows.map(_.toArray).zipWithUniqueId().map{case (v,i) => (i,v)}.join(matrixDF.toRowMatrix().rows.map(_.toArray).zipWithUniqueId().map{case (v,i) => (i,v)})
        val W_matrixDF = new CoordinateMatrix(W_matrixDF_joined_mat.map {case (row_index, (a,b)) => a.zip(b).map(m => m._1*m._2).zipWithIndex.map{ case (v,col_index) => new MatrixEntry(row_index, col_index,v)}}.flatMap(m => m))

        val log_Y_epsilonT = new CoordinateMatrix(Y_epsilonT.toIndexedRowMatrix().rows.map(m => m.vector.toArray.map(m => math.log(m))).zipWithIndex().map{ case (a, row_index) => a.zipWithIndex.map{ case (v, col_index) => new MatrixEntry(row_index, col_index, v)}}.flatMap(m => m)).transpose()

        val W_X_Y_epsilonT = W_X.toBlockMatrix().multiply(Y_epsilonT.toBlockMatrix()).toCoordinateMatrix()
        val W_matrixDF_log_Y_epsilonT = W_matrixDF.toBlockMatrix().multiply(log_Y_epsilonT.toBlockMatrix()).toCoordinateMatrix()

        val joined_mat :RDD[ (Long, (Array[Double], Array[Double]))] = W_X_Y_epsilonT.toRowMatrix().rows.map(_.toArray).zipWithUniqueId().map{case (v,i) => (i,v)}.join(W_matrixDF_log_Y_epsilonT.toRowMatrix().rows.map(_.toArray).zipWithUniqueId().map{case (v,i) => (i,v)})
        new CoordinateMatrix(joined_mat.map {case (row_index, (a,b)) => a.zip(b).map(m => m._1-m._2).zipWithIndex.map{ case (v,col_index) => new MatrixEntry(row_index, col_index,v)}}.flatMap(m => m))
      }
    }

    /*
    assign_cluster <- function(dist, matrixDF, X, Y, W, epsilon) {
      D <- similarity_measure(dist, matrixDF, X, Y, W, epsilon)
      # apply(Matrix, <row or col>, func) -> <row or col> 1 is row-wise, 2 is col-wise
      # sapply is like lapply (it applies a function to each element of a list and the result is also a list) consumes data as a vector.
      # dim(D)[1] gives number of rows.
      # sort a each row increasing order and return index, we get the indice of the highest value
        id <- sapply(1:dim(D)[1], function(i) sort(D[i,], index.return = TRUE)$ix[1])
      res <- sapply(1:dim(D)[1], function(i) sort(D[i,])[1]^(2-dist))

      # Create an identity matrix, diag(dim(Y)[1]), which has num_rows and num_cols = dim(Y)[1], i.e., number of rows of Y, and set diagonal to 1.
      # dim(Y)[1])[id,] -> Give me the row from the identity matrix which has indice "id"
      return(list(Cluster = diag(dim(Y)[1])[id,], Error = sum(res)))
    }
    */

    def assign_cluster (dist: Int, matrixDF :CoordinateMatrix, X: CoordinateMatrix, Y: CoordinateMatrix, W: CoordinateMatrix, epsilon :Double) :(RDD[MatrixEntry], Double) = {
      val D = similarity_measure(dist, matrixDF, X, Y, W, epsilon)
      val id = D.toRowMatrix().rows.map(_.toArray.zipWithIndex.sortBy(_._1).map(_._2).head)
      val dist2 :Int = 2-dist
      val dist2B = sc.broadcast(dist2)
      val res = D.toRowMatrix().rows.map( m => math.pow(m.toArray.sorted.head, dist2B.value))
      dist2B.destroy()

      val Y_num_rows = Y.numRows().toInt
      val Y_num_rowsB = sc.broadcast(Y_num_rows)
      val byColumnAndRow = matrixDF.toRowMatrix().rows.zipWithIndex.map {
        case (row, rowIndex) => Array.fill(0)(Y_num_rowsB.value).zipWithIndex.map {
          case (number, columnIndex) => if (columnIndex == rowIndex) (rowIndex, new MatrixEntry(rowIndex, columnIndex, 1)) else (rowIndex, new MatrixEntry(rowIndex, columnIndex, number))
        }
      }.flatMap(x => x)
      Y_num_rowsB.destroy()

      (byColumnAndRow.join(id.map(_.toLong).zipWithUniqueId()).map(_._2._1), res.reduce((a,b) => a+b))
    }

    /*
    bbac <- function(matrixDF, numRowC, numColC, W = NULL, distance = "euclidean", errobj = 1e-6, niters = 100, nruns = 5, epsilon = 1e-8) {
      error <- Inf
      error_now <- Inf
      dist <- pmatch(tolower(distance), c("euclidean","divergence")) - 1

      for (r in 1:nruns) {
        # Initialization of R and C
        #Create an identity matrix
        # Get row which is a random sample (betwewn numRowC and num of rows) with replacement
        R <- diag(numRowC)[base::sample(numRowC, dim(matrixDF)[1], replace = TRUE),]
        C <- diag(numColC)[base::sample(numColC, dim(matrixDF)[2], replace = TRUE),]

        for (s in 1:niters) {
          # Row estimation
            rs <- coCavg(dist, "row", R, matrixDF, C, W, epsilon)
          ra <- assign_cluster(dist, matrixDF, rs$Zrowc, rs$Zrowv, W, epsilon)
          R  <- ra$Cluster

          # Column estimation
            cs <- coCavg(dist, "col", R, matrixDF, C, W, epsilon)
          ca <- assign_cluster(dist, t(matrixDF), t(cs$Zcolc), t(cs$Zcolv), W, epsilon)
          C  <- ca$Cluster

          #
          if (abs(ca$Error - error_now) < errobj) {
            status <- paste("converged in",s,"iterations")
            return(list(R = R, C = C, status = status))
          }

          # Update objective value
          error_now <- ca$Error

        }

        # Keep pair with min error
        if (error_now < error) {
          R_star <- R
          C_star <- C
          error <- error_now
        }
      }

      status <- paste("reached maximum of", niters, "iterations")
      return(list(R = R_star, C = C_star, status = status))
    }
    */

    def bbac (matrixDF :CoordinateMatrix, numRowC: Int, numColC :Int, W :CoordinateMatrix, distance :String, errobj :Double, niters :Int, nruns :Int, epsilon :Double) :(CoordinateMatrix, CoordinateMatrix, String) = {
      var error :Double = Double.MaxValue
      var error_now :Double = Double.MaxValue
      var status :String = ""
      var R_star :CoordinateMatrix = null
      var C_star :CoordinateMatrix = null
      var R :CoordinateMatrix = null
      var C :CoordinateMatrix = null

      val dist = if (distance.toLowerCase.equals("euclidean")) 0 else 1 // "divergence"

      for (r <- 0 until nruns) {
        val rnd = new scala.util.Random
        var start = numRowC
        var end   = matrixDF.numCols().toInt

        val R_rnd = numRowC + rnd.nextInt((matrixDF.numRows().toInt - numRowC) + 1)
        val R_rndB = sc.broadcast(R_rnd)
        val R_byColumnAndRow = matrixDF.toRowMatrix().rows.zipWithIndex.map {
          case (row, rowIndex) => row.toArray.zipWithIndex.map {
            case (number, columnIndex) => if (columnIndex == rowIndex) new MatrixEntry(rowIndex, columnIndex, R_rndB.value) else new MatrixEntry(rowIndex, columnIndex, 0)
          }
        }.flatMap(x => x)
        R = new CoordinateMatrix(R_byColumnAndRow)
        R_rndB.destroy()

        val C_rnd = numColC + rnd.nextInt((matrixDF.numCols().toInt - numColC) + 1)
        val C_rndB = sc.broadcast(C_rnd)
        val C_byColumnAndRow = matrixDF.toRowMatrix().rows.zipWithIndex.map {
          case (row, rowIndex) => row.toArray.zipWithIndex.map {
            case (number, columnIndex) => if (columnIndex == rowIndex) new MatrixEntry(rowIndex, columnIndex, C_rndB.value) else new MatrixEntry(rowIndex, columnIndex, 0)
          }
        }.flatMap(x => x)
        C = new CoordinateMatrix(R_byColumnAndRow)
        C_rndB.destroy()

        for (s <- 0 until niters) {
          //Row estimation
          val rs = coCavg(dist, "row", R, matrixDF, C, W, epsilon)
          val ra = assign_cluster(dist, matrixDF, rs._1, rs._2, W, epsilon)
          R  = new CoordinateMatrix(ra._1)

          //Column estimation
          val cs = coCavg(dist, "col", R, matrixDF, C, W, epsilon)
          val ca = assign_cluster(dist, matrixDF.transpose(), cs._1.transpose(), cs._2.transpose(), W, epsilon)
          C  = new CoordinateMatrix(ca._1)

          if (math.abs(ca._2 - error_now) < errobj) {
            val status = "converged in " + s + " iterations"
            return (R, C, status)
          }

          //Update objective value
          error_now = ca._2
        }

        //Keep pair with min error
        if (error_now < error) {
          R_star = R
          C_star = C
          error = error_now
        }
      }
      status = "reached maximum of " + niters + " iterations"
      (R_star, C_star, status)
    }

  }
}
