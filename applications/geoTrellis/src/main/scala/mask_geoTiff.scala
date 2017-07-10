import geotrellis.proj4.CRS
import geotrellis.raster.{DoubleArrayTile, Tile}
import geotrellis.raster.io.geotiff.writer.GeoTiffWriter
import geotrellis.raster.io.geotiff.{SinglebandGeoTiff, _}
import geotrellis.spark.io.hadoop._
import geotrellis.vector.{Extent, ProjectedExtent}
import org.apache.spark.{SparkConf, SparkContext}

import scala.sys.process._

//Spire is a numeric library for Scala which is intended to be generic, fast, and precise.
import spire.syntax.cfor._

object mask_geoTiff extends App {

  override def main(args: Array[String]): Unit = {
    val appName = this.getClass.getName
    val masterURL = "spark://emma0.emma.nlesc.nl:7077"

    val sc = new SparkContext(new SparkConf().setAppName(appName).setMaster(masterURL))

    //Read GeoTiff
    var geo_projected_extent = new ProjectedExtent(new Extent(0,0,0,0), CRS.fromName("EPSG:3857"))
    var geo_num_cols_rows :(Int, Int) = (0, 0)
    val geo_path = "hdfs:///user/hadoop/spring-index/LastFreeze/1980.tif"
    val geo_tiles_RDD = sc.hadoopGeoTiffRDD(geo_path).values

    val geo_extents_withIndex = sc.hadoopGeoTiffRDD(geo_path).keys.zipWithIndex().map{case (e,v) => (v,e)}
    geo_projected_extent = (geo_extents_withIndex.filter(m => m._1 == 0).values.collect())(0)

    val geo_tiles_withIndex = geo_tiles_RDD.zipWithIndex().map{case (e,v) => (v,e)}
    val geo_tile0 = (geo_tiles_withIndex.filter(m => m._1==0).values.collect())(0)
    geo_num_cols_rows = (geo_tile0.cols, geo_tile0.rows)
    val geo_cellT = geo_tile0.cellType
    var mask_tile0 :Tile = new SinglebandGeoTiff(geotrellis.raster.ArrayTile.empty(geo_cellT, geo_num_cols_rows._1, geo_num_cols_rows._2), geo_projected_extent.extent, geo_projected_extent.crs, Tags.empty, GeoTiffOptions.DEFAULT).tile

    //Read Mask
    val mask_path = "hdfs:///user/hadoop/usa_mask.tif"
    val mask_tiles_RDD = sc.hadoopGeoTiffRDD(mask_path).values
    val mask_tiles_withIndex = mask_tiles_RDD.zipWithIndex().map{case (e,v) => (v,e)}
    mask_tile0 = (mask_tiles_withIndex.filter(m => m._1==0).values.collect())(0)

    //Mask GeoTiff
    //We need to broadcast the mask to all nodes to then be able to run a distributed mask.
    //First we need to make sure we are masking.
    val res_tile = geo_tile0.localMask(mask_tile0, 1, Double.NaN.toInt)

    res_tile.toArrayDouble().map( m => if (m == 0.0) Double.NaN else m).take(50)
    //Save the new GeoTiff
    val clone_tile = DoubleArrayTile(res_tile.toArrayDouble(), geo_num_cols_rows._1, geo_num_cols_rows._2)

    val cloned = geotrellis.raster.ArrayTile.empty(geo_cellT, geo_num_cols_rows._1, geo_num_cols_rows._2)
    cfor(0)(_ < geo_num_cols_rows._1, _ + 1) { col =>
      cfor(0)(_ < geo_num_cols_rows._2, _ + 1) { row =>
        val v = clone_tile.getDouble(col, row)
        cloned.setDouble(col, row, v)
      }
    }

    val geoTif = new SinglebandGeoTiff(cloned, geo_projected_extent.extent, geo_projected_extent.crs, Tags.empty, GeoTiffOptions.DEFAULT)

    //Save GeoTiff to /tmp
    val output = "/user/emma/spring-index/LastFreeze/1980_mask_clone.tif"
    val tmp_output = "/tmp/1980_mask_clone.tif"
    GeoTiffWriter.write(geoTif, tmp_output)

    //Upload to HDFS
    var cmd = "hadoop dfs -copyFromLocal -f " + tmp_output + " " + output
    Process(cmd)!

    cmd = "rm -fr " + tmp_output
    Process(cmd)!

  }
}