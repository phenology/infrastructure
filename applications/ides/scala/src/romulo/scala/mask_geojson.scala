
import sys.process._
import geotrellis.proj4.CRS
import geotrellis.raster.io.geotiff.writer.GeoTiffWriter
import geotrellis.raster.io.geotiff.{SinglebandGeoTiff, _}
import geotrellis.raster.{DoubleArrayTile, Tile}
import geotrellis.spark.io.hadoop._
import geotrellis.vector.io.json.GeoJson
import geotrellis.vector.io.wkb.WKB
import geotrellis.vector._
import org.apache.spark.{SparkConf, SparkContext}

//Spire is a numeric library for Scala which is intended to be generic, fast, and precise.
import spire.syntax.cfor._

//spray-json to parse strings as json
import spray.json._
import spray.json.JsonFormat

object mask_geojson extends App {

  override def main(args: Array[String]): Unit = {
    val appName = this.getClass.getName
    val masterURL = "spark://emma0.emma.nlesc.nl:7077"
    val sc = new SparkContext(new SparkConf().setAppName(appName).setMaster(masterURL))

    var geo_projected_extent = new ProjectedExtent(new Extent(0, 0, 0, 0), CRS.fromName("EPSG:3857"))
    var geo_num_cols_rows: (Int, Int) = (0, 0)
    val geo_path = "hdfs:///user/hadoop/modis/Onset_Greenness_Maximum/A2001001__Onset_Greenness_Maximum.tif"
    val geo_tiles_RDD = sc.hadoopGeoTiffRDD(geo_path).values

    val geo_extents_withIndex = sc.hadoopMultibandGeoTiffRDD(geo_path).keys.zipWithIndex().map { case (e, v) => (v, e) }
    geo_projected_extent = (geo_extents_withIndex.filter(m => m._1 == 0).values.collect()) (0)

    val geo_tiles_withIndex = geo_tiles_RDD.zipWithIndex().map { case (e, v) => (v, e) }
    val geo_tile0 = (geo_tiles_withIndex.filter(m => m._1 == 0).values.collect()) (0)
    geo_num_cols_rows = (geo_tile0.cols, geo_tile0.rows)
    val geo_cellT = geo_tile0.cellType

    val geojson_path = "/user/hadoop/modis/usa_mask.geojson"
    val geojson_file = sc.wholeTextFiles(geojson_path)
    //val geojson :String = geojson_file.first()._2.parseJson.prettyPrint
    val geojson =
      """{
        |  "type": "Polygon",
        |  "coordinates": [[[0.0, 0.0], [0.0, 1.0], [1.0, 1.0], [0.0, 0.0]]]
        |}""".
        stripMargin.parseJson
    val geom : Geometry = geojson.convertTo[Polygon]

    val res_tile = geo_tile0.mask(geo_projected_extent.extent, geom)

    val clone_tile = DoubleArrayTile(res_tile, geo_num_cols_rows._1, geo_num_cols_rows._2)

    val cloned = geotrellis.raster.DoubleArrayTile.empty(geo_num_cols_rows._1, geo_num_cols_rows._2)
    cfor(0)(_ < geo_num_cols_rows._1, _ + 1) { col =>
      cfor(0)(_ < geo_num_cols_rows._2, _ + 1) { row =>
        val v = clone_tile.getDouble(col, row)
        cloned.setDouble(col, row, v)
      }
    }

    val geoTif = new SinglebandGeoTiff(cloned, geo_projected_extent.extent, geo_projected_extent.crs, Tags.empty, GeoTiffOptions(compression.DeflateCompression))

    //Save GeoTiff to /tmp
    val output = "/user/emma/modis/Onset_Greenness_Maximum/A2001001__Onset_Greenness_Maximum_masked.tif"
    val tmp_output = "/tmp/A2001001__Onset_Greenness_Maximum_masked.tif"
    GeoTiffWriter.write(geoTif, tmp_output)

    //Upload to HDFS
    var cmd = "hadoop dfs -copyFromLocal -f " + tmp_output + " " + output
    Process(cmd) !

    cmd = "rm -fr " + tmp_output
    Process(cmd) !

  }
}
