import geotrellis.proj4.CRS
import geotrellis.raster.io.geotiff.compression.{DeflateCompression, LZWDecompressor}
import geotrellis.raster.io.geotiff.writer.GeoTiffWriter
import geotrellis.raster.io.geotiff.{SinglebandGeoTiff, _}
import geotrellis.raster.{CellType, DoubleArrayTile, UByteCellType}
import geotrellis.spark.io.hadoop._
import geotrellis.vector.{Extent, ProjectedExtent}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.compress.Compressor
import org.apache.spark.input.PortableDataStream
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import spire.syntax.cfor._

object load_and_save_GeoTiff extends App {

  override def main(args: Array[String]): Unit = {
    val appName = this.getClass.getName
    val masterURL = "spark://emma0.emma.nlesc.nl:7077"

    val sc = new SparkContext(new SparkConf().setAppName(appName).setMaster(masterURL))
    //val band_count = geotrellis.raster.io.geotiff.reader.TiffTagsReader.read(filepath).bandCount;
    val band_count = 1;
    var projected_extent = new ProjectedExtent(new Extent(0,0,0,0), CRS.fromName("EPSG:3857"))
    var num_cols_rows :(Int, Int) = (0, 0)
    var band_RDD: RDD[Array[Double]] = sc.emptyRDD
    var band_vec: RDD[Vector] = sc.emptyRDD
    var band0: RDD[(Long, Double)] = sc.emptyRDD
    var band0_index: RDD[Long] = sc.emptyRDD
    var cellT :CellType = UByteCellType// CellType..fromAwtType(1)//.fromName("Double")
    val pattern: String = "2.tif"
    var filepath: String = ""
    if (band_count == 1) {
      //Single band GeoTiff
      filepath = "hdfs:///user/hadoop/spring-index/LastFreeze/1980"
    } else {
      //Multi band GeoTiff
      filepath = "hdfs:///user/hadoop/spring-index/BloomFinal/1980"
    }

    if (band_count == 1) {
      //Lets load a Singleband GeoTiff and return RDD just with the tiles.
      //Since it is a single GeoTiff, it will be a RDD with a tile.
      val tiles_RDD = sc.hadoopGeoTiffRDD(filepath).values
      val bands_RDD = tiles_RDD.map(m => m.toArrayDouble())

      val extents_withIndex = sc.hadoopGeoTiffRDD(filepath, pattern).keys.zipWithIndex().map{case (e,v) => (v,e)}
      projected_extent = (extents_withIndex.filter(m => m._1 == 0).values.collect())(0)

      val tiles_withIndex = tiles_RDD.zipWithIndex().map{case (e,v) => (v,e)}
      //num_cols_rows = (tiles_withIndex.lookup(0).apply(0).cols, tiles_withIndex.lookup(0).apply(0).rows)
      val tile0 = (tiles_withIndex.filter(m => m._1==0).values.collect())(0)
      num_cols_rows = (tile0.cols,tile0.rows)
      cellT = tile0.cellType

      //Get Index for Cells
      val bands_withIndex = bands_RDD.zipWithIndex().map { case (e, v) => (v, e) }
      //band0_index = bands_withIndex.lookup(0).apply(0).zipWithIndex.filter{ case (v, i) => !v.isNaN }.map { case (v, i) => (i) }
      //val band0_index = bands_withIndex.lookup(0).apply(0).zipWithIndex.filter(m => !m._1.isNaN).take(sample).map { case (v, i) => (i) }
      band0_index = bands_withIndex.filter(m => m._1 == 0).values.flatMap(m => m).zipWithIndex.filter(m => !m._1.isNaN).map { case (v, i) => (i) }

      //Get Array[Double] of a Title to later store the cluster ids.
      //band0 = sc.parallelize(bands_withIndex.lookup(0).take(1)).flatMap( m => m).zipWithIndex.map{case (v,i) => (i,v)}
      band0 = bands_withIndex.filter(m => m._1 == 0).values.flatMap( m => m).zipWithIndex.map{case (v,i) => (i,v)}

      //Lets filter out NaN
      band_RDD = bands_RDD.map(m => m.filter(!_.isNaN))
    } else {
      //Lets load a Multiband GeoTiff and return RDD just with the tiles.
      //Since it is a multi-band GeoTiff, we will take band 4
      val tiles_RDD = sc.hadoopMultibandGeoTiffRDD(filepath).values
      val bands_RDD = tiles_RDD.map(m => m.band(3).toArrayDouble())

      val extents_withIndex = sc.hadoopGeoTiffRDD(filepath, pattern).keys.zipWithIndex().map{case (e,v) => (v,e)}
      projected_extent = (extents_withIndex.filter(m => m._1 == 0).values.collect())(0)

      val tiles_withIndex = tiles_RDD.zipWithIndex().map{case (e,v) => (v,e)}
      val tile0 = (tiles_withIndex.filter(m => m._1==0).values.collect())(0)
      num_cols_rows = (tile0.cols,tile0.rows)

      //Get Index for Cells
      val bands_withIndex = bands_RDD.zipWithIndex().map { case (e, v) => (v, e) }
      //band0_index = bands_withIndex.lookup(0).apply(0).zipWithIndex.filter { case (v, i) => !v.isNaN }.take(sample).map { case (v, i) => (i) }
      band0_index = bands_withIndex.filter(m => m._1 == 0).values.flatMap(m => m).zipWithIndex.filter(m => !m._1.isNaN).map { case (v, i) => (i) }

      //Get Array[Double] of a Title to later store the cluster ids.
      band0 = bands_withIndex.filter(m => m._1 == 0).values.flatMap( m => m).zipWithIndex.map{case (v,i) => (i,v)}

      //Let's filter out NaN
      band_RDD = bands_RDD.map(m => m.filter(v => !v.isNaN))
    }
    /*
     Create a GeoTiff and save to HDFS.
    */

    val cluster_tile = DoubleArrayTile(band0.values.collect(), num_cols_rows._1, num_cols_rows._2)
    val geoTiff = SinglebandGeoTiff(cluster_tile, projected_extent.extent, projected_extent.crs, Tags.empty, GeoTiffOptions.DEFAULT)
    val test = new SinglebandGeoTiff(cluster_tile, projected_extent.extent, projected_extent.crs, Tags.empty, GeoTiffOptions.DEFAULT)

    //sc.parallelize(GeoTiffWriter.write(geoTiff)).saveAsObjectFile("hdfs:///users/emma/spring-index/BloomFinal/clusters_3.tif")
    GeoTiffWriter.write(geoTiff, "/tmp/test3.tif")

    val expected = geotrellis.raster.ArrayTile.empty(cellT, num_cols_rows._1, num_cols_rows._2)
    cfor(0)(_ < num_cols_rows._1, _ + 1) { col =>
      cfor(0)(_ < num_cols_rows._2, _ + 1) { row =>
        val v = cluster_tile.get(col, row)
        expected.setDouble(col, row, v)
      }
    }

    val geoTif = SinglebandGeoTiff(expected, projected_extent.extent, projected_extent.crs, Tags.empty, GeoTiffOptions.DEFAULT)
    val geoTif_Comp = DeflateCompression.createCompressor(1)
    val geoTif_C = geoTif_Comp.compress(GeoTiffWriter.write(geoTif),1)

    GeoTiffWriter.write(geoTif_C, "/tmp/test3.tif")

    val file = "hdfs:///users/emma/spring-index/BloomFinal/clusters.tif"
    val path = new Path(file)

    val output = sc.objectFile("hdfs:///users/emma/spring-index/BloomFinal/clusters.tif")
    val arry = GeoTiffWriter.write(geoTif)
    val ss = new PortableDataStream(arry)
    val biFile = sc.binaryFiles(file)
    val ff =   biFile.take(1)
    val ss  = sc.objectFile(file).collect()

      sc.parallelize(arry).saveAsTextFile("hdfs:///users/emma/spring-index/BloomFinal/clusters_3.tif")

  }
}