import geotrellis.raster.resample.Bilinear
import geotrellis.raster.{MultibandTile, Tile}
import geotrellis.spark.{SpatialKey, TileLayerMetadata}
import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.spark.io.hadoop._
import geotrellis.spark.render.Render
import geotrellis.spark.tiling.{FloatingLayoutScheme, Tiler}
import geotrellis.vector.ProjectedExtent
import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext, rdd}

object geoTrellis_multiTiffs_render extends App {

  override def main(args: Array[String]): Unit = {
    val appName = this.getClass.getName
    val masterURL="spark://emma0.emma.nlesc.nl:7077"

    val sc = new SparkContext(new SparkConf().setAppName(appName).setMaster(masterURL))
    //val band_count = geotrellis.raster.io.geotiff.reader.TiffTagsReader.read(filepath).bandCount;
    val band_count = 4;
    var singBands_RDD :RDD[(ProjectedExtent, Tile)] = sc.emptyRDD
    var multiBands_RDD :RDD[(ProjectedExtent, MultibandTile)] = sc.emptyRDD
    var band_RDD :RDD[(ProjectedExtent, Array[Double])] = sc.emptyRDD
    var filepath :String = ""
    if (band_count == 1)
      filepath = "hdfs:///user/hadoop/spring-index/LastFreeze/1980.tif"
    else
      filepath = "hdfs:///user/hadoop/spring-index/BloomFinal/1980.tif"

    if (band_count == 1) {
      singBands_RDD = sc.hadoopGeoTiffRDD(filepath)

      val layoutScheme = FloatingLayoutScheme(512)
      val (_: Int, metadata: TileLayerMetadata[SpatialKey]) = singBands_RDD.collectMetadata[SpatialKey](layoutScheme)

      // Here we set some options for our tiling.
      // For this example, we will set the target partitioner to one
      // that has the same number of partitions as our original RDD.
      val tilerOptions =
      Tiler.Options(
        resampleMethod = Bilinear,
        partitioner = new HashPartitioner(singBands_RDD.partitions.length)
      )

      metadata.extent.

      singBands_RDD.
      Render.renderGeoTiff(singBands_RDD)
    } else {
      multiBands_RDD = sc.hadoopMultibandGeoTiffRDD(filepath)
      Render.renderGeoTiff(multiBands_RDD)
    }


  }
}
