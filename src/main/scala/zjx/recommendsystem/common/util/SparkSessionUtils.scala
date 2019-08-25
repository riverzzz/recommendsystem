package zjx.recommendsystem.common.util

import java.io.File

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.api.java.JavaStreamingContext
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 项目名称：data-application
  * 类 名 称：SparkSessionUtils
  * 类 描 述：sparkSession工具类
  * 创建时间：2019/7/16 15:22
  * 创 建 人：jiaxin.zhang
  */
object SparkSessionUtils {

  def createHiveSparkSession(appName: String): SparkSession = {
    val warehouseLocation = new File("spark-warehouse").getAbsolutePath
    val conf = new SparkConf()
    conf.set("spark.sql.warehouse.dir", warehouseLocation)
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val spark = SparkSession
      .builder()
      .appName(appName)
      .config(conf)
      .enableHiveSupport()
      .getOrCreate()
    spark
  }

  def createJavaStreamingContext(appName: String, batchDuration: Int): JavaStreamingContext = {
    val sparkSession = createHiveSparkSession(appName)
    sparkSession.conf.set("spark.streaming.stopGracefullyOnShutdown", "true")//设置stop时回调关闭
    val ssc = new JavaStreamingContext(sparkSession.sparkContext, Seconds(batchDuration))
    ssc
  }

  def createStreamingContext(appName: String, batchDuration: Int): StreamingContext = {
    val sparkSession = createHiveSparkSession(appName)
    sparkSession.conf.set("spark.streaming.stopGracefullyOnShutdown", "true")//设置stop时回调关闭
    val sc = new StreamingContext(sparkSession.sparkContext, Seconds(batchDuration))
    sc
  }

}
