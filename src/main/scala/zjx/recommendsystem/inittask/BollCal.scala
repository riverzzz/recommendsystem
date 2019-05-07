package zjx.recommendsystem.inittask


import java.sql.Timestamp

import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.math._

/**
  * @title: BollCal
  * @projectName recommendsystem
  * @description: 参数为n的布林线计算
  * @author zjx
  * @date 19-4-25下午12:30
  */
object BollCal {

  lazy val log = org.apache.log4j.LogManager.getLogger("BollCal")

  /**
    * 跨度为day_range的布林线计算
    *
    * @param day_range 天跨度
    */
  def bollCalculate(day_range: Int, stockInfo: DataFrame): Unit = {
    import stockInfo.sparkSession.implicits._

    val stockBoll = stockInfo.groupByKey(stock => stock.getAs[Long]("stock_id"))
      .flatMapGroups((stockId, stockInfoList) => {
        //每只股票根据时间排序
        val stockInfoArray = stockInfoList.toArray
          .sortBy(stock => stock.getAs[Timestamp]("timestamp").getTime)

        val bollArray = stockInfoArray.indices.map(index => {
          val timestamp = stockInfoArray(index).getAs[Timestamp]("timestamp")
          val closePrice = stockInfoArray(index).getAs[Double]("close_price")
          val bollInfo = if (index == 0) { //当index=0时，每只股票只有一条数据，约定mb=-1时，代表mb没有值
            (stockId, timestamp, closePrice, closePrice, -1.0, closePrice, closePrice)
          } else if (0 < index && index <= day_range) { //当0<index<=day_range时
            //此时，每只股票有index+1条数据
            val closePriceArray = stockInfoArray.slice(0, index + 1)
              .map(stock => stock.getAs[Double]("close_price"))
            val sum = closePriceArray.sum //计算收盘价总和和总数据条数
            val ma = sum / (index + 1).toDouble //计算ma
            val mb = (sum - closePrice) / index.toDouble //计算mb
            val se = closePriceArray.reduce((a, b) => pow(a - ma, 2) + pow(b - ma, 2)) //计算平方差
            val md = sqrt(se / (index + 1).toDouble) //计算md
            val up = ma + 2 * md //计算up
            val dn = ma - 2 * md //计算dn
            (stockId, timestamp, closePrice, ma, mb, up, dn)
          } else { //当index>day_range时
            //每只股票从当前index开始，取day_range条数据
            val closePriceArray = stockInfoArray.slice(index + 1 - day_range, index + 1)
              .map(stock => stock.getAs[Double]("close_price"))
            val sum = closePriceArray.sum
            val ma = sum / day_range.toDouble
            val mb = (sum - closePrice + stockInfoArray(index - day_range).getAs[Double]("close_price")) / day_range.toDouble
            val se = closePriceArray.reduce((a, b) => pow(a - ma, 2) + pow(b - ma, 2))
            val md = sqrt(se / day_range.toDouble)
            val up = ma + 2 * md
            val dn = ma - 2 * md
            (stockId, timestamp, closePrice, ma, mb, up, dn)
          }
          bollInfo
        })
        bollArray
      }).toDF("stock_id", "timestamp", "close_price", "ma", "mb", "up", "dn")

    stockBoll.show()
  }


  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("bollcal")
      .enableHiveSupport()
      .getOrCreate()
    //    spark.sql("use hive_namespace")
    //    val stockInfo = spark.sql("select stock_id,close_price,timestamp from table")
    val seq = Seq((1L, 1.0, new Timestamp(1556121600000L)), (1L, 2.0, new Timestamp(1556035200000L)), (1L, 3.0, new Timestamp(1555948800000L)),
      (2L, 3.0, new Timestamp(1556121600000L)), (2L, 6.0, new Timestamp(1555862400000L)), (2L, 2.0, new Timestamp(1555948800000L)))
    val stockInfo = spark.createDataFrame(seq).toDF("stock_id", "close_price", "timestamp")
    bollCalculate(20, stockInfo)
  }

}



