package zjx.recommendsystem.common.transformer

import java.lang.reflect.Field

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializerFeature
import org.apache.commons.lang3.StringUtils
import org.apache.kudu.client.{KuduClient, KuduSession, KuduTable, PartialRow}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{ForeachWriter, Row}
import zjx.recommendsystem.common.constant.ConfigConstant
import zjx.recommendsystem.model.MyFirstTable

import scala.reflect._
import scala.reflect.runtime.universe._

/**
  * 项目名称：data-application
  * 类 名 称：KuduForeachWriter
  * 类 描 述：kudu写入转换器
  * 创建时间：2019/8/6 11:52
  * 创 建 人：jiaxin.zhang
  */
/**
  * kudu写入转换类
  *
  * @param kuduTableName 写入kudu表名
  * @param kuduMaster    kudu地址
  */
class KuduForeachWriter[T: ClassTag](val kuduTableName: String, val kuduMaster: String = ConfigConstant.kuduMaster)
  extends ForeachWriter[Row] with Logging {

  var client: KuduClient = _
  var kuduSession: KuduSession = _
  var kuduTable: KuduTable = _
  var clazz: Class[_] = _

  def dfRow2kuduRow(record: Row, row: PartialRow):Unit = {
    clazz.getDeclaredFields.foreach(field => {
      rocord2row(record, field, row)
    })
  }

  /**
    * df行转kudu行转换方法
    *
    * @param record df 对应一行记录
    * @param field 字段信息
    * @param row kudu对应一行记录
    */
  def rocord2row(record: Row, field: Field, row: PartialRow): Unit = {
    val fieldTypeStr = field.getType.getSimpleName
    val fieldName = field.getName
    fieldTypeStr match {
      case "String" =>
        val col = record.getAs[String](fieldName)
        if (StringUtils.isEmpty(col)) row.setNull(fieldName)
        else row.addString(fieldName, col)

      case "int" | "Integer" =>
        val col = record.getAs[Integer](fieldName)
        if (StringUtils.isEmpty(col.toString)) row.setNull(fieldName)
        else row.addInt(fieldName, col)

      case "long" | "Long" =>
        val col = record.getAs[Double](fieldName)
        if (StringUtils.isEmpty(col.toString)) row.setNull(fieldName)
        else row.addDouble(fieldName, col)

      case "float" | "Float" =>
        val col = record.getAs[Float](fieldName)
        if (StringUtils.isEmpty(col.toString)) row.setNull(fieldName)
        else row.addFloat(fieldName, col)

      case "double" | "Double" =>
        val col = record.getAs[Double](fieldName)
        if (StringUtils.isEmpty(col.toString)) row.setNull(fieldName)
        else row.addDouble(fieldName, col)

      case "boolean" | "Boolean" =>
        val col = record.getAs[Boolean](fieldName)
        if (StringUtils.isEmpty(col.toString)) row.setNull(fieldName)
        else row.addBoolean(fieldName, col)

      case _ =>
        logInfo("df row 2 kudu row>>>>>>>> 未知类型转换")
    }
  }

  def open(partitionId: Long, version: Long): Boolean = {
    this.client = new KuduClient.KuduClientBuilder(kuduMaster).build()
    this.kuduTable = client.openTable(kuduTableName)
    this.kuduSession = client.newSession()
    this.clazz = classTag[T].runtimeClass
    true
  }

  def process(record: Row): Unit = {
    val upsert = this.kuduTable.newUpsert()
    val row: PartialRow = upsert.getRow()
    dfRow2kuduRow(record, row)
    this.kuduSession.apply(upsert)
  }

  def close(errorOrNull: Throwable): Unit = {
    this.kuduSession.close()
    this.client.shutdown()
  }

  def getTypeTag[T: TypeTag]() = typeTag[T]

  def getClassTag[T: ClassTag]() = classTag[T]

  def getType[T]()(implicit tag: TypeTag[T]): Type = typeOf[T]

  def getFields(clazz: Class[_]) = {
    clazz.getDeclaredFields.foreach(field => {
      println(field.getName, field.getType, field.toString)
    })
  }

  def main(args: Array[String]): Unit = {
    println(getTypeTag[MyFirstTable]())
    println(getClassTag[MyFirstTable]().runtimeClass)
    println(getClassTag[MyFirstTable]().runtimeClass.getDeclaredFields.foreach(field => {
      println(field.getName, field.getType.getSimpleName, field.toString)
    }))
    println(getType[MyFirstTable]())
    println(JSON.toJSONString(new MyFirstTable("aa", 1), SerializerFeature.WriteMapNullValue))
    //    println(getFields(Borrower.getClass))
    //    println(getFields(new Borrower("11",22,List()).getClass))
    //    new Borrower("11",22,List())
  }
}



