package zjx.recommendsystem.model

/**
  * 项目名称：data-application
  * 类 名 称：KuduModel
  * 类 描 述：kudumodel
  * 创建时间：2019/8/5 11:31
  * 创 建 人：jiaxin.zhang
  */

/**
  * kudu test model
  * @param value value
  * @param count count
  */
case class MyFirstTable(
                         value: String,
                         count: Int
                       ) {

}

object MyFirstTable {
  /** ------------ kudu tablename ------------ **/
  val table_name = "impala::default.my_first_table"
  val pri_key = Seq("value")
}