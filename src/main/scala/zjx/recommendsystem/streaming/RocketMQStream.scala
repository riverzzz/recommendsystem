//package zjx.recommendsystem.streaming
//
//import org.apache.rocketmq.spark.RocketMQConfig
//import org.apache.spark.internal.Logging
//import org.apache.spark.sql.functions._
//import org.apache.spark.sql.{SaveMode, SparkSession}
//import org.apache.spark.streaming.api.java.JavaStreamingContext
//import zjx.recommendsystem.common.constant.ConfigConstant
//import zjx.recommendsystem.common.util.SparkRocketMQStreamUtils
//
//import scala.util.Try
//
///**
//  * 项目名称：data-application
//  * 类 名 称：RocketMQStreamTest
//  * 类 描 述：spark streaming 处理 rocketmq流数据
//  * 创建时间：2019/7/12 15:37
//  * 创 建 人：jiaxin.zhang
//  */
//object RocketMQStream extends Logging {
//
//  /**
//    * 处理rocketmq风控进件数据
//    *
//    * @param ssc
//    */
//  def loanInfoStream(ssc: JavaStreamingContext): Unit = {
//    val loanInfoStream = SparkRocketMQStreamUtils.createJavaReliableMQPushStream(ssc, ProjectConstant.GROUP_LOAN_INFO_PER_APPLY, ProjectConstant.TOPIC_LOAN_INFO_PER_APPLY, RocketMQConfig.CONSUMER_OFFSET_LATEST)
//    loanInfoStream.dstream.map(msg => Try(new String(msg.getBody)).get).foreachRDD(rdd => {
//      if (!rdd.isEmpty()) {
//        val spark = SparkSession.builder.config(rdd.sparkContext.getConf).getOrCreate()
//        spark.sql(ProjectConstant.HIVE_DATABASE)
//        import spark.implicits._
//        val otherLoanInfoDS = spark.createDataset(rdd)
//        val zipBodyInfo = spark.read.json(otherLoanInfoDS).map(row => Try(unzip(row.getAs[String]("zipBody"))).get)
//        val otherLoanInfo = spark.read.json(zipBodyInfo).cache()
//        otherLoanInfo.show()
//        logInfo(s"json结构>>>>>${otherLoanInfo.schema.printTreeString()}")
//        otherLoanInfo.write.mode("append").parquet(s"${ConfigConstant.hdfsUrlRocketMQ}/${ProjectConstant.TOPIC_LOAN_INFO_PER_APPLY}")
//
//        val loanInfoFilter = otherLoanInfo.filter("interfaceName='LOAN_INFO_PER_APPLY'").cache()
//        if (!loanInfoFilter.rdd.isEmpty()) {
//          loanInfoFilter.select($"request.borrower.id_no".alias("borrower_id_no"), $"request.borrower.*").write.mode(SaveMode.Append).saveAsTable("ods_borrower")
//          loanInfoFilter.select($"request.borrower.id_no".alias("borrower_id_no"), explode($"request.relational_humans")).select($"borrower_id_no", $"col.*")
//            .write.mode(SaveMode.Append).saveAsTable("ods_relational_human")
//          loanInfoFilter.select($"request.borrower.id_no".alias("borrower_id_no"), $"request.repayment_account.*").write.mode(SaveMode.Append).saveAsTable("ods_repayment_account")
//        }
//
//        otherLoanInfo.unpersist()
//        loanInfoFilter.unpersist()
//      }
//    })
//  }
//
//  def main(args: Array[String]): Unit = {
//    val json =
//      """{
//                 	"request": {
//                 		"company": null,
//                 		"partner": "0001",
//                 		"service": "LOAN_INFO_PER_APPLY",
//                 		"borrower": {
//                 			"age": 24,
//                 			"sex": null,
//                 			"area": "这是详细地址",
//                 			"city": "北京市",
//                 			"name": "梁晨",
//                 			"id_no": "142702199407270123",
//                 			"address": "山西省永济市中山西街15号北43-6-9",
//                 			"backUrl": null,
//                 			"id_type": "I",
//                 			"open_id": "SC190713000008",
//                 			"frontUrl": null,
//                 			"incomeM3": null,
//                 			"incomeM6": null,
//                 			"industry": "Z",
//                 			"province": "北京市",
//                 			"IncomeM12": null,
//                 			"education": "C",
//                 			"have_house": "Null",
//                 			"family_worth": null,
//                 			"housing_area": null,
//                 			"mobile_phone": "13267107695",
//                 			"annual_income": null,
//                 			"housing_value": null,
//                 			"privateOwners": "Null",
//                 			"marital_statusEnum": "O"
//                 		},
//                 		"guaranties": null,
//                 		"product_no": "001503",
//                 		"request_no": "a40ced2958e344a0b669d2f8ecc737a1",
//                 		"contract_no": "122131321",
//                 		"loan_account": {
//                 			"bank_name": "B0307",
//                 			"account_num": "6214832167417770",
//                 			"branch_name": "核心你赶紧改掉",
//                 			"account_type": "PERSONAL",
//                 			"mobile_phone": "13924382457"
//                 		},
//                 		"currency_type": "RMB",
//                 		"schedule_base": {
//                 			"terms": 6,
//                 			"loan_date": "2019-07-15",
//                 			"loan_rate": 0.1668,
//                 			"repay_type": "RT01",
//                 			"loan_end_date": "2020-01-15",
//                 			"deduction_date": 15,
//                 			"year_rate_base": 360,
//                 			"repay_frequency": "MONTH"
//                 		},
//                 		"loan_apply_use": "LAU99",
//                 		"loan_rate_type": "LRT01",
//                 		"contract_amount": 35000,
//                 		"service_version": "V1.0",
//                 		"company_loan_bool": false,
//                 		"relational_humans": [{
//                 			"age": null,
//                 			"sex": null,
//                 			"area": null,
//                 			"city": null,
//                 			"name": "两个",
//                 			"id_no": null,
//                 			"address": null,
//                 			"id_type": null,
//                 			"province": null,
//                 			"sequence": 1,
//                 			"mobile_phone": "13467348461",
//                 			"relationship": "O",
//                 			"hdTwoEleResult": null,
//                 			"relational_human_type": "RHT01"
//                 		}, {
//                 			"age": null,
//                 			"sex": null,
//                 			"area": null,
//                 			"city": null,
//                 			"name": "力气",
//                 			"id_no": null,
//                 			"address": null,
//                 			"id_type": null,
//                 			"province": null,
//                 			"sequence": 2,
//                 			"mobile_phone": "13864673484",
//                 			"relationship": "O",
//                 			"hdTwoEleResult": null,
//                 			"relational_human_type": "RHT01"
//                 		}, {
//                 			"age": null,
//                 			"sex": null,
//                 			"area": null,
//                 			"city": null,
//                 			"name": "明个",
//                 			"id_no": null,
//                 			"address": null,
//                 			"id_type": null,
//                 			"province": null,
//                 			"sequence": 3,
//                 			"mobile_phone": "13934972481",
//                 			"relationship": "O",
//                 			"hdTwoEleResult": null,
//                 			"relational_human_type": "RHT01"
//                 		}],
//                 		"repayment_account": {
//                 			"bank_name": "B0307",
//                 			"account_num": "6214832167417770",
//                 			"branch_name": "核心你赶紧改掉",
//                 			"account_type": "PERSONAL",
//                 			"mobile_phone": "13924382457"
//                 		}
//                 	},
//                 	"agencyId": "1502",
//                 	"response": {
//                 		"partner": "0001",
//                 		"service": "LOAN_INFO_PER_APPLY",
//                 		"result_msg": "请求成功",
//                 		"contract_no": "a40ced2958e344a0b669d2f8ecc737a1",
//                 		"result_code": "000000",
//                 		"acct_setup_ind": "Y",
//                 		"service_version": "V1.0",
//                 		"apply_request_no": "a40ced2958e344a0b669d2f8ecc737a1"
//                 	},
//                 	"projectId": "cl0021",
//                 	"interfaceName": "LOAN_INFO_PER_APPLY"
//                 }"""
//    val spark = SparkSession
//      .builder()
//      .master("local[*]")
//      .appName("test")
//      .getOrCreate()
//
//    import spark.implicits._
//    val otherLoanInfoDS = spark.createDataset(json :: Nil)
//    val otherLoanInfo = spark.read.json(otherLoanInfoDS)
//
//    val fil = otherLoanInfo
//      .filter("request.service='LOAN_INFO_PER_APPLY'")
//      .select($"request.borrower.id_no".alias("borrower_id_no"), $"request.repayment_account.*")
//    fil.show()
//    logInfo(s"count>>>>>>>${fil.count()}")
//    fil.schema.printTreeString()
//  }
//}
