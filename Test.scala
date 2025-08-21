package com.citi.purgeFramework.utils

import com.citi.audit._
import com.citi.purgeFramework.driver.Driver.{audit_config, audit_int_flg, audit_param_dict, logger}
import com.citi.purgeFramework.utils.AuditIntegration.getAuditConfigDetails
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import com.fasterxml.jackson.databind.{DeserializationFeature, JsonNode, ObjectMapper}
import com.typesafe.config.ConfigException.Missing
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.commons.text.StringEscapeUtils
import org.apache.spark.sql.execution.datasources.json.JsonUtils
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import java.io.{File, FileInputStream, PrintWriter, StringWriter}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.Properties
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._

import java.text.SimpleDateFormat

object AuditCall extends AppLogger {

  def main(sparkargs: Array[String]): Unit = {

    //try {
      val onlyAuditCall = true
      val processDate: String = new SimpleDateFormat("yyyyMMdd").format(java.util.Calendar.getInstance().getTime)
      println("sparkargs: " + sparkargs.toList)
      val spark_args = sparkargs.map(_.split("=") match { case Array(x, y) => (x, y) }).toMap
      val user_config = ConfigFactory.parseFile(new File(spark_args("user_config").split("/").last))
      val env = spark_args("env") //Execution environment. Ex: Dev/uat/prod

      var err_mail_sub = try { user_config.getString("mail_config.err_mail_sub")} catch { case _: Missing => "" }
      val sent_to = user_config.getString("mail_config.send_to_".concat(env))
      var Input_Table_query = user_config.getString("Input.Input_table")
      var reference_table = user_config.getString("Input.Reference_table")
      var runAtATime = user_config.getString("Input.runAtATime")
      val Log_table_query = user_config.getString("Input.Log_table_query")
      var Customer_ID = user_config.getString("Input.Customer_Identification")
      var LKP_CUSTOMER_ID = user_config.getString("Input.LKP_CUSTOMER_ID")
      val LOG_table = user_config.getString("Input.LOG_table")
      var Join_Query = user_config.getString("Input.Join_Query")
      val extra_cols = try {
        user_config.getString("Input.extra_cols")
      } catch {
        case _: Exception =>
          ""
      }

      var Reference_Query = user_config.getString("Input.Reference_Query")

      var hdfs_for_temp_data = user_config.getString("log_paths.hdfs_path_for_temp_data")
      val Reconciliation_table = user_config.getString("Input.Reconciliation_table")
      var Input_table_bkp_path = user_config.getString("Input.Input_table_bkp_path")
      var jobApplicationName = user_config.getString("spark.jobApplicationName")

      val purge_status_table = "gcganamrswp_work.ccpa_purge_status"
      val purge_mode=spark_args("purge_mode")
      val CSI_ID=spark_args("CSI_ID")
      val ARG_GROUP_ID=spark_args("ARG_GROUP_ID")
      val ARG_TYPE_OF_PURGE=spark_args("ARG_TYPE_OF_PURGE")
      val ARG_PARTITION_NAME=spark_args("ARG_PARTITION_NAME")
      val LKP_DB_NAME=spark_args("LKP_DB_NAME")
      val LKP_TABLE_NAME=spark_args("LKP_TABLE_NAME")


      //val ARG_CUSTOMER_ID=${ARG_CUSTOMER_ID}
      val ARG_AGING=spark_args("ARG_AGING")
      val increment_column=spark_args("increment_column")
      val increment_datatype=spark_args("increment_datatype")
      val TARGET_TABLE_DATE_FORMAT=spark_args("ARG_TARGET_TABLE_DATE_FORMAT")
      val LH_IND=spark_args("LH_IND")

      val purge_date=spark_args("purge_date")
      val column_mapping=user_config.getString("Input.Column_mapping").split(",")
      var lits_override = try { spark_args("lits_override")} catch { case exception: Exception => "false" }


      val Identifier_Type = spark_args("Identifier_Type")
      val exit_code = spark_args("exit_code")
      val err_msg = spark_args("err_msg")
      val exit_status:String = if (exit_code.toInt == 0) "SUCCESS" else "FAILURE"

      // reading values from config file
      logger.info("Getting conf values from local file")
      //auditFrameworkConfig = conf.getString("auditFrameworkConfig").trim.split("/").last
      val spark = SparkSession.builder.appName(jobApplicationName)
        .config("spark.sql.autoBroadcastJoinThreshold","-1")
        .config("spark.default.parallelism","1")
        .config("spark.sql.shuffle.partitions","1")
        .enableHiveSupport()
        .getOrCreate()

    var Input_Table = "<ARG_DB_NAME>.<ARG_TABLE_NAME>"

    for ((k, v) <- spark_args) {
      Input_Table_query = Input_Table_query.replaceAll("<" + k + ">", v)
      Input_Table = Input_Table.replaceAll("<" + k + ">", v)
      reference_table = reference_table.replaceAll("<" + k + ">", v)
      Customer_ID = Customer_ID.replaceAll("<" + k + ">", v)
      LKP_CUSTOMER_ID = LKP_CUSTOMER_ID.replaceAll("<" + k + ">", v)
      jobApplicationName = jobApplicationName.replaceAll("<" + k + ">", v)
      err_mail_sub = err_mail_sub.replaceAll("<" + k + ">", v)
      Input_table_bkp_path = Input_table_bkp_path.replaceAll("<" + k + ">", v)
      hdfs_for_temp_data = hdfs_for_temp_data.replaceAll("<" + k + ">", v)
      runAtATime = runAtATime.replaceAll("<" + k + ">", v)
    }

      audit_param_dict = audit_param_dict + ("spark" -> spark, "appId" -> spark.sparkContext.applicationId, "sc" -> spark.sparkContext)


      var audit_int_flg = try {
        user_config.getString("Datalens_Integration_Details.Datalens_Integration_Flag").toLowerCase
      } catch {
        case _: Exception =>
          "false"
      }
       if (audit_int_flg == "true") {
         audit_config = if (user_config.hasPath("Datalens_Integration_Details.Datalens_user_config_".concat(env))) {
          user_config.getString("Datalens_Integration_Details.Datalens_user_config_".concat(env))
        } else if (user_config.hasPath("Datalens_Integration_Details.Datalens_user_config")) {
          user_config.getString("Datalens_Integration_Details.Datalens_user_config")
        } else {
          ""
        }
      }
      audit_param_dict = audit_param_dict + ("audit_config" -> audit_config, "env" -> env, "sent_to" -> sent_to, "lits_override" -> lits_override, "ARG_PARTITION_NAME" -> ARG_PARTITION_NAME,
        "increment_column" -> increment_column, "increment_datatype" -> increment_datatype, "TARGET_TABLE_DATE_FORMAT" -> TARGET_TABLE_DATE_FORMAT, "LH_IND" -> LH_IND, "lits_purge_dt" -> purge_date)
      audit_param_dict = audit_param_dict+("input_sql"-> Input_Table_query,"target_table" -> Input_Table, "reference_table" -> reference_table, "Customer_ID" -> Customer_ID , "LKP_CUSTOMER_ID" -> LKP_CUSTOMER_ID, "LKP_DB_NAME" -> LKP_DB_NAME, "LKP_TABLE_NAME" -> LKP_TABLE_NAME)


      if (audit_int_flg == "true") {
        val audit_int_flg_returned = getAuditConfigDetails("user_config")
        audit_int_flg = audit_int_flg_returned
        //audit_config = audit_config_path_returned
      }
      else {// minimal audit integration only when country is nam. To prevent failures in APAC
        logger.info("Datalens integration flag is false proceeding with minimal integration")
        audit_param_dict = audit_param_dict + ("audit_config" -> "")
        audit_int_flg = "true"
        AuditIntegration.audit_start_call("true")
      }

      logger.info("After audit start call ")
      AuditIntegration.audit_final_call(exit_code, exit_status, err_msg)
    /*} catch {
      case ex: Exception =>
        logger.error(""" Error in Audit call """)
        logger.error(ex.getStackTrace.mkString("\n"))
        logger.error(ex.getMessage)
    }

     */
  }
}
