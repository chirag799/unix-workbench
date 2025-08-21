
package com.citi.purgeFramework.utils
import com.citi.audit._
import com.citi.purgeFramework.driver.Driver.{audit_config, audit_param_dict}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import com.fasterxml.jackson.databind.{DeserializationFeature, JsonNode, ObjectMapper}
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


object AuditIntegration extends AppLogger {
  var autosys_job_name = ""
  var trace_uuid=""
  def setReqAuditDetails(spark: SparkSession , appId:String): Unit = {

    // "minimal_details" is JSON which contains - autosys job name, trce_uuid, pipelineid,stdout file and stderr file
    val minimal_details = sys.env.getOrElse("minimal_details", "")
    val mapper = new ObjectMapper()
    val actualObj = mapper.readTree(minimal_details)
    autosys_job_name = actualObj.get("Autosys_job_name").asText
    trace_uuid = actualObj.get("uuid").asText

    System.setProperty("logger_autosys_job_name", autosys_job_name)
    System.setProperty("logger_uuid", trace_uuid)
    System.setProperty("logger_appid", appId)

    // Initialize MDC context once for the entire application
    setMDCContext(spark)

  }

  def getAuditConfigDetails(user_config: String): String = {

    var audit_int_flg = "true"
    val sndEmail = new SendEmail


    // read the audit user config tag from dq user config.
    /*var audit_config = try {
     user_config.getString("Datalens_Integration_Details.Datalens_user_config_".concat(env))
   } catch {
     case _: Exception => ""
   }*/

    logger.info("Audit config name: " + audit_config)
    audit_param_dict = audit_param_dict + ("audit_config" -> audit_config)
    if (audit_config != "") {
      AuditIntegration.audit_start_call("false")
    }
    else {
      audit_int_flg = "false"
      logger.info("Datalens integration flag in DQ user config file is true but Datalens user config file is not present")
    }
    return audit_int_flg

  }

  def audit_start_call(minimal_integration_flag: String): Unit = {
    logger.info("Before audit start call")
    /* Access the Map from QualityCheckMain class. In this Map all key value are present that are req to make audit first and final call
      keys present is DataCheckImplementation.audit_param_dict Map =>
      audit_req_param("audit_config"),
      audit_req_param("env"),
      audit_req_param("dq_error_sent_to"),
      audit_req_param("framework_config")
     */
    try {

      // Get the framework config and cast it to config to get the required field values from it

      val spark = audit_param_dict.getOrElse("spark", null).asInstanceOf[SparkSession]

      var param_dict = scala.collection.immutable.Map[String, Any]()
      var mapping_keys = scala.collection.immutable.Map[String, String]().empty
      param_dict = Map("edge_node_user_config_file" -> audit_param_dict.getOrElse("audit_config","").toString,
        "env" -> audit_param_dict("env").toString,
        "audit_email" -> audit_param_dict("sent_to").toString,
        "spark" -> spark
      )
      if(minimal_integration_flag == "true") {
        val minimal_details = sys.env.getOrElse("minimal_details", "")
        logger.info("minimal_details: " + minimal_details)
        val mapper = new ObjectMapper()
        val actualObj = mapper.readTree(minimal_details)
        //need to check with paras
        param_dict += ("minimal_integ_val" -> Map("Autosys_job_name" -> actualObj.get("Autosys_job_name").asText()))

        mapping_keys += (
          "logFileStderr" -> actualObj.get("logFileStderr").asText(),
          "logFileStdout" -> actualObj.get("logFileStdout").asText(),
          "jobExecType" -> "Purge")
      } else {
        param_dict += ("minimal_integ_val" -> Map("job_exec_type" -> "Purge"))
      }
      //val mapping_keys = Map[String, String]().empty
      logger.info("Param dict in start call: "+param_dict.mkString)
      AuditFrameworkDriver.startJob(param_dict, mapping_keys,minimal_integration_flag)
      logger.info("After audit start call ")
    }
    catch {
      case e: Exception =>
        val writer = new StringWriter();
        e.printStackTrace(new PrintWriter(writer));
        val stackTrace = writer.toString();
        logger.error("Error occurred while making audit start call from DQFIRST EXCEPTION1: "+ stackTrace)
    }
  }


  def audit_final_call(exit_code: String, job_status: String, error_str: String, e: Throwable = null): Unit = {
    logger.info("Before audit final call")
    // Befor making audit final call some more key value pairs has been added which are req for making final call
    // Now keys present are
    /*
    audit_req_param("audit_config"),
    audit_req_param("env"),
    audit_req_param("dq_error_sent_to"),
    audit_req_param("framework_config")
    audit_req_param("appId")
    audit_req_param("spark")
    audit_req_param("sc")
     */
    try {
      var error_msg = "" // occured while executing DQ. Please check yarn logs"
      if (e != null) {
        var writer = new StringWriter();
        e.printStackTrace(new PrintWriter(writer))
        error_msg = writer.toString//StringEscapeUtils.escapeJava(writer.toString)
        logger.info(" Inside audit final call - exception message: " + error_msg)
      }
      else {
        // In certain cases script is intentionally killed ex: Hive Insert Failures
        // In such cases you will get only error message. below line will escape the error message
        if (error_str != null && error_str != "") {
          error_msg = error_str//StringEscapeUtils.escapeJava(error_str)
          logger.info("Inside audit final call - error message: " + error_msg)
        }
      }
      var audit_req_param = audit_param_dict
      var spark_args = audit_req_param.getOrElse("other_user_provided_arguments", Map[String, String]().empty).asInstanceOf[Map[String, String]]
      // spark args contans the list of run time arguments passed while running script.
      spark_args = spark_args.-("pipeline_id")
      val dataset_name = audit_req_param.getOrElse("target_table", "").toString
      var database_name = ""
      var table_name = ""
      if(dataset_name !="" && dataset_name.contains(".")) {
        database_name = dataset_name.split("\\.")(0)
        table_name = dataset_name.split("\\.")(1)
      }

      var mapping_keys = Map("frameworkName" -> "CPRA_Purge_framework",
        "frameworkVersion" -> "V2.0.1",
        "litsPurgeDt" -> audit_req_param.getOrElse("lits_purge_dt", "").toString,
        "LegalHoldInd" -> audit_req_param.getOrElse("LH_IND", "").toString,
        //"target_table" -> audit_req_param.getOrElse("target_table", 0).toString,
        "tgtTblSql" -> StringEscapeUtils.escapeJava(audit_req_param.getOrElse("input_sql", "").toString).replace("\"","").replace("\\",""),
        "lkpTblRecCnt" -> audit_req_param.getOrElse("ref_tbl_total_count", "").toString,
        "tgtTblInitialRecCnt" -> audit_req_param.getOrElse("tgt_tbl_initial_count", "").toString,
        "tgtTblPurgeRecCnt" -> audit_req_param.getOrElse("tgt_tbl_purge_count", "").toString,
        "tgtTblFinalCount" -> audit_req_param.getOrElse("tgt_tbl_final_count", "").toString,
        "databaseName" -> database_name,
        "tableName" -> table_name,
          "litsOverride" -> audit_req_param.getOrElse("lits_override", "").toString,
        "lkpDbNAME" -> audit_req_param.getOrElse("LKP_DB_NAME", "").toString,
        "lkpTblName" -> audit_req_param.getOrElse("LKP_TABLE_NAME", "").toString,
          "lkpKey" -> audit_req_param.getOrElse("LKP_CUSTOMER_ID", "").toString,
          "tgtKeyColumn" -> audit_req_param.getOrElse("Customer_ID", "").toString,
          "tgtPartColNames" -> audit_req_param.getOrElse("ARG_PARTITION_NAME", "").toString,
          "tgtTblIncrementCol" -> audit_req_param.getOrElse("increment_column", "").toString,
          "tgtTblIncrementColDtFmt" -> audit_req_param.getOrElse("TARGET_TABLE_DATE_FORMAT", "").toString
      )

      logger.info("Mapping Keys for Datalens: \n " + mapping_keys.toString())

      val param_dict = Map("edge_node_user_config_file" -> audit_req_param("audit_config").toString,
        "job_status" -> job_status, "console_log_file_name" -> "N/A", "app_id" -> audit_req_param.getOrElse("appId", "").toString,
        "env" -> audit_req_param.getOrElse("env", "").toString, "exit_code" -> exit_code, "error_category" -> "N/A",
        "error_message" -> error_msg, "audit_email" -> audit_req_param.getOrElse("sent_to", "").toString)
      val spark: SparkSession = audit_req_param("spark").asInstanceOf[SparkSession]
      val sc: SparkContext = audit_req_param("sc").asInstanceOf[SparkContext]
      logger.info("Param dict in final call: " + param_dict.mkString(","))
      AuditFrameworkDriver.endJob(param_dict, mapping_keys, spark, sc)
      logger.info("After audit final call")
    }
    catch {
      case e: Exception =>
        logger.error("Error occurred while making audit final call from DQ\n"+e.printStackTrace())
    }
  }
}
