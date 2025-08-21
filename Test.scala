
package com.citi.purgeFramework.driver

import com.citi.purgeFramework.utils.AuditIntegration.{getAuditConfigDetails, logger}
import com.citi.purgeFramework.utils.{AppLogger, AuditIntegration, SendEmail}
import com.typesafe.config.ConfigException.Missing
import com.typesafe.config.ConfigFactory

import java.io.File
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

object Driver extends AppLogger with Serializable {

  var audit_param_dict = scala.collection.mutable.Map[String, Any]()
  var autosys_job_name = ""
  var audit_int_flg = ""
  var audit_config = ""


  def main(sparkargs: Array[String]) : Unit  = {
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
    audit_int_flg = try {
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
    logger.info("audit_config: " + audit_config)
    var Reference_Query = user_config.getString("Input.Reference_Query")

    var hdfs_for_temp_data = user_config.getString("log_paths.hdfs_path_for_temp_data")
    val Reconciliation_table = user_config.getString("Input.Reconciliation_table")
    var Input_table_bkp_path = user_config.getString("Input.Input_table_bkp_path")
    val purge_status_table = "gcganamrswp_work.ccpa_purge_status"
    val purge_mode=spark_args("purge_mode")
    val CSI_ID=spark_args("CSI_ID")
    val ARG_GROUP_ID=spark_args("ARG_GROUP_ID")
    val ARG_TYPE_OF_PURGE=spark_args("ARG_TYPE_OF_PURGE")
    val ARG_PARTITION_NAME=spark_args("ARG_PARTITION_NAME")
    val LKP_DB_NAME=spark_args("LKP_DB_NAME")
    val LKP_TABLE_NAME=spark_args("LKP_TABLE_NAME")
    val LH_IND=spark_args("LH_IND")


    //val ARG_CUSTOMER_ID=${ARG_CUSTOMER_ID}
    val ARG_AGING=spark_args("ARG_AGING")
    val increment_column=spark_args("increment_column")
    val increment_datatype=spark_args("increment_datatype")
    val TARGET_TABLE_DATE_FORMAT=spark_args("ARG_TARGET_TABLE_DATE_FORMAT")
    val purge_date=spark_args("purge_date")
    val column_mapping=user_config.getString("Input.Column_mapping").split(",")
    var lits_override = try { spark_args("lits_override")} catch { case exception: Exception => "false" }

    audit_param_dict = audit_param_dict + ("audit_config" -> audit_config, "env" -> env, "sent_to" -> sent_to, "lits_override" -> lits_override, "ARG_PARTITION_NAME" -> ARG_PARTITION_NAME, "increment_column" -> increment_column,
      "increment_datatype" -> increment_datatype, "TARGET_TABLE_DATE_FORMAT" -> TARGET_TABLE_DATE_FORMAT, "LH_IND" -> LH_IND)


    val Identifier_Type = spark_args("Identifier_Type")


    var jobApplicationName = user_config.getString("spark.jobApplicationName")
    var LOB = user_config.getString("spark.LOB")
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

    val column_mapping_lkp = column_mapping.map("lkp.<" + _ + ">")
    val column_mapping_reference = column_mapping.map("reference_<" + _ + ">")
    val column_mapping_target = column_mapping.map("target_table.<" + _ + ">")
    val map_value = scala.collection.mutable.Map[String,String]()
    val s2List = Customer_ID.split(",").map("target_table." + _ )
    val s3List = LKP_CUSTOMER_ID.split(",").map("lkp." + _ )
    val reference_collist = LKP_CUSTOMER_ID.split(",").map("reference_" + _ )


    logger.info("column_mapping_lkp: " + column_mapping_lkp.mkString(" , "))
    logger.info("column_mapping_reference: " + column_mapping_reference.mkString(" , "))
    logger.info("column_mapping_target: " + column_mapping_target.mkString(" , "))

    logger.info("s3List: " + s3List.mkString(" , "))
    logger.info("s2List: " + s2List.mkString(" , "))


    for (i <- 0 to column_mapping_lkp.length-1) {
      map_value += column_mapping_lkp(i).trim() -> s3List(i).trim()}
    for (i <- 0 to column_mapping_reference.length-1) {
      map_value += column_mapping_reference(i).trim() -> reference_collist(i).trim()}
    for (i <- 0 to column_mapping_target.length-1) {
      map_value += column_mapping_target(i).trim() -> s2List(i).trim()}

    logger.info(map_value.map(pair => pair._1+"="+pair._2).mkString("?","&",""))


    for ((k, v) <- map_value) {
      Join_Query = Join_Query.replaceAll(k, v)
    }
    for ((k,v) <- map_value) {
    Reference_Query = Reference_Query.replaceAll(k, v)}

    logger.info("Join_Query: " + Join_Query)

    val other_user_provided_arguments = spark_args.-("audit_integration_flag").-("ARG_TYPE_OF_PURGE").-("user_config").-("ARG_AGING").-("LKP_TABLE_NAME").-("ARG_GROUP_ID").-("Identifier_Type").-("ARG_DB_NAME").-("purge_mode").-("increment_datatype").-("env").-("ARG_TABLE_NAME").-("runAtATime").mapValues(_.toString)

    audit_param_dict = audit_param_dict+("input_sql"-> Input_Table_query, "target_table" -> Input_Table, "other_user_provided_arguments" -> other_user_provided_arguments, "reference_table" -> reference_table, "Customer_ID" -> Customer_ID , "LKP_CUSTOMER_ID" -> LKP_CUSTOMER_ID, "LKP_DB_NAME" -> LKP_DB_NAME, "LKP_TABLE_NAME" -> LKP_TABLE_NAME)


    var argsList: List[Array[String]] = List[Array[String]]()

    var args: Array[String] = null
    args = Array("appName", jobApplicationName,
      "LOB", LOB,
      "Input_Table_query", Input_Table_query,
      "Input_Table", Input_Table,
      "reference_table", reference_table,
      "Reconciliation_table", Reconciliation_table,
      "purge_status_table", purge_status_table,
      "Customer_ID",Customer_ID,
      "LKP_CUSTOMER_ID",LKP_CUSTOMER_ID,
      "Join_Query", Join_Query,
      "extra_cols", extra_cols,
      "Reference_Query", Reference_Query,
      "LOG_table",LOG_table,
      "runAtATime", runAtATime,
      "purge_mode", purge_mode,
      "CSI_ID", CSI_ID,
      "LITS_DT",purge_date,
      "ARG_GROUP_ID",ARG_GROUP_ID,
      "ARG_TYPE_OF_PURGE",ARG_TYPE_OF_PURGE,
      "ARG_PARTITION_NAME",ARG_PARTITION_NAME,
      "ARG_AGING",ARG_AGING,
      "increment_column",increment_column,
      "increment_datatype",increment_datatype,
      "TARGET_TABLE_DATE_FORMAT",TARGET_TABLE_DATE_FORMAT,
      "Identifier_Type",Identifier_Type,
      "Log_table_query",Log_table_query,
      "lits_override", lits_override,
      "env", env,
      "sent_to", sent_to,
      "hdfs_for_temp_data", hdfs_for_temp_data,
      "Input_table_bkp_path", Input_table_bkp_path)
    argsList = args :: argsList


    val orchestrator = new Orchestrator
    orchestrator.setUp(args)
    orchestrator.processInput(args)
    }
}
