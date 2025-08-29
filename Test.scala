package com.citi.purgeFramework.utils

import com.citi.purgeFramework.utils.HelperFunctions.logger
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.storage.StorageLevel

import java.nio.file.Paths

object updateHDFSfile {
  def updateHDFS(hdfs_path: String, data_df: DataFrame, spark: SparkSession, savemode: String = "append") = {

    var filePath = ""
    //val filenamewithoutextension = report_details(1).split("\\.")(0)
    //val filePath_csv = Paths.get(hdfs_path, filenamewithoutextension).toString
    import spark.implicits._
    val delete_df_schema = StructType(Array(
      StructField("account_id", StringType, true),
      StructField("file_name", StringType, true),
      StructField("partition_details", StringType, true)))

    logger.info("hdfs_path: " + hdfs_path)
    //.option("lineSep","\\n")
    val insrt_countr = spark.sparkContext.longAccumulator("Hive insert attempt counter")
    var insertFlag: Boolean = false
    //insrt_countr.add(1)
    while (insrt_countr.value <= 3 && !insertFlag) {
      val df_existing_records = try {
        spark.read.format("csv").schema(delete_df_schema).option("header", "true").load(hdfs_path)
      }
      catch {
        case e: Exception =>
          Seq.empty[(String, String, String)].toDF("account_id", "file_name", "partition_details")
      }
      logger.info("df_existing_records")
      logger.info("data_df")
      //data_df.show(1000,false)

      //val data_df_new = try {
      //  val filePattern = "*/purged_accounts.csv"
      //  spark.read.format("csv").schema(delete_df_schema).option("header", "true").load(hdfs_folder_delete_df_loop + filePattern)
      //}
      //catch {
      //  case e: Exception =>
      //    Seq.empty[(String, String, String)].toDF("account_id", "file_name", "partition_details")
      //}
      //data_df_new.repartition(1).write.mode("append").format("csv").option("emptyValue", null).option("nullValue", null).option("header", "true").option("lineSep", "\\n").save(hdfs_folder_delete_df_loop + "_stg")
      //data_df_new.show(1000,false)
      var df_existing_records_persist = Seq.empty[(String, String, String)].toDF("app_name", "indicator_str", "status")
      if (df_existing_records.count() >= 1) {
        df_existing_records.repartition(1).write.mode("overwrite").format("csv").option("emptyValue", null).option("nullValue", null).option("header", "true").option("lineSep", "\n").save(hdfs_path + "_stg")
        df_existing_records_persist = spark.read.format("csv").schema(delete_df_schema).option("header", "true").load(hdfs_path + "_stg")
      }
      data_df.union(df_existing_records_persist).repartition(1).write.mode("overwrite").format("csv").option("emptyValue", null).option("nullValue", null).option("header", "true").option("lineSep", "\n").save(hdfs_path)
      insertFlag = true
    }
    if (!insertFlag) {
      print("Error occurred while inserting the data, Exceeded maximum number of retry attempts")
      AuditIntegration.audit_final_call("1", "Failed", "Error occurred while inserting the data, Exceeded maximum number of retry attempts")
      System.exit(1)
    }
  }

  def updatereconHDFS(hdfs_path: String, reconciliation_seq: Seq[List[Any]], spark: SparkSession, savemode: String = "append") = {

    var filePath = ""
    //val filenamewithoutextension = report_details(1).split("\\.")(0)
    //val filePath_csv = Paths.get(hdfs_path, filenamewithoutextension).toString
    import spark.implicits._
    val recon_df_schema = StructType(Array(
      StructField("partition_details", StringType, true),
      StructField("initial_count", DoubleType, true),
      StructField("purged_count", DoubleType, true),
      StructField("postpurge_count", DoubleType, true)))

    //partition_details", "initial_count", "purged_count", "postpurge_count
    val resultDataFrame = reconciliation_seq.map(x => (x(0).toString, x(1).asInstanceOf[Double],x(2).asInstanceOf[Double], x(3).asInstanceOf[Double])).toDF()

    var deletedDF_union = if(reconciliation_seq.isEmpty){
      logger.info("INFO: No records to delete, returning empty dataframe")
    }
    else {
    }

    val reconciliationColumnNames: Array[String] = Array("partition_details", "initial_count", "purged_count", "postpurge_count")
    val newNames = Seq("partition_details", "initial_count", "purged_count", "postpurge_count")
    val data_df: DataFrame = resultDataFrame.toDF(newNames: _*)

    logger.info("hdfs_path: " + hdfs_path)
    //.option("lineSep","\\n")
    val insrt_countr = spark.sparkContext.longAccumulator("Hive insert attempt counter")
    var insertFlag: Boolean = false
    //insrt_countr.add(1)
    while (insrt_countr.value <= 3 && !insertFlag) {
      val df_existing_records = try {
        spark.read.format("csv").schema(recon_df_schema).option("header", "true").load(hdfs_path)
      }
      catch {
        case e: Exception =>
          Seq.empty[(String, Double, Double,Double)].toDF("partition_details", "initial_count", "purged_count","postpurge_count")
      }
      var df_existing_records_persist = Seq.empty[(String, Double, Double, Double)].toDF("partition_details", "initial_count", "purged_count","postpurge_count")
      if (df_existing_records.count() >= 1) {
        df_existing_records.repartition(1).write.mode("overwrite").format("csv").option("emptyValue", null).option("nullValue", null).option("header", "true").option("lineSep", "\n").save(hdfs_path + "_stg")
        df_existing_records_persist = spark.read.format("csv").schema(recon_df_schema).option("header", "true").load(hdfs_path + "_stg")
      }
      data_df.union(df_existing_records_persist).repartition(1).write.mode("overwrite").format("csv").option("emptyValue", null).option("nullValue", null).option("header", "true").option("lineSep", "\n").save(hdfs_path)
      insertFlag = true
    }
    if (!insertFlag) {
      print("Error occurred while inserting the data, Exceeded maximum number of retry attempts ")
      AuditIntegration.audit_final_call("1", "Failed", "Error occurred while inserting the data, Exceeded maximum number of retry attempts")
      System.exit(1)
    }
  }

}
