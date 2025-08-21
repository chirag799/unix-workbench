package com.citi.purgeFramework.driver

import com.citi.purgeFramework.driver.Driver.{audit_int_flg, audit_param_dict, logger}
import com.citi.purgeFramework.utils.AuditIntegration.{getAuditConfigDetails, setReqAuditDetails}
import com.citi.purgeFramework.utils.FileUtils._
import com.citi.purgeFramework.utils.HelperFunctions.{md5Hash, parse_LOG_Results, parse_reconciliation_Result, partitiontoprocess}
import com.citi.purgeFramework.utils.{AppLogger, AuditIntegration, SendEmail, updateHDFSfile}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.functions.{array, col, concat_ws, lit}
import org.apache.spark.sql.types.{DoubleType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.storage.StorageLevel

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.concurrent._
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer


class Orchestrator extends AppLogger with Serializable {

  //class variable definitions
  var spark: SparkSession = _
  val sndEmail = new SendEmail
  var mail_config = scala.collection.mutable.Map[String, String]()
  var errMailGlblConfig = scala.collection.mutable.Map[String, String]()
  var run_date = DateTimeFormatter.ofPattern("yyyyMMdd").format(LocalDateTime.now)
  val timestmp = DateTimeFormatter.ofPattern("yyyyMMdd_HHmm").format(LocalDateTime.now)

  val err_mail_sub = "Purge framework job failed. Name - <jobApplicationName>, Job Date - <run_date> Env - <env>"
  val err_mail_body = "The data purge job with following id has failed. Please find the below details in below log path.\n\nSpark Log Path (Refer to the 'Logs' link for details):\n<spark_log_path>"

  val mail_sub = "Data Purge Report - <jobStatus>, Name - <jobApplicationName>, Job Date - <run_date> Env - <env>"
  val mail_body = "Attached is the data purge report for '<Input_Table>' executed in <env> environment.\n CSI_ID: <CSI_ID>\n LOB: <LOB>\n Reference table: <reference_table>\n\nSpark Log Path (Refer to the 'Logs' link for details):\n<spark_log_path>"
  val spark_log_path_dev = "https://bdmwr015x11h3.nam.nsroot.net:8090/cluster/app/<app_id>"
  val spark_log_path_uat = "https://bdmwr015x12h3.nam.nsroot.net:8090/cluster/app/<app_id>"
  val spark_log_path_prod = "https://bdswr402x03h2.nam.nsroot.net:8090/cluster/app/<app_id>"
  val bucketedData_final_schema = StructType(
    StructField("account_id", StringType, true) ::
      StructField("file_name", StringType, true) ::
      StructField("partition_details", LongType, true) :: Nil)

  def setUp(inputArguments: Array[String]): Option[Exception] = {

    logger.info("Inside setUp")
    val kvpairs = inputArguments.grouped(2).collect { case Array(k, v) => k -> v }.toMap
    val jobApplicationName = kvpairs("appName")
    spark = SparkSession.builder.appName(jobApplicationName)
      .config("spark.sql.autoBroadcastJoinThreshold","-1")
      .config("spark.default.parallelism","1")
      .config("spark.sql.shuffle.partitions","1")
      .enableHiveSupport()
      .getOrCreate()

    val appId = spark.sparkContext.applicationId
    setReqAuditDetails(spark, appId)


    audit_param_dict = audit_param_dict + ("spark" -> spark, "appId" -> appId, "sc" -> spark.sparkContext)
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
    None
  }

  def processInput(inputArguments: Array[String]) = {

    logger.info("Inside process")
    val kvpairs = inputArguments.grouped(2).collect { case Array(k, v) => k -> v }.toMap
    logger.info("Before Updating: " + kvpairs)
    try {
      val runAtATime = Integer.parseInt(kvpairs.getOrElse("runAtATime", "100"))
      val Input_Table_query = kvpairs.getOrElse("Input_Table_query", "Null")
      val Input_Table = kvpairs.getOrElse("Input_Table", "Null")
      val reference_table = kvpairs.getOrElse("reference_table", "Null")
      val Reconciliation_table = kvpairs.getOrElse("Reconciliation_table", "Null")
      val Customer_ID = kvpairs.getOrElse("Customer_ID", "Null")
      val LKP_CUSTOMER_ID = kvpairs.getOrElse("LKP_CUSTOMER_ID", "Null")
      val Identifier_Type = kvpairs.getOrElse("Identifier_Type", "Null")
      val LOB = kvpairs.getOrElse("LOB", "Null")
      val LOG_table = kvpairs.getOrElse("LOG_table", "Null")
      val CSI_ID = kvpairs.getOrElse("CSI_ID", "Null")
      var Join_Query = kvpairs.getOrElse("Join_Query", "Null")
      var Reference_Query = kvpairs.getOrElse("Reference_Query", "Null")
      val extra_cols = kvpairs.getOrElse("extra_cols", "Null")

      var Log_table_query = kvpairs.getOrElse("Log_table_query", "Null")
      var hdfs_for_temp_data = kvpairs.getOrElse("hdfs_for_temp_data", "Null")
      hdfs_for_temp_data = hdfs_for_temp_data.replace("$USER_ID", spark.sparkContext.sparkUser)
      val hdfs_folder_delete_df_loop = hdfs_for_temp_data + "/delete_df_temp/"
      //deleteHDFS(spark, hdfs_folder_delete_df_loop)

      val hdfs_temp_delete_df = hdfs_for_temp_data + "purged_accounts.csv"
      val hdfs_temp_reconciliation_df = hdfs_for_temp_data + "reconciliation_report.csv"
      val hdfs_temp_reconciliation_df_stg = hdfs_for_temp_data + "reconciliation_report_stg.csv"
      deleteHDFS(spark,hdfs_temp_reconciliation_df_stg)    //

      val Input_table_bkp_path = kvpairs.getOrElse("Input_table_bkp_path", "Null").replace("$USER_ID", spark.sparkContext.sparkUser) + "/" + timestmp + "/"

      Reference_Query = Reference_Query.replace("<reference_table>", reference_table)

      for ((k, v) <- kvpairs) {
        Reference_Query = Reference_Query.replaceAll("<" + k + ">", v)
      }

      for ((k, v) <- kvpairs) {
        Join_Query = Join_Query.replaceAll("<" + k + ">", v)
      }

      for ((k, v) <- kvpairs) {
        Log_table_query = Log_table_query.replaceAll("<" + k + ">", v)
      }

      val spark_log_path_env = kvpairs("env") match {
        case "dev" => spark_log_path_dev
        case "uat" => spark_log_path_uat
        case "prod" => spark_log_path_prod
      }

      mail_config += ("mail_sub" -> mail_sub,
        "mail_body" -> mail_body,
        "jobApplicationName" -> kvpairs("appName"),
        "appId" -> spark.sparkContext.applicationId,
        "run_date" -> run_date,
        "env" -> kvpairs("env"),
        "results_sent_to" -> kvpairs("sent_to"),
        "spark_log_path" -> spark_log_path_env.replace("<app_id>", spark.sparkContext.applicationId),
        "err_mail_sub" -> err_mail_sub,
        "err_mail_body" -> err_mail_body,
        "Input_Table" -> Input_Table,
        "reference_table" -> reference_table,
        "LOB" -> LOB,
        "CSI_ID" -> CSI_ID
      )
      mail_config = mail_config.++(kvpairs)

      errMailGlblConfig += ("err_mail_sub" -> err_mail_sub,
        "err_mail_body" -> err_mail_body,
        "results_sent_to" -> kvpairs("sent_to"),
        "jobApplicationName" -> kvpairs("appName"),
        "spark_log_path" -> spark_log_path_env.replace("<app_id>", spark.sparkContext.applicationId),
        "env" -> kvpairs("env"),
        "run_date" -> DateTimeFormatter.ofPattern("yyyy-MM-dd").format(LocalDateTime.now))

      val Customer_ID_List = Customer_ID.split(",")
      val LKP_CUSTOMER_ID_List = LKP_CUSTOMER_ID.split(",").map("reference_" + _)
      val Customer_ID_seq = Customer_ID_List.toSeq
      val Customer_ID_List_column = Customer_ID_List.map(col(_))

      val LKP_CUSTOMER_ID_seq = LKP_CUSTOMER_ID_List.toSeq
      val LKP_CUSTOMER_ID_List_column = LKP_CUSTOMER_ID_List.map(col(_))


      val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
      val MEGABYTE: Long = 1024L * 1024L
      val hdfs_metadata = ListHDFSDirectories(spark, Input_Table)
      logger.info("hdfs_metadata._2 :" + hdfs_metadata._2.toList)
      logger.info("hdfs_metadata._3 : " + hdfs_metadata._3)
      val base_path = hdfs_metadata._1
      var filePaths = new ListBuffer[Path]()
      val folder_size = fs.getContentSummary(new Path(base_path)).getLength / MEGABYTE
      val partitions_folder_list = listAllFolderInDir(spark, base_path, true, filePaths).distinct //Sort the partitions_file_list
      logger.info("partitions_list" + partitions_folder_list.toList)
      logger.info("partitions_list.size" + partitions_folder_list.size)
      val df = spark.sql(Input_Table_query)
      logger.info(df.show(false))
      val partition_column = kvpairs("increment_column")
      logger.info("partition_column" + partition_column)
      val filterlist = df.select(partition_column).collect().map(_(0)).toList.map(_.toString)
      logger.info("filterlist: " + filterlist)

      val filtered_list = partitiontoprocess(spark, Input_Table, Input_Table_query, kvpairs("LITS_DT"), Reconciliation_table, partition_column, kvpairs("increment_datatype"), kvpairs("TARGET_TABLE_DATE_FORMAT"), kvpairs("lits_override"), partitions_folder_list: ListBuffer[Path])

      //val filtered_list = partitions_folder_list.map(_.toString).filter(s => filterlist.exists(s.contains))
      logger.info("filtered_list" + filtered_list)

      val base_path_bkp = Input_table_bkp_path
      deleteHDFS(spark, base_path_bkp)




      val referenceDF = spark.sql(Reference_Query).distinct()
      val referenceDF_persist = referenceDF.persist(StorageLevel.MEMORY_AND_DISK)
      audit_param_dict = audit_param_dict + ("lits_purge_dt" -> kvpairs("LITS_DT"), "ref_tbl_total_count" -> referenceDF_persist.count())
      referenceDF.createOrReplaceGlobalTempView("referenceDF_tempview")
      //broadcast(referenceDF) //.persist(StorageLevel.MEMORY_AND_DISK) //Broadcasting reference table


      for (start_index <- 0 to filtered_list.size by runAtATime) {
        var deletedReference_Df_seq = Seq[DataFrame]()
        var reconciliation_seq = Seq[List[Any]]()
        var backup_files_seq = Seq[List[Any]]()
        var curated_files_seq = Seq[List[Any]]()


        var argsList: List[Array[String]] = List[Array[String]]()
        logger.info("Start_index: " + start_index)
        val end_index = if (start_index + runAtATime >= filtered_list.size) filtered_list.size else start_index + runAtATime
        logger.info("end_index: " + end_index)
        val partition_list = filtered_list.slice(start_index, end_index).map(_.toString)
        var pool = 0

        def poolId = {
          pool = pool + 1
          pool
        }

        for (inner_index <- 0 to partition_list.length - 1) {
          var args: Array[String] = null
          args = Array(
            "partition_name", partition_list(inner_index),
            "inner_index", inner_index.toString,
            "join_query", Join_Query
          )
          argsList = args :: argsList
        }

        def runner(qcArgsThread: Array[String]) = Future {
          spark.sparkContext.setLocalProperty("spark.scheduler.pool", poolId.toString)
          // set MDC context for logging purpose
          setMDCContext(spark)
          //Print the dataedge executed
          val kvpairs = qcArgsThread.grouped(2).collect { case Array(k, v) => k -> v }.toMap
          val partition_base_path = kvpairs("partition_name")
          val inner_index = kvpairs("inner_index")
          logger.info("inner_index : " + inner_index)
          var filePath_per_partition = new ListBuffer[Path]()
          val partitions_file_list = listAllFilesFolderInDir(spark, partition_base_path, true, filePath_per_partition).par //Sort the partitions_file_list
          logger.info("partitions_file_list : " + partitions_file_list)
          var i = 0
          for (file_name <- partitions_file_list) {
            setMDCContext(spark)
            var Input_table_file = s"parquet.`${file_name}`"
            /*
            val df = spark.read.option("header", "true").option("basePath", base_path).parquet(file_name)
            val df_reference = spark.sql(s"select ${LKP_CUSTOMER_ID} from ${reference_table}")
            val selectColumns = df.columns.map(df(_)) ++ Array(coalesce(df(Customer_ID)).alias(s"target_${Customer_ID}"), coalesce(df_reference(LKP_CUSTOMER_ID)).alias(s"reference_${LKP_CUSTOMER_ID}"))
            val df_joined = df.alias("fst").join(broadcast(df_reference), df(Customer_ID) === df_reference(LKP_CUSTOMER_ID), "fullouter").select(selectColumns:_*)
            */
            var Join_Query = kvpairs.getOrElse("join_query", "Null")
            Join_Query = Join_Query.replace("<Input_table_file>", Input_table_file)

            logger.info("Join_Query : " + Join_Query)
            var beforepurge_table_count: Double = 0

            val dataDF = spark.sql(Join_Query)
            beforepurge_table_count = dataDF.count()
            logger.info("Customer_ID_List : " + Customer_ID_List.mkString(" , "))
            logger.info("LKP_CUSTOMER_ID_List : " + LKP_CUSTOMER_ID_List.mkString(" , "))

            // Zip the lists to pair column names
            val joinConditions = Customer_ID_List.zip(LKP_CUSTOMER_ID_List).map { case (col1, col2) => dataDF(col1) <=> referenceDF_persist(col2) }

            // Join DataFrames dynamically based on the constructed conditions
            var df_joined: DataFrame = dataDF.join(referenceDF_persist, joinConditions.reduce(_ && _), "full_outer")
            //var df_joined = spark.sql(Join_Query)

            ///var curated_df = df_joined.filter(col(s"${Customer_ID}").isNotNull && col(s"reference_${LKP_CUSTOMER_ID}").isNull).drop(s"reference_${LKP_CUSTOMER_ID}")
            //var deleted_df = df_joined.filter(col(s"${Customer_ID}").isNotNull && col(s"reference_${LKP_CUSTOMER_ID}").isNotNull).select(s"reference_${LKP_CUSTOMER_ID}")
            //beforepurge_table_count = df_joined.filter(Customer_ID_seq.map(name => col(name).isNotNull).reduce(_ && _)).count()
            var curated_df = if (Customer_ID_List.length > 1) {
              df_joined.filter(Customer_ID_seq.map(name => col(name).isNotNull).reduce(_ && _) && LKP_CUSTOMER_ID_seq.map(name => col(name).isNull).reduce(_ && _)).drop(LKP_CUSTOMER_ID_List: _*)
            }
            else {
              df_joined.filter(col(s"${Customer_ID}").isNotNull && col(s"reference_${LKP_CUSTOMER_ID}").isNull).drop(s"reference_${LKP_CUSTOMER_ID}")
            }


            val deleted_df = if (Customer_ID_List.length > 1) {
              df_joined.filter(Customer_ID_seq.map(name => col(name).isNotNull).reduce(_ && _) && LKP_CUSTOMER_ID_seq.map(name => col(name).isNotNull).reduce(_ && _)).select(array(LKP_CUSTOMER_ID_List.map(col): _*).as("account_id")).withColumn("account_id", concat_ws(",", col("account_id")))
              //df_joined.filter(col(s"${Customer_ID}").isNotNull && col(s"reference_${LKP_CUSTOMER_ID}").isNotNull).select(s"reference_${LKP_CUSTOMER_ID}")
            }
            else {
              df_joined.filter(col(s"${Customer_ID}").isNotNull && col(s"reference_${LKP_CUSTOMER_ID}").isNotNull).select(s"reference_${LKP_CUSTOMER_ID}")
            }
            val delete_count: Double = deleted_df.count()
            //val beforepurge_table_count: Double = df_joined.filter(col(s"${Customer_ID}").isNotNull).count()
            //val postpurge_table_count: Double = 0
            val folder_name = (file_name.toString splitAt (file_name.toString lastIndexOf "/") + 1)._1
            val partition_details = folder_name.replace(base_path + "/", "")
            //val x = List(partition_details, beforepurge_table_count, delete_count, postpurge_table_count, file_name)
            //reconciliation_seq = reconciliation_seq :+ x
            if (delete_count > 0) {
              //deleteHDFS(spark, folder_name.replace(base_path, base_path + "_temp"))            Not required
              var files_name_bkp = file_name.toString.replace(base_path, Input_table_bkp_path)
              mkdirsHDFS(spark, files_name_bkp)
              logger.info("Backup folder created for " + file_name)
              val existing_file_list = List(file_name.toString, files_name_bkp)
              backup_files_seq = backup_files_seq :+ existing_file_list

              val hash_value = md5Hash((file_name.toString splitAt (file_name.toString lastIndexOf "/") + 1)._2)
              var folder_name_temp = partition_base_path.replace(base_path, base_path + s"_temp$hash_value")
              if (extra_cols != "Null" && extra_cols.nonEmpty) {
                val colsToDrop = extra_cols.split(",").filter(curated_df.columns.contains)
                if (colsToDrop.nonEmpty) {
                  curated_df = curated_df.drop(colsToDrop: _*)
                }              }
              curated_df.coalesce(1).write.mode("overwrite").parquet(folder_name_temp)
              val postpurge_table_count: Double = curated_df.count()
              curated_df.unpersist()

              var filePaths_target = new ListBuffer[Path]()
              val file_list_temp = listAllFilesFolderInDir(spark, folder_name_temp, true, filePaths_target)
              for (file_temp <- file_list_temp) {
                val curated_file_list = List(file_name.toString, file_temp)
                curated_files_seq = curated_files_seq :+ curated_file_list
              }

              val deleted_df_withPartition = deleted_df.withColumn("file_name", lit(file_name.toString)).withColumn("partition_details", lit(partition_details))
              //val deleted_df_folder_temp = hdfs_folder_delete_df_loop + s"$hash_value/purged_accounts.csv"
              //deleted_df_withPartition.repartition(1).write.mode("overwrite").format("csv").option("emptyValue", null).option("nullValue", null).option("header", "true").option("lineSep", "\\n").save(deleted_df_folder_temp)

              ////.withColumnRenamed(s"reference_${LKP_CUSTOMER_ID}", "account_id")  Defect
              //val reconciliation_df = deleted_df.withColumn("file_name", lit(base_path)).withColumn("partition_details",lit(partition_details))
              deletedReference_Df_seq = deletedReference_Df_seq :+ deleted_df_withPartition

              val x = List(partition_details, beforepurge_table_count, delete_count, postpurge_table_count, file_name)
              reconciliation_seq = reconciliation_seq :+ x
            }
            else {
              //var files_list_bkp = file_name.replace(base_path, base_path + "_bkp")
              //files_list_temp = (files_list_temp splitAt (files_list_temp lastIndexOf "/") + 1)._1
              logger.info("files_name inside else " + inner_index + " : " + file_name)
              val postpurge_table_count: Double = beforepurge_table_count
              val x = List(partition_details, beforepurge_table_count, delete_count, postpurge_table_count, file_name)
              reconciliation_seq = reconciliation_seq :+ x
            }
            i = i + 1
          }

          /* var folder_name_temp = partition_base_path.replace(base_path, base_path + s"_temp")
             if(i == partitions_file_list.length && curated_df_seq.length > 0){
              logger.info ("inside loop i : " + i)
              curated_df_seq.reduce(_ union _).coalesce(curated_df_seq.length).write.mode("overwrite").parquet(folder_name_temp)
              //var df_union = spark.sql(curated_join_query_union)
              //df_union.coalesce(10).write.mode("overwrite").parquet(folder_name_temp)
             }		 */
        }

        val futures = argsList map (i => runner(i))
        logger.info("---------- Data purge process successfully completed for batch----------")
        // now you need to wait all your futures to be completed
        val allFutures = Future.sequence(futures)

        val result =
          try {
            Await.result(allFutures, Duration.Inf)
          } catch {
            case e: Exception =>
              logger.info(e.getMessage)
              e.printStackTrace()
              System.err.println("Failure happened!")
              //spark.sql("MSCK REPAIR TABLE " + Input_Table)
              val err_msg = e.printStackTrace().toString
              AuditIntegration.audit_final_call("1", "Failed", e.getMessage, e)
              sndEmail.errorMail(errMailGlblConfig.asJava, err_msg)
              System.exit(1)
          }

        val deletedDF_union = if (deletedReference_Df_seq.isEmpty) {
          logger.info("INFO: No records to delete, returning empty dataframe")
          spark.createDataFrame(spark.sparkContext.emptyRDD[Row], bucketedData_final_schema)
        }
        else {
          deletedReference_Df_seq.reduce(_ union _)
        }

        //val reconciliationDF = reconciliation_seq.map(x => (x(0).asInstanceOf[String], x(1).toString)).toDF()

        if (start_index <= 1) {
          deleteHDFS(spark, hdfs_temp_delete_df)
          deleteHDFS(spark, hdfs_temp_reconciliation_df)
        }
        updateHDFSfile.updateHDFS(hdfs_temp_delete_df, deletedDF_union, spark)
        deleteHDFS(spark, hdfs_folder_delete_df_loop)

        updateHDFSfile.updatereconHDFS(hdfs_temp_reconciliation_df, reconciliation_seq, spark)

        val backup_files_list = backup_files_seq.map(x => (x(0).asInstanceOf[String], x(1).toString)).toList
        for (name <- backup_files_list) {
          renameHDFSFile(spark, name._1, name._2)
        }

        val curated_files_list = curated_files_seq.map(x => (x(0).asInstanceOf[String], x(1).toString)).toList
        for (name <- curated_files_list) {
          renameHDFSFile(spark, name._2, name._1)
        }

/*
        spark.catalog.clearCache()
        spark.sqlContext.clearCache()
        for ((id, rdd) <- spark.sparkContext.getPersistentRDDs) {
          rdd.unpersist()
        }

 */
        //distcp -update hdfs://nn1:8020/source/first hdfs://nn1:8020/source/second hdfs://nn2:8020/target
      }
      //spark.sql("MSCK REPAIR TABLE " + Input_Table)

      deleteHDFS_wildcard(spark, base_path + s"_temp*")
      //folder_name.replace(base_path, base_path + s"_temp

      val delete_df_schema = StructType(Array(
        StructField("account_id", StringType, true),
        StructField("file_name", StringType, true),
        StructField("partition_details", StringType, true)))

      val recon_df_schema = StructType(Array(
        StructField("partition_details", StringType, true),
        StructField("initial_count", DoubleType, true),
        StructField("purged_count", DoubleType, true),
        StructField("postpurge_count", DoubleType, true)))

      val deletedDF_final = spark.read.format("csv").schema(delete_df_schema).option("header", "true").load(hdfs_temp_delete_df)
      deletedDF_final.show(100,false)
      val reconDF_final = spark.read.format("csv").schema(recon_df_schema).option("header", "true").load(hdfs_temp_reconciliation_df)


      val attachment_names = hdfs_temp_reconciliation_df_stg

      mail_config += ("attachment_names" -> attachment_names,
        "jobStatus" -> "Success")
      parse_LOG_Results(LOB, spark, Input_Table, reference_table: String, Log_table_query: String, LKP_CUSTOMER_ID: String, LOG_table: String, CSI_ID: String, Identifier_Type: String, deletedDF_final: DataFrame)
      parse_reconciliation_Result(LOB, spark, reconDF_final, Input_Table: String, kvpairs("appName"): String, Reconciliation_table: String, hdfs_temp_reconciliation_df_stg: String, Identifier_Type: String, partition_column: String, kvpairs("LITS_DT"): String, filtered_list: ListBuffer[String], kvpairs("TARGET_TABLE_DATE_FORMAT"): String)

      //deletedDF_final = deletedDF_final.withColumn("account_id",concat(lit("'") , col("account_id"),lit("'") ))
      //deletedDF_final.repartition(1).write.mode("overwrite").format("csv").option("emptyValue", null).option("nullValue", null).option("header", "true").option("lineSep", "\\n").save(hdfs_temp_delete_stg_df)
      sndEmail.sendReport(mail_config.toMap.asJava)
      AuditIntegration.audit_final_call("0","Success","")
    }
    catch {
      case exception: Exception => {
        logger.info(exception.printStackTrace().toString)
        System.err.println("Failure happened! Exiting job")
        AuditIntegration.audit_final_call("1", "Failed", exception.getMessage, exception)
        val err_msg = exception.printStackTrace().toString
        sndEmail.errorMail(errMailGlblConfig.asJava, err_msg)
        System.exit(1)
      }
    }
  }
}
