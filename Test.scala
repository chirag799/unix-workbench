package com.citi.purgeFramework.utils

import com.citi.purgeFramework.driver.Driver.audit_param_dict
import com.citi.purgeFramework.utils.FileUtils.ListHDFSDirectories
import org.apache.spark.sql.functions.{array_contains, broadcast, coalesce, col, collect_list, collect_set, concat, concat_ws, current_timestamp, from_unixtime, lit, max, regexp_replace, size, split, substring_index, sum, to_date, trim, unix_timestamp, when}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.{AnalysisException, DataFrame, SparkSession}
import org.slf4j.LoggerFactory
import org.apache.spark.sql.Row
import org.apache.spark.storage.StorageLevel

import java.time.{LocalDate, LocalDateTime}
import java.time.format.DateTimeFormatter
import scala.collection.mutable.ListBuffer
import scala.io.Source
import scala.util.{Failure, Success, Try}
import org.apache.hadoop.fs.{FileSystem, Path}

object HelperFunctions extends AppLogger with Serializable {
	val sndEmail = new SendEmail

	def md5Hash(usString: String): String = {
		import java.math.BigInteger
		import java.security.MessageDigest
		val md = MessageDigest.getInstance("MD5")
		val digest: Array[Byte] = md.digest(usString.getBytes)
		val bigInt = new BigInteger(1, digest)
		val hashedStr = bigInt.toString(16).trim
		hashedStr
	}

	def getTablePartition(spark: SparkSession, db_table: String): Array[String] = {
		import spark.implicits._
		val partitionedCols: Array[String] = try {
			logger.info(s"show partitions ${db_table}")
			spark.sql(s"show partitions ${db_table}").as[String].first.split('/').map(_.split("=").head)
		}
		catch {
			case e: AnalysisException => Array.empty[String]
		}
		return partitionedCols
	}


	def processCRS(spark: SparkSession, reference_table: String, LKP_CUSTOMER_ID: String,log_table_Query: String,deletedReference_Df:DataFrame): DataFrame = {
		val columns = "description"
		logger.info("log_table_Query: " + log_table_Query)
		val LKP_CUSTOMER_ID_List = LKP_CUSTOMER_ID.split(",")
		val LKP_CUSTOMER_ID_List_column = LKP_CUSTOMER_ID_List.map(col(_))
		val df = spark.sql(log_table_Query)
		val deleted_df_distinct = deletedReference_Df.select("account_id").distinct
		val df_joined = if(LKP_CUSTOMER_ID.split(",").length > 1){
			val df_concat = df.withColumn("LKP_CUSTOMER_ID",concat_ws(",", LKP_CUSTOMER_ID_List_column : _*))
			df_concat.alias("fst").join(broadcast(deleted_df_distinct), df_concat("LKP_CUSTOMER_ID") === deleted_df_distinct("account_id"), "fullouter").select(coalesce(deleted_df_distinct("account_id"), df_concat("LKP_CUSTOMER_ID")).alias(s"reference_customer_id"),deleted_df_distinct("account_id"),df_concat("description"))
		}
		else{
			val selectColumns = Array(coalesce(df(LKP_CUSTOMER_ID), deleted_df_distinct("account_id")).alias(s"reference_${LKP_CUSTOMER_ID}"), deleted_df_distinct("account_id"),df("description"))
			df.alias("fst").join(broadcast(deleted_df_distinct), df(LKP_CUSTOMER_ID) === deleted_df_distinct("account_id"), "fullouter").select(selectColumns:_*)
		}
		//val joinExprs = join_keys.map{case (c1) => df(c1) === deleted_df_distinct(c1)}.reduce(_ && _)
		//val selectColumns = Array(coalesce(df(LKP_CUSTOMER_ID)).alias(s"reference_${LKP_CUSTOMER_ID}"), deleted_df_distinct("account_id"),df("description"))
		//val df_joined = df.alias("fst").join(broadcast(deleted_df_distinct), joinExprs, "fullouter").select(selectColumns:_*)
		val df_final = df_joined.withColumn("status", when(col(s"account_id").isNotNull, "SUCCESS").when(col(s"account_id").isNull, "NOT FOUND").otherwise("NA")).withColumnRenamed(s"reference_${LKP_CUSTOMER_ID}","reference_customer_id")
		return df_final	}

	def processCARDS(spark: SparkSession, reference_table: String, LKP_CUSTOMER_ID: String,log_table_Query: String,deletedReference_Df:DataFrame): DataFrame = {
		val columns = "purge_type,description"
		logger.info("log_table_Query: " + log_table_Query)
		val LKP_CUSTOMER_ID_List = LKP_CUSTOMER_ID.split(",")
		val LKP_CUSTOMER_ID_List_column = LKP_CUSTOMER_ID_List.map(col(_))
		val df = spark.sql(log_table_Query)
		val deleted_df_distinct = deletedReference_Df.select("account_id").distinct
		val df_joined = if(LKP_CUSTOMER_ID.split(",").length > 1){
			val df_concat = df.withColumn("LKP_CUSTOMER_ID",concat_ws(",", LKP_CUSTOMER_ID_List_column : _*))
			df_concat.alias("fst").join(broadcast(deleted_df_distinct), df_concat("LKP_CUSTOMER_ID") === deleted_df_distinct("account_id"), "fullouter").select(coalesce(deleted_df_distinct("account_id"), df_concat("LKP_CUSTOMER_ID")).alias(s"reference_customer_id"),deleted_df_distinct("account_id"),df_concat("description"),df_concat("purge_type"))
		}
		else{
			val selectColumns = Array(coalesce(df(LKP_CUSTOMER_ID), deleted_df_distinct("account_id")).alias(s"reference_${LKP_CUSTOMER_ID}"), deleted_df_distinct("account_id"),df("description"),df("purge_type"))
			df.alias("fst").join(broadcast(deleted_df_distinct), df(LKP_CUSTOMER_ID) === deleted_df_distinct("account_id"), "fullouter").select(selectColumns:_*)
		}
		val df_final = df_joined.withColumn("status", when(col(s"account_id").isNotNull and col(s"purge_type") =!= "partial_purge", "SUCCESS").when(col(s"account_id").isNotNull and col(s"purge_type") === "partial_purge", "partial_purge").when(col(s"account_id").isNull, "NOT FOUND").otherwise("NA")).withColumnRenamed(s"reference_${LKP_CUSTOMER_ID}","reference_customer_id")
		return df_final
	}

	def processRETAIL(spark: SparkSession, reference_table: String,LKP_CUSTOMER_ID: String,log_table_Query: String,deletedReference_Df:DataFrame): DataFrame = {
		val columns = "description"
		logger.info("log_table_Query: " + log_table_Query)
		val LKP_CUSTOMER_ID_List = LKP_CUSTOMER_ID.split(",")
		val LKP_CUSTOMER_ID_List_column = LKP_CUSTOMER_ID_List.map(col(_))
		logger.info("LKP_CUSTOMER_ID_List_column:"  + LKP_CUSTOMER_ID_List_column)
		val df = spark.sql(log_table_Query)
		df.show(100,false)
		val deleted_df_distinct = deletedReference_Df.select("account_id").distinct
		deleted_df_distinct.show(100,false)
		val df_joined = if(LKP_CUSTOMER_ID.split(",").length > 1){
			val df_concat = df.withColumn("LKP_CUSTOMER_ID",concat_ws(",", LKP_CUSTOMER_ID_List_column : _*))
			df_concat.alias("fst").join(broadcast(deleted_df_distinct), df_concat("LKP_CUSTOMER_ID") === deleted_df_distinct("account_id"), "fullouter").select(coalesce(deleted_df_distinct("account_id"), df_concat("LKP_CUSTOMER_ID")).alias(s"reference_customer_id"),deleted_df_distinct("account_id"),df_concat("description"))
		}
		else{
			val selectColumns = Array(coalesce(df(LKP_CUSTOMER_ID),deleted_df_distinct("account_id")).alias(s"reference_${LKP_CUSTOMER_ID}"), deleted_df_distinct("account_id"),df("description"))
			logger.info("selectColumns:" + selectColumns.toList)
			df.alias("fst").join(broadcast(deleted_df_distinct), df(LKP_CUSTOMER_ID) === deleted_df_distinct("account_id"), "fullouter").select(selectColumns:_*)
		}
		df_joined.show(100,false)
		val df_final = df_joined.withColumn("status", when(col(s"account_id").isNotNull, "SUCCESS").when(col(s"account_id").isNull, "NOT FOUND").otherwise("NA")).withColumnRenamed(s"reference_${LKP_CUSTOMER_ID}","reference_customer_id")
		return df_final	}

	def parse_LOG_Results(LOB: String, spark: SparkSession, Input_Table:String,reference_table: String,log_table_Query: String, LKP_CUSTOMER_ID: String, LOG_table: String,CSI_ID:String,Identifier_Type:String,deletedReference_Df :DataFrame) = {
		import spark.implicits._
		val year = LocalDateTime.now.format(DateTimeFormatter.ofPattern("YYYY"))

		val resultDataFrame = LOB.toLowerCase() match {
			case "crs" => processCRS(spark, reference_table, LKP_CUSTOMER_ID,log_table_Query,deletedReference_Df)
			case "cards" => processCARDS(spark, reference_table, LKP_CUSTOMER_ID,log_table_Query,deletedReference_Df)
			case "retail" => processRETAIL(spark, reference_table, LKP_CUSTOMER_ID,log_table_Query,deletedReference_Df)
		}
		var df_filter = resultDataFrame

		logger.info("------------Initial processed df----------------")
		df_filter.createTempView("CheckStagingTbl")

		var CheckStagingTblOp = spark.sql(s"select account_id,description,status,reference_customer_id from CheckStagingTbl")
			.withColumn("purge_ts", current_timestamp())
			//.withColumn("rule_name",extrctRuleName(col("validation_rule")))
			.withColumn("eap_as_of_year", lit(year))
			//Need to extract table name
			.withColumn("purge_table", lit(Input_Table))
			.withColumn("identifier", col("reference_customer_id").cast("decimal(30,0)"))
			.withColumn("identifier_type",lit(Identifier_Type))
			//.withColumnRenamed("reference_customer_id","identifier")
			.withColumn("functional_id",lit(spark.sparkContext.sparkUser))
			.withColumn("csi_id",lit(CSI_ID))
			.drop("reference_customer_id")

		val reorderedColumnNames: Array[String] = Array("functional_id", "csi_id", "identifier", "identifier_type", "status", "purge_ts", "description", "eap_as_of_year", "purge_table")
		val df_final_freq_final: DataFrame = CheckStagingTblOp.select(reorderedColumnNames.head, reorderedColumnNames.tail: _*)
		spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
		df_final_freq_final.show(truncate = false)

		//df_final_freq_final.write.mode("append").insertInto(Output_profiling_database + "." + Output_profiling_table)
		hiveSave_log(df_final_freq_final: DataFrame, spark: SparkSession, LOG_table: String)
	}

	def hiveSave_log(data_df: DataFrame, spark: SparkSession, Outputhive_table: String, savemode: String = "append"): Unit = {
		//This function will re-try inserting into hive table for a max of three times
		val insrt_countr = spark.sparkContext.longAccumulator("Hive insert attempt counter")
		var insertFlag: Boolean = false
		val start = 300000
		val end = 500000
		val rnd = new scala.util.Random
		//insrt_countr.add(1)
		val data_df_persist = data_df.repartition(1).persist(StorageLevel.MEMORY_ONLY_2)
		while (insrt_countr.value <= 2 && !insertFlag && data_df_persist.count() > 0) {
			Try {
				insrt_countr.add(1)
				//spark.conf.set("spark.hadoop.hive.exec.dynamic.partition", "true")
				spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")

				//spark.conf.set("hive.exec.dynamic.partition", "true")

				data_df_persist.select(spark.table(s"$Outputhive_table").columns.map(col): _*).createOrReplaceTempView("final_data")

				val eap_as_of_year = spark.sql("select eap_as_of_year from final_data limit 1").collect().map(_ (0)).toList
				logger.info("partn_col_values: " + eap_as_of_year(0))

				val purge_table = spark.sql("select purge_table from final_data limit 1").collect().map(_ (0)).toList
				logger.info("purge_table: " + purge_table(0))
				//data_df.select(spark.table(s"$Outputhive_table").columns.map(col): _*).createOrReplaceTempView("final_data")
				spark.sql("INSERT INTO " + Outputhive_table + " PARTITION(eap_as_of_year='" + eap_as_of_year(0) + "',purge_table='" + purge_table(0) + "') SELECT * FROM" +
					"(SELECT functional_id,csi_id,identifier,identifier_type,status,purge_ts,description from final_data) final_data")
				//data_df.repartition(1).write.mode("append").format("hive").insertInto(Outputhive_table) //.mode(savemode)
			}
			match {
				case Success(v) =>
					insertFlag = true
				case Failure(e: Throwable) =>
					Thread.sleep(start + rnd.nextInt((end - start) + 1))
					logger.info(e.printStackTrace().toString)
					System.err.println("Failure happened! Exiting job")
					val err_msg = e.printStackTrace().toString
					AuditIntegration.audit_final_call("1", "Failed", e.getMessage, e)
					System.exit(1)
			}
		}
	}

	def parse_reconciliation_Result(LOB: String, spark: SparkSession, reconciliation_df: DataFrame,Input_Table: String, appName: String, Reconciliation_table: String,hdfs_temp_reconciliation_df:String,Identifier_Type:String,partition_column:String,LITS_DT:String,filtered_list:ListBuffer[String],TARGET_TABLE_DATE_FORMAT:String) = {
		import spark.implicits._
		val year = LocalDateTime.now.format(DateTimeFormatter.ofPattern("YYYY"))
		//List(partition_details, beforepurge_table_count.toString, delete_count.toString, postpurge_table_count.toString)

		val reconciliationColumnNames: Array[String] = Array("partition_details", "initial_count", "purged_count", "postpurge_count")
		val newNames = Seq("partition_details", "initial_count", "purged_count", "postpurge_count")
		var df_filter: DataFrame = reconciliation_df.toDF(newNames: _*)
		val sum_counts = df_filter.agg(
			sum("initial_count").alias("sum_initial_count"),
			sum("purged_count").alias("sum_purged_count"),
			sum("postpurge_count").alias("sum_postpurge_count")
		).first()
		val sum_initial_count = sum_counts.getAs[Double]("sum_initial_count")
		val sum_purged_count = sum_counts.getAs[Double]("sum_purged_count")
		val sum_postpurge_count = sum_counts.getAs[Double]("sum_postpurge_count")

		val l = List("initial_count", "purged_count", "postpurge_count")
		val exprs = l.map( m => m -> "sum").toMap
		df_filter = df_filter.groupBy("partition_details").agg(exprs).toDF(newNames: _*)

		df_filter.show(false)
		audit_param_dict = audit_param_dict + ("tgt_tbl_initial_count" -> sum_initial_count, "tgt_tbl_purge_count" -> sum_purged_count ,"tgt_tbl_final_count" -> sum_postpurge_count)

		//val hdfs_metadata = ListHDFSDirectories(spark,Input_Table)
		//val base_path = hdfs_metadata._1
		//val filtered_list_df = filtered_list.map(element => element.replaceAll(base_path + "/", "")).toDF("partition_details")
		//filtered_list_df.show(false)
		//val df_union = df_filter.as("d1").join(filtered_list_df.as("d2"),df_filter("partition_details") === filtered_list_df("partition_details"),"fullouter").select("d2.partition_details","d1.purged_count").na.fill(0)
		//df_union.show(false)

		logger.info("------------Initial processed df----------------")

		var CheckStagingTblOp = df_filter.withColumn("insert_timestamp", current_timestamp())
			.withColumn("eap_as_of_year", lit(year))
			.withColumn("appname",lit(appName))
			.withColumn("appid",lit(spark.sparkContext.applicationId))
			.withColumn("input_table",lit(Input_Table))
			.withColumn("appname",lit(appName))
			.withColumn("appid",lit(spark.sparkContext.applicationId))
			.withColumn("date_column",lit(partition_column))
			.withColumn("lits_purge_dt",lit(LITS_DT))
			.withColumn("partition_col_format",lit(TARGET_TABLE_DATE_FORMAT))
		logger.info(CheckStagingTblOp.count())

		val reorderedColumnNames: Array[String] = Array("input_table", "partition_details", "initial_count", "purged_count", "postpurge_count", "date_column","partition_col_format","lits_purge_dt","insert_timestamp", "appid", "appname", "eap_as_of_year")
		val df_final_freq_final: DataFrame = CheckStagingTblOp.select(reorderedColumnNames.head, reorderedColumnNames.tail: _*)
		spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
		df_final_freq_final.show(truncate = false)
		hiveSave_reconciliation(df_final_freq_final: DataFrame, spark: SparkSession, Reconciliation_table: String)

		val reorderedColumnNames_csv: Array[String] = Array("input_table", "partition_details", "purged_count", "date_column","partition_col_format","lits_purge_dt","insert_timestamp", "appid", "appname", "eap_as_of_year")
		//val df_final_freq_csv: DataFrame = CheckStagingTblOp.select(reorderedColumnNames_csv.head, reorderedColumnNames_csv.tail: _*)
		//df_final_freq_csv.show(12,false)
		df_final_freq_final.repartition(1).write.mode("overwrite").format("csv").option("emptyValue", null).option("nullValue", null).option("header", "true").option("lineSep", "\n").save(hdfs_temp_reconciliation_df)

	}

	def hiveSave_reconciliation(data_df: DataFrame, spark: SparkSession, Outputhive_table: String, savemode: String = "append"): Unit = {
		//This function will re-try inserting into hive table for a max of three times
		val insrt_countr = spark.sparkContext.longAccumulator("Hive insert attempt counter")
		var insertFlag: Boolean = false
		val start = 300000
		val end = 500000
		val rnd = new scala.util.Random
		//insrt_countr.add(1)
		val data_df_persist = data_df.repartition(1).persist(StorageLevel.MEMORY_ONLY_2)
		while (insrt_countr.value <= 2 && !insertFlag && data_df_persist.count() > 0) {
			Try {
				insrt_countr.add(1)
				//spark.conf.set("spark.hadoop.hive.exec.dynamic.partition", "true")
				spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")

				//spark.conf.set("hive.exec.dynamic.partition", "true")

				data_df_persist.select(spark.table(s"$Outputhive_table").columns.map(col): _*).createOrReplaceTempView("final_data")

				val eap_as_of_year = spark.sql("select eap_as_of_year from final_data limit 1").collect().map(_ (0)).toList
				logger.info("partn_col_values: " + eap_as_of_year(0))

				val input_table = spark.sql("select input_table from final_data limit 1").collect().map(_ (0)).toList
				logger.info("purge_table: " + input_table(0))
				//data_df.select(spark.table(s"$Outputhive_table").columns.map(col): _*).createOrReplaceTempView("final_data")
				spark.sql("INSERT INTO " + Outputhive_table + " PARTITION(eap_as_of_year='" + eap_as_of_year(0) + "',input_table='" + input_table(0) + "') SELECT * FROM" +
					"(SELECT partition_details,initial_count,purged_count,postpurge_count,date_column,partition_col_format,lits_purge_dt,insert_timestamp,appid,appname from final_data) final_data")
				//data_df.repartition(1).write.mode("append").format("hive").insertInto(Outputhive_table) //.mode(savemode)
			}
			match {
				case Success(v) =>
					insertFlag = true
				case Failure(e: Throwable) =>
					Thread.sleep(start + rnd.nextInt((end - start) + 1))
					logger.info(e.printStackTrace().toString)
					System.err.println("Failure happened! Exiting job")
					AuditIntegration.audit_final_call("1", "Failed", e.getMessage, e)
					System.exit(1)
					val err_msg = e.printStackTrace().toString
			}
		}
	}

	def partitiontoprocess(spark: SparkSession, input_table: String,Input_Table_query:String, curr_purge_dt: String,reconciliation_table: String,partition_column:String,increment_datatype:String,dateFormat:String,lits_override:String,partitions_folder_list:ListBuffer[Path]) = {
		var list_intersect = new ListBuffer[String]
		val df_input_query = spark.sql(Input_Table_query)
		logger.info("lits_override:" + lits_override)
		logger.info("partition_column: " + partition_column)
		logger.info("increment_datatype: " + increment_datatype)
		logger.info("dateFormat: " + dateFormat)
		logger.info("curr_purge_dt: " + curr_purge_dt)
		val filterlist_input_query = df_input_query.select(partition_column).collect().map(_(0)).toList.map(_.toString)
		val filtered_list_input_path = partitions_folder_list.map(_.toString).filter(s => filterlist_input_query.exists(s.contains))
		//TO_DATE(CAST(UNIX_TIMESTAMP(purge_dt, 'MM/dd/yyyy') AS TIMESTAMP))
		val lits_date_format = "yyyy-MM-dd"
		val df = spark.sql(s"select input_table, TO_DATE(CAST(UNIX_TIMESTAMP(lits_purge_dt, '${lits_date_format}') AS TIMESTAMP)) as purge_date,partition_details from ${reconciliation_table} where input_table = '${input_table}'")
		logger.info(df.show(10,false))
		if(df.count()> 0 && lits_override == "false") {
			val max_purge_dt = df.groupBy("input_table").agg(max("purge_date")).collect().map(_ (1)).toList(0).toString
			logger.info("max_purge_dt: " + max_purge_dt)
			//val df2 = df.withColumn("purge_dt", unix_timestamp(col("lits_purge_dt"), dateFormat)).max(col("date")).as("maxDate").withColumn("maxDate", from_unixtime(col("maxDate"), dateFormat))
			//val max_purge_dt= df.collect().map(_ (0)).toList(1)
			val df_filter = df.filter(col("purge_date") === max_purge_dt)
			df_filter.show(11,false)
			var df_1 = df_filter.withColumn("partition", split(col("partition_details"), "\\/"))
			val numCols = df_1.withColumn("partition_size", size(col("partition"))).agg(max(col("partition_size"))).head().getInt(0)
			df_1 = df_1.select((0 until numCols).map(i => col("partition").getItem(i).as(s"col$i")): _*)

			for (i <- 0 until numCols) {
				val first_value = df_1.select(s"col${i}").first.getString(0).split("=")(0)
				df_1 = df_1.withColumnRenamed(s"col$i", first_value)
			}
			val job_max_partition = if ( increment_datatype matches "date|timestamp") {
				df_1 = df_1.withColumn(partition_column,split(df_1(partition_column), "\\=").getItem(1))
				df_1.show(false)
				// Step 1: Select the column and apply the to_date function
				logger.info("Step 1: Selecting the column and applying to_date function")
				val dateColumn = df_1.select(to_date(col(partition_column), dateFormat).as("date_column"))
				dateColumn.show(false)

				df_1.select(max(to_date(col(partition_column),dateFormat)).name("max_date")).collect().map(_ (0)).toList(0).toString
			}
			else {
				df_1.select(max(col(partition_column)).name("max_date")).collect().map(_ (0)).toList(0).toString.split("=")(1)			}
			//val job_max_partition = df_1.agg(max(partition_column)).collect().map(_ (0)).toList(0).toString.split("=")(1)
			logger.info("job_max_partition: " + job_max_partition)
			val sql_query = if (LocalDate.parse(curr_purge_dt).compareTo(LocalDate.parse(max_purge_dt.toString)) <= 0) {
				increment_datatype match {
					case "date" | "timestamp" =>
						val formatted_partition = s"TO_DATE(CAST(UNIX_TIMESTAMP(${partition_column}, '${dateFormat}') AS TIMESTAMP))"
						s"select distinct ${partition_column} from ${input_table} where ${formatted_partition} > TO_DATE('${job_max_partition}', '${lits_date_format}')"
					case "string" =>
						s"select distinct ${partition_column} from ${input_table} where ${partition_column} > '${job_max_partition}'"
					case "numeric" =>
						s"select distinct ${partition_column} from ${input_table} where ${partition_column} > ${job_max_partition}"
					case _ =>
						s"select distinct ${partition_column} from ${input_table} where ${partition_column} > '${job_max_partition}'"
					//s"select distinct ${partition_column} from ${input_table} where ${partition_column} > ${job_max_partition}"
				}
			}
			else
				s"select distinct ${partition_column} from ${input_table}"
			/*
			if (increment_datatype matches "date|timestamp") {
				val formatted_partition = s"TO_DATE(CAST(UNIX_TIMESTAMP(${partition_column}, '${dateFormat}') AS TIMESTAMP))"
				s"select distinct ${partition_column} from ${input_table} where ${formatted_partition} > TO_DATE('${job_max_partition}', '${dateFormat}')"
			} else {
			} */
			logger.info("sql_query" + sql_query)
			val df1 = spark.sql(sql_query)
			logger.info("filtered_list_input_query:" + filtered_list_input_path)
			if(df1.count()>0) {
				val filterlist_lits_dt = df1.select(partition_column).collect().map(_ (0)).toList.map(_.toString)
				logger.info("filterlist_lits_dt:" + filterlist_lits_dt)
				val filterlist_lits_dt_path = partitions_folder_list.map(_.toString).filter(s => filterlist_lits_dt.exists(s.contains))
				logger.info("filterlist_lits_dt_path:" + filterlist_lits_dt_path)
				logger.info(df1.show(5,false))
				list_intersect = filtered_list_input_path.intersect(filterlist_lits_dt_path)
			}
      else {
				val filterlist_lits_dt = new ListBuffer[String]
				list_intersect = filtered_list_input_path.intersect(filterlist_lits_dt)
			}
		}
		else{
			list_intersect = filtered_list_input_path
		}

		logger.info("list_intersect: " + list_intersect)
		list_intersect
	}

	def validateDf(spark: SparkSession, df: DataFrame, partition_name:String, Date_format:String): String = {
		//assume row.getString(1) with give Datetime string
		val partition_list = partition_name.split(",").map(_.toString).distinct
		var date_col = ""
		logger.info("partition_list: " + partition_list)
		for (partition <- partition_list) {
			val col_value = df.select(partition).collect().map(_ (0)).toList(0).toString
			logger.info("col_value: "  + col_value)
			logger.info("Date_format: " + Date_format)
			try {
				//assume row.getString(1) with give Datetime string
				java.time.LocalDate.parse(col_value, java.time.format.DateTimeFormatter.ofPattern(Date_format))
				date_col = partition
			} catch {
				case ex: java.time.format.DateTimeParseException => {
					// Handle exception if you want
					false
				}
			}
		}
		if (date_col == "" && Date_format != ""){
			System.err.println("Failure happened! Not able to find matching column with Date_Format mentioned in Config file. Exiting job")
			AuditIntegration.audit_final_call("1", "Failed", "Failure happened! Not able to find matching column with Date_Format mentioned in Config file. Exiting job")
			System.exit(1)
		}
		date_col
	}
}

/*
functional_id varchar(20),
csi_id varchar(20),
identifier decimal(30,0),
identifier_type varchar(50),
status string,
purge_ts timestamp,
description string)
PARTITIONED BY (
eap_as_of_year int,
purge_table string)

db_Name
table_name
Partition_details
beforepurge_count
purged_recordcount
postpurge_count
insert_timestamp
appid
appname
eap_as_of_year
 */

