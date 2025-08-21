package com.citi.purgeFramework.datawriter

import com.citi.purgeFramework.utils.{AppLogger, AuditIntegration}
import org.apache.spark.SparkContext

import java.util
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.{Failure, Success, Try}

class DataWriter(sparkSession:SparkSession) extends AppLogger with Serializable {


	def writeDataStaging(inputDataFrame: DataFrame): Boolean = {

		logger.info("Inside writeDataStaging")

		//val deltaTableWriter = new DeltaFileWriter()
		// Check if table exits of not and write routine to generate the tables.
		//new DeltaFileWriter().write(targetTableDefinition,inputDataFrame)
		true
	}

	val start = 300000
	val end = 500000
	val rnd = new scala.util.Random

	def hiveInsert(data_df: DataFrame, spark: SparkSession, sc: SparkContext, hiveDB: String, Outputhive_table: String): Unit = {
		//This function will re-try inserting into hive table for a max of three times
		val insrt_countr = sc.longAccumulator("Hive insert attempt counter")
		var insertFlag: Boolean = false
		//insrt_countr.add(1)
		while (insrt_countr.value <= 3 && !insertFlag) {
			Try {
				insrt_countr.add(1)
				logger.info("Inside hiveInsert routine for inserting values into  -----> " + hiveDB + "." + Outputhive_table + "insrt_countr value: " + insrt_countr)
				spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
				data_df.coalesce(1).write.mode("append").insertInto(hiveDB + "." + Outputhive_table)
			}
			match {
				case Success(v) =>
					logger.info("The data successfully inserted into  -----> " + hiveDB + "." + Outputhive_table)
					insertFlag = true
				//break the loop.
				case Failure(e: Throwable) =>
					logger.error("Error occurred while inserting the data  -----> " + hiveDB + "." + Outputhive_table + " Insert attempt number " + insrt_countr.value + " of 3. Error message: " + e.printStackTrace())
					logger.info("The schema for "+ hiveDB + "." + Outputhive_table + " " + data_df.printSchema())
					Thread.sleep(start + rnd.nextInt( (end - start) + 1 ))
			}
		}
		if (!insertFlag) {
			logger.error("Error occurred while inserting the data  -----> " + hiveDB + "." + Outputhive_table + " Exceeded maximum number of retry attempts ")
			val err_msg = "Error occurred while inserting the data  -----> " + hiveDB + "." + Outputhive_table + " Exceeded maximum number of retry attempts. Please check the spark log for details."
			AuditIntegration.audit_final_call("1", "Failed", err_msg)
			System.exit(1)
		}
	}


}
