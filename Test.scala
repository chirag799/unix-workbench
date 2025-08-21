package com.citi.purgeFramework.utils

import org.apache.log4j.{Logger, MDC}
import org.apache.spark.sql.SparkSession

trait AppLogger extends Serializable {
	val logger: Logger = Logger.getLogger(this.getClass)


	MDC.put("jobName", System.getProperty("logger_autosys_job_name"))
	MDC.put("uuid", System.getProperty("logger_uuid"))
	MDC.put("appId", System.getProperty("logger_appid"))

	// Method to set MDC context
	def setMDCContext(spark: SparkSession): Unit = {
		MDC.put("jobName", System.getProperty("logger_autosys_job_name"))
		MDC.put("uuid", System.getProperty("logger_uuid"))
		MDC.put("appId", System.getProperty("logger_appid"))
	}
}
