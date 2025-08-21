package com.citi.purgeFramework.datawriter

import com.citi.purgeFramework.utils.AppLogger
import com.citi.purgeFramework.utils.FileUtils.{listAllFilesFolderInDir, listAllFolderInDir}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.functions.{col, mean}

import java.util.Date
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.immutable.Map
import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks._


class ParquetFileWriter(spark: SparkSession) extends AppLogger with Serializable {


	def writemultilevelpartition(df:DataFrame,files_list:Array[String], partition_keys: Set[String],partition_groupby:String,base_path: String,output_path: String): Unit = {

		val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
		//val df = spark.sql(s"show partitions ${db_table}")
		//fs.listFiles( new Path( path ), true )
		//val filenames = ListBuffer[ String ]( )
		//while (files.hasNext ) filenames += files.next().getPath().toString()
		//filenames.foreach(println(_))
		//val sublevel_partitions = df.select(partition_groupby).distinct()  // to get states in a df
		//val sublevel_partition_list = sublevel_partitions.rdd.map(x=>x(0)).collect.toList //to get states in a list
		//--------------------*********-------------------//

		var filePaths = new ListBuffer[Path]()
		val nbrkeys = partition_keys.size
		var increment = 0
		var pathArray = listAllFolderInDir(spark, base_path, true, filePaths).distinct
		for (path_prin <- pathArray) { println(path_prin)}
		var data = new ListBuffer[(String, Long)]
		breakable {
			for (path <- pathArray) {
				increment = increment + 1
				var pathSplit = path.toString().split("/")
				for( path1 <- pathSplit) {println(path1)}
				var pathSplitSize = pathSplit.size
				var basepath_partition = base_path
				for (i <- nbrkeys to 1 by -1) {
					basepath_partition = basepath_partition + "/" + pathSplit(pathSplitSize - i)
				}
				val MEGABYTE: Long = 1024L * 1024L
				val folder_size = fs.getContentSummary(new Path(basepath_partition)).getLength / MEGABYTE
				val last_partition_level = pathSplit(pathSplitSize - 1)
				data += ((last_partition_level, folder_size))
				logger.info("last_partition_level, folder_size" + (last_partition_level, folder_size))
				//var processFolderName = target_path + "/" + pathSplit(pathSplitSize - 2)
				logger.info(path)
				logger.info(folder_size)
				if (increment > 500) break // break out of the for loop
				//fs.rename(new Path(rawFileName), new Path(processFileName))
			}
			import spark.implicits._
			var DF = spark.sparkContext.parallelize(data).toDF("last_partition_level","folder_size")
			DF = DF.groupBy("last_partition_level").agg(mean(col("folder_size")).as("folder_size") )
			DF.show(false)
			val sublevel_partitions = DF.select(col("last_partition_level")).distinct()  // to get states in a df
			val sublevel_partition_list = sublevel_partitions.rdd.map(x=>x(0)).collect.toList //to get states in a list



			for (sublevel_partition <- sublevel_partition_list)  //loop to get each state
			{
				logger.info("sublevel_partition : " + (sublevel_partition.toString splitAt (sublevel_partition.toString lastIndexOf "=") + 1)._2)
				df.show(false)
				val filtered_df = df.filter(col(partition_groupby) === (sublevel_partition.toString splitAt (sublevel_partition.toString lastIndexOf "=") +1)._2)
				filtered_df.show(false)
				filtered_df.printSchema()
				var folder_size = DF.filter($"last_partition_level" === sublevel_partition).select("folder_size").collect().map(_.getDouble(0).ceil).mkString("")
				val repartition_size = (folder_size.toDouble/512).ceil.toInt
				logger.info("repartition_size : " + repartition_size)
				val columns = partition_keys.toSeq.map(x => col(x))
				filtered_df.coalesce(repartition_size).write.partitionBy((partition_keys.toSeq):_*).mode("overwrite").parquet(base_path + "_temp")
			}
		}
	}
}
