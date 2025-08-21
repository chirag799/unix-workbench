
package com.citi.purgeFramework.utils

import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs.{FileSystem, LocatedFileStatus, Path, RemoteIterator}
import org.apache.spark.sql.functions.{col, regexp_extract}

import scala.collection.mutable.ListBuffer
import scala.sys.exit


object FileUtils extends AppLogger with Serializable {

  def copyFileToHDFS(spark: SparkSession, srcNasLocation: String, tgtHDFSLocation: String): Unit = {
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    fs.copyFromLocalFile(new Path(srcNasLocation), new Path(tgtHDFSLocation))
  }

  def mkdirsHDFS(spark: SparkSession, hdfs_path: String): Unit = {
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    if (!fs.exists(new Path(hdfs_path))) {
      fs.mkdirs(new Path(hdfs_path))
    }
  }

  def deleteHDFS(spark: SparkSession, hdfs_path: String): Unit = {
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    logger.info("Delete path: " + hdfs_path)
    if (fs.exists(new Path(hdfs_path)) && fs.isFile(new Path(hdfs_path)))
      fs.delete(new Path(hdfs_path), true)

    //To Delete Directory
    if (fs.exists(new Path(hdfs_path)) && fs.isDirectory(new Path(hdfs_path)))
      fs.delete(new Path(hdfs_path), true)
  }

  def deleteHDFS_wildcard(spark: SparkSession, hdfs_path: String): Unit = {
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    logger.info("Delete path: " + hdfs_path)
    val deletePaths = fs.globStatus(new Path(hdfs_path) ).map(_.getPath)
    deletePaths.foreach{ path => fs.delete(path, true)}
  }



  ////Rename a File
  //if(fs.exists(srcPath) && fs.isFile(srcPath))
  //     fs.rename(srcPath,destPath)
  //val hadoopConfig = new Configuration()
  //val hdfs = FileSystem.get(hadoopConfig)
  //hdfs.rename(srcPath,destPath)
  //distcp -update hdfs://nn1:8020/source/first hdfs://nn1:8020/source/second hdfs://nn2:8020/target


  def copyHDFSToHDFS(spark: SparkSession, srcNasLocation: String, tgtHDFSLocation: String): Unit = {
    val conf = new org.apache.hadoop.conf.Configuration()
    val srcPath = new org.apache.hadoop.fs.Path(srcNasLocation)
    val dstPath = new org.apache.hadoop.fs.Path(tgtHDFSLocation)

    org.apache.hadoop.fs.FileUtil.copy(
      srcPath.getFileSystem(conf),
      srcPath,
      dstPath.getFileSystem(conf),
      dstPath,
      false,
      conf)
  }

  def renameHDFSFile(spark: SparkSession, srcHDFSFile: String, tgtHDFSLocation: String): Unit = {
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val srcPath = new org.apache.hadoop.fs.Path(srcHDFSFile)
    val dstPath = new org.apache.hadoop.fs.Path(tgtHDFSLocation)
    fs.rename(srcPath, dstPath)
  }

  def ListHDFSDirectories(spark: SparkSession, db_table: String): (String, Array[String], Long) = {
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    import spark.implicits._

    val df = spark.sql(s"desc formatted ${db_table}") //convert to dataframe will have 3 columns col_name,data_type,comment
    //var date_filter = df.withColumn("Value",regexp_extract($"constraint_msg","Value: (-?\\ *[0-9]+\\.?[0-9]*(?:[Ee]\\ *-?\\ *[0-9]+)?)",1))

    val path = df.filter(col("col_name") === "Location") //filter on colname
      .collect()(0)(1).toString
    //java.lang.ClassCastException: [Lorg.apache.hadoop.fs.Path; cannot be cast to [Ljava.lang.String
    var files = fs.listStatus(new Path(s"${path}")).filter(_.isDirectory).map(_.getPath) //.asInstanceOf[Array[String]]
    var files_1 = files.map(_.toString)
    files_1 = files_1.filterNot(_.matches(".+hive-staging_hive.+"))
    files_1 = files_1.filterNot(_.matches(".+_SUCCESS+"))
    logger.info("files_1")
    for(file <- files_1) {logger.info(file)}
    val MEGABYTE: Long = 1024L * 1024L
    val folder_size = (fs.getContentSummary(new Path(s"${path}")).getLength) / MEGABYTE
    //fs.listFiles( new Path( path ), true )
    //val filenames = ListBuffer[ String ]( )
    //while (files.hasNext ) filenames += files.next().getPath().toString()
    return (path, files_1, folder_size)
  }

  def getSizeperpartition(spark: SparkSession, db_table: String): (String, Array[String], Long) = {
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val df = spark.sql(s"desc formatted ${db_table}") //convert to dataframe will have 3 columns col_name,data_type,comment
    val path = df.filter(col("col_name") === "Location").collect()(0)(1).toString
    val files = fs.listStatus(new Path(s"${path}")).filter(_.isDirectory).map(_.getPath).asInstanceOf[Array[String]]
    val folder_size = fs.getContentSummary(new Path(s"${path}")).getLength
    //fs.listFiles( new Path( path ), true )
    //val filenames = ListBuffer[ String ]( )
    //while (files.hasNext ) filenames += files.next().getPath().toString()
    return (path, files, folder_size)
  }

  def listAllFilesFolderInDir(spark: SparkSession, filePath: String, recursiveTraverse: Boolean, filePaths: ListBuffer[Path]): ListBuffer[Path] = {
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val files = fs.listStatus(new Path(filePath))
    files.foreach { fileStatus => {
      if (!fileStatus.isDirectory()) {
        if(!fileStatus.toString.matches(".+hive-staging_hive.+")) {
          if (!fileStatus.toString.matches(".+_SUCCESS.+")) {
            filePaths += fileStatus.getPath()
          }
        }
      }
      else {
        listAllFilesFolderInDir(spark,fileStatus.getPath().toString(), recursiveTraverse, filePaths)
      }
    }
    }
    filePaths
  }

  def renameHDFSDir(spark: SparkSession, inputDir: String, target_path: String,partition_keys:Set[String]) = {
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    var filePaths = new ListBuffer[Path]()
    var pathArray = listAllFilesFolderInDir(spark, inputDir, true, filePaths)
    val nbrkeys = partition_keys.size + 1
    var processFolderName = ""
    for (path <- pathArray) {
      var pathSplit = path.toString().split("/")
      var pathSplitSize = pathSplit.size
      var rawFileName = inputDir
      for (i <- nbrkeys to 2 by -1) {
        rawFileName = rawFileName + "/" + pathSplit(pathSplitSize - nbrkeys)
        processFolderName = target_path + "/" + pathSplit(pathSplitSize - nbrkeys)
        var processFolderPath = new Path(processFolderName)
        if (!(fs.exists(processFolderPath)))
          fs.mkdirs(processFolderPath)
      }
      val folder_size = fs.getContentSummary(new Path(rawFileName)).getLength
      rawFileName = rawFileName + "/" + pathSplit(pathSplitSize - 1)
      //var processFolderName = target_path + "/" + pathSplit(pathSplitSize - 2)
      val processFileName = processFolderName + "/" + pathSplit(pathSplitSize - 1)
      logger.info(path)
      logger.info(processFileName)
      logger.info(folder_size)
      //fs.rename(new Path(rawFileName), new Path(processFileName))
    }
  }

  def listAllFolderInDir(spark: SparkSession, filePath: String, recursiveTraverse: Boolean, folderPaths: ListBuffer[Path]): ListBuffer[Path] = {
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val files = fs.listStatus(new Path(filePath))
    files.foreach { fileStatus => {
      if (!fileStatus.isDirectory()) {
        //.trim()
        folderPaths += new Path(filePath)
      }
      else {
        if(!fileStatus.toString.matches(".+hive-staging_hive.+"))
        listAllFolderInDir(spark,fileStatus.getPath().toString(), recursiveTraverse, folderPaths)
      }
    }
    }
    folderPaths
  }

  def getSizeperfile(spark: SparkSession, file: String):  Long = {
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val MEGABYTE: Long = 1024L * 1024L
    val file_size = fs.getContentSummary(new Path(s"${file}")).getLength
    //fs.listFiles( new Path( path ), true )
    //val filenames = ListBuffer[ String ]( )
    //while (files.hasNext ) filenames += files.next().getPath().toString()
    return file_size/MEGABYTE
  }

}
