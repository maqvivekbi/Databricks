// Databricks notebook source
// MAGIC %md
// MAGIC Read CSV File from DBFS, process it and write to Blob Storage
// MAGIC =============================================================

// COMMAND ----------

// MAGIC %md
// MAGIC ###Read from csv file and store to dataframe 

// COMMAND ----------

val revenue = sqlContext.read.format("csv")
  .option("header", "true")
  .option("inferSchema", "true")
  .load("/FileStore/tables/Sample.csv")
display (revenue)

// COMMAND ----------

// MAGIC %md
// MAGIC ###Group Revenue by Area

// COMMAND ----------

var aggregate = revenue.groupBy("AccountArea").sum("ProductRevenue").withColumnRenamed("sum(ProductRevenue)", "Revenue")
display(aggregate)

// COMMAND ----------

// MAGIC %md
// MAGIC ###Write aggregated output to csv format

// COMMAND ----------

aggregate
    .write
    .mode("overwrite")
    //.option("header", true) //headers will cause duplicate header rows in consolidated csv file
    .csv("/FileStore/tables/Resultcsv")

// COMMAND ----------

// MAGIC %md
// MAGIC ###display data from csv file

// COMMAND ----------

val result = sqlContext.read.format("csv")
  //.option("header", "true")
  .option("inferSchema", "true")
  .load("/FileStore/tables/Resultcsv")

display(result)

// COMMAND ----------

// MAGIC %md
// MAGIC ### function to merge multiple csv file part to single csv file

// COMMAND ----------

import scala.util.Try
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.IOUtils
import java.io.IOException

def customCopyMerge(
    srcFS: FileSystem,
    srcDir: Path,
    dstFS: FileSystem, 
    dstFile: Path,
    deleteSource: Boolean, 
    conf: Configuration
): Boolean = {
 
  if (!srcFS.exists(srcDir))
    throw new IOException("Source $srcDir does not exists")
 
  // Source path is expected to be a directory:
  if (srcFS.getFileStatus(srcDir).isDirectory()) {
 
    val outputFile = dstFS.create(dstFile, true) //if true file will be overwritten
    Try {
      srcFS
        .listStatus(srcDir)
        .sortBy(_.getPath.getName)
        .collect {
          case status if status.isFile() && status.getPath().toString.endsWith(".csv")=>
            val inputFile = srcFS.open(status.getPath())
            println(status.getPath())
            Try(IOUtils.copyBytes(inputFile, outputFile, conf, false))
            inputFile.close()
        }
    }
    outputFile.close()
 
    if (deleteSource) srcFS.delete(srcDir, true) else true
  }
  else false
}

// COMMAND ----------

// MAGIC %md
// MAGIC ###mount blob storage container with dbfs

// COMMAND ----------

/*spark.conf.set(
  "fs.azure.account.key.stagingadls.dfs.core.windows.net",
  dbutils.secrets.get(scope = "dev", key = "adlskey"))
 */
dbutils.fs.mount(
  source = "wasbs://databricks@hygienepoc.blob.core.windows.net/csvfiles",
  mountPoint = "/mnt/hygieneblob",
  extraConfigs = Map("fs.azure.account.key.hygienepoc.blob.core.windows.net" -> "KJQ30ec3LO9J6ab2amC3dpjtcTNGcUCfW6RGEwWhyX0pSgCrwkn6oNKMkIDBVlaIzQrzIuvzU0rKz9vKylxbVA=="))///dbutils.secrets.get(scope = "<scope-name>", key = "<key-name>")))

// COMMAND ----------

// MAGIC %md
// MAGIC ### Call merge function to generate single csv output file in Azure Blob

// COMMAND ----------


val conf = sc.hadoopConfiguration
    val fs = FileSystem.get(conf)
 
customCopyMerge(fs, new Path("/FileStore/tables/Resultcsv"),  fs, new Path("/mnt/hygieneblob/Result.csv"), false, conf)
///FileStore/tables/Result.csv
///mnt/hygieneblob/Result.csv

// COMMAND ----------

//%fs rm -r "/FileStore/tables/Result1.csv"

// COMMAND ----------

// MAGIC %md
// MAGIC ###Read data from Azure Bolb csv file

// COMMAND ----------

val resultcsv = sqlContext.read.format("csv")
  //.option("header", "true")
  .option("inferSchema", "true")
  .load("/mnt/hygieneblob/Result.csv")
display(resultcsv)