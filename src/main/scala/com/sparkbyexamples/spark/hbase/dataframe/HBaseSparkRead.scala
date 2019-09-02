package com.sparkbyexamples.spark.hbase.dataframe

import org.apache.hadoop.hbase.spark.datasources.HBaseTableCatalog
import org.apache.spark.sql.SparkSession

// https://mvnrepository.com/artifact/org.apache.hbase/hbase-spark

/**
 * References:
 * https://hbase.apache.org/book.html#_sparksql_dataframes
 * https://github.com/hortonworks-spark/shc
 * https://mvnrepository.com/artifact/org.apache.hbase/hbase-spark
 * https://mvnrepository.com/artifact/org.apache.hbase/hbase-client
 * https://github.com/hortonworks-spark/shc/tree/master/examples/src/main/scala/org/apache/spark/sql/execution/datasources/hbase
 */
object HBaseSparkRead {

  def main(args: Array[String]): Unit = {

    def catalog =
      s"""{
         |"table":{"namespace":"default", "name":"employee"},
         |"rowkey":"key",
         |"columns":{
         |"key":{"cf":"rowkey", "col":"key", "type":"string"},
         |"fName":{"cf":"person", "col":"firstName", "type":"string"},
         |"lName":{"cf":"person", "col":"lastName", "type":"string"},
         |"mName":{"cf":"person", "col":"middleName", "type":"string"},
         |"addressLine":{"cf":"address", "col":"addressLine", "type":"string"},
         |"city":{"cf":"address", "col":"city", "type":"string"},
         |"state":{"cf":"address", "col":"state", "type":"string"},
         |"zipCode":{"cf":"address", "col":"zipCode", "type":"string"}
         |}
         |}""".stripMargin

    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("SparkByExamples.com")
      .getOrCreate()

    import spark.implicits._

    // Reading from HBase to DataFrame
    val hbaseDF = spark.read
      .options(Map(HBaseTableCatalog.tableCatalog -> catalog))
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()

    //Display Schema from DataFrame
    hbaseDF.printSchema()

    //Collect and show Data from DataFrame
    hbaseDF.show(false)

    //Applying Filters
    hbaseDF.filter($"key" === "1" && $"state" === "FL")
      .select("key", "fName", "lName")
      .show()

    //Create Temporary Table on DataFrame
    hbaseDF.createOrReplaceTempView("employeeTable")

    //Run SQL
    spark.sql("select * from employeeTable where fName = 'Amaya' ").show

  }
}