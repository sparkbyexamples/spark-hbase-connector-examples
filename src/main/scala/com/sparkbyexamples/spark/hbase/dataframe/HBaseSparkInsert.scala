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
object HBaseSparkInsert {

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


    val data = Seq(Employee("1", "Abby", "Smith", "K", "3456 main", "Orlando", "FL", "45235"),
      Employee("2", "Amaya", "Williams", "L", "123 Orange", "Newark", "NJ", "27656"),
      Employee("3", "Alchemy", "Davis", "P", "Warners", "Sanjose", "CA", "34789"))

    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("SparkByExamples.com")
      .getOrCreate()

    import spark.implicits._
    val df = spark.sparkContext.parallelize(data).toDF

    df.write.options(
      Map(HBaseTableCatalog.tableCatalog -> catalog, HBaseTableCatalog.newTable -> "4"))
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()



  }
}