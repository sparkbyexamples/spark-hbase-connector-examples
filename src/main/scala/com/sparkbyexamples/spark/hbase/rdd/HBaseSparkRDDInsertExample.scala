package com.sparkbyexamples.spark.hbase.rdd

import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.hadoop.hbase.spark.HBaseRDDFunctions._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.spark.sql.SparkSession

object HBaseSparkRDDInsertExample {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("SparkByExamples.com")
      .getOrCreate()

    val hbaseConf = HBaseConfiguration.create()

    val hbaseContext = new HBaseContext(spark.sparkContext, hbaseConf)
    try {
      val rowKey = "5"
      val rdd = spark.sparkContext.parallelize(Array(
        (Bytes.toBytes(rowKey),
          Array((Bytes.toBytes("person"), Bytes.toBytes("firstName"), Bytes.toBytes("Raman")))),
        (Bytes.toBytes(rowKey),
          Array((Bytes.toBytes("person"), Bytes.toBytes("lastName"), Bytes.toBytes("Upalla")))),
        (Bytes.toBytes(rowKey),
          Array((Bytes.toBytes("person"), Bytes.toBytes("middleName"), Bytes.toBytes("K")))),
        (Bytes.toBytes(rowKey),
          Array((Bytes.toBytes("address"), Bytes.toBytes("addressLine"), Bytes.toBytes("3456 main")))),
        (Bytes.toBytes(rowKey),
          Array((Bytes.toBytes("address"), Bytes.toBytes("city"), Bytes.toBytes("Orlando")))),
        (Bytes.toBytes(rowKey),
          Array((Bytes.toBytes("address"), Bytes.toBytes("state"), Bytes.toBytes("FL")))),
        (Bytes.toBytes(rowKey),
          Array((Bytes.toBytes("address"), Bytes.toBytes("zipCode"), Bytes.toBytes("45235"))))
      ))

      //make sure you import import org.apache.hadoop.hbase.spark.HBaseRDDFunctions._
      rdd.hbaseBulkPut(hbaseContext, TableName.valueOf("employee"),
        (putRecord) => {
          val put = new Put(putRecord._1)

          putRecord._2.foreach((putValue) => put.addColumn(putValue._1, putValue._2,
            putValue._3))
          put
        })
    }finally{
      spark.sparkContext.stop()
    }
  }
}
