package com.tzy.dmp.commonds

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

trait AppConf {
  val CLUSTER = "spark://master:7077"
  val LOCAL = "local[*]"
  val conf = new SparkConf().setMaster(CLUSTER).setAppName("ETL")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)
  val hc = new HiveContext(sc)
}
