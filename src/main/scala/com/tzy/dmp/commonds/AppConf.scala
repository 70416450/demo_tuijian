package com.tzy.dmp.commonds

import java.util.Properties

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

  //jdbc连接
  val jdbcURL = "jdbc:mysql://master:3306/hive_db"
  val recResultTable = "hive_db.user_movie_recommandation"
  val mysqlusername = "root"
  val mysqlpassword = "root"
  val prop = new Properties
  prop.put("driver", "com.mysql.jdbc.Driver")
  prop.put("user", mysqlusername)
  prop.put("password", mysqlpassword)
}
