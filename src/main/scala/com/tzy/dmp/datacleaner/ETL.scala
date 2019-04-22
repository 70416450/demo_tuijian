package com.tzy.dmp.datacleaner

import com.tzy.dmp.caseclass._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

object ETL {
  def main(args: Array[String]): Unit = {
    val CLUSTER = "spark://master:7077"
    val LOCAL = "local[*]"
    val conf = new SparkConf().setMaster(CLUSTER).setAppName("ETL")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val hc = new HiveContext(sc)
    val minPartitions = 8
    import sqlContext.implicits._
    val links: DataFrame = sc.textFile("/data/links.txt", minPartitions).filter(!_.endsWith(","))
      .map(_.split(",")).map(x => Links(x(0).trim.toInt, x(1).trim.toInt, x(0).trim.toInt)).toDF()
    val movies = sc.textFile("/data/movies.txt", minPartitions).filter { !_.endsWith(",") }
      .map(_.split(",")).map(x => Movies(x(0).trim().toInt, x(1).trim(), x(2).trim())).toDF()

    val ratings = sc.textFile("/data/ratings.txt", minPartitions).filter { !_.endsWith(",") }
      .map(_.split(",")).map(x => Ratings(x(0).trim().toInt, x(1).trim().toInt, x(2).trim().toDouble, x(3).trim().toInt)).toDF()

    val tags = sc.textFile("/data/tags.txt", minPartitions).filter { !_.endsWith(",") }
      .map(x=>rebuild(x)).map(_.split(",")).map(x => Tags(x(0).trim().toInt, x(1).trim().toInt, x(2).trim(), x(3).trim().toInt)).toDF()
    //在SQLCONTEXT下只能构建临时表
    //      links.registerTempTable("links0629")
    //通过数据写入到HDFS，将表存到hive中
    //links
    links.write.mode(SaveMode.Overwrite).parquet("/tmp/links")
    hc.sql("drop table if exists links")
    hc.sql("create table if not exists links(movieId int,imdbId int,tmdbId int) stored as parquet")
    hc.sql("load data inpath '/tmp/links' overwrite into table links")

    //movies
    movies.write.mode(SaveMode.Overwrite).parquet("/tmp/movies")
    hc.sql("drop table if exists movies")
    hc.sql("create table if not exists movies(movieId int,title string,genres string) stored as parquet")
    hc.sql("load data inpath '/tmp/movies' overwrite into table movies")

    //ratings
    ratings.write.mode(SaveMode.Overwrite).parquet("/tmp/ratings")
    hc.sql("drop table if exists ratings")
    hc.sql("create table if not exists ratings(userId int,movieId int,rating double,timestamps int) stored as parquet")
    hc.sql("load data inpath '/tmp/ratings' overwrite into table ratings")

    //tags
    tags.write.mode(SaveMode.Overwrite).parquet("/tmp/tags")
    hc.sql("drop table if exists tags")
    hc.sql("create table if not exists tags(userId int,movieId int,tag string,timestamps int) stored as parquet")
    hc.sql("load data inpath '/tmp/tags' overwrite into table tags")


  }
  private def rebuild(input:String):String = {
    val a = input.split(",")
    val head = a.take(2).mkString(",")
    val tail = a.takeRight(1).mkString
    val b = a.drop(2).dropRight(1).mkString.replace("\"", "")
    val output = head + "," + b + "," + tail
    output
  }
}
