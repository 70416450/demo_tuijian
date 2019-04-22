package com.tzy.dmp.datacleaner

import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.sql.hive.HiveContext

object RatingData {
  def main(args: Array[String]): Unit = {
    val CLUSTER = "spark://master:7077"
    val LOCAL = "local[*]"
    val conf = new SparkConf().setMaster(CLUSTER).setAppName("ETL")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val hc = new HiveContext(sc)
    val minPartitions = 8
    import sqlContext.implicits._
    //RDD[Rating]需要从原始表中提取userid,movieid,rating数据
    //并把这些数据切分成训练集和测试集数据
    val ratings = hc.sql("cache table ratings")
    val count = hc.sql("select count(*) from ratings").first().getLong(0).toInt
    val percent = 0.6
    val trainingdatacount = (count * percent).toInt
    val testdatacount = (count * (1 - percent)).toInt

    //用scala feature:String Interpolation来往SQL语句中传递参数
    //order by limit的时候，需要注意OOM的问题
    val trainingDataAsc = hc.sql(s"select userId,movieId,rating from ratings order by timestamps asc")
    trainingDataAsc.write.mode(SaveMode.Overwrite).parquet("/tmp/trainingDataAsc")
    hc.sql("drop table if exists trainingDataAsc")
    hc.sql("create table if not exists trainingDataAsc(userId int,movieId int,rating double) stored as parquet")
    hc.sql("load data inpath '/tmp/trainingDataAsc' overwrite into table trainingDataAsc")

    val trainingDataDesc = hc.sql(s"select userId,movieId,rating from ratings order by timestamps desc")
    trainingDataDesc.write.mode(SaveMode.Overwrite).parquet("/tmp/trainingDataDesc")
    hc.sql("drop table if exists trainingDataDesc")
    hc.sql("create table if not exists trainingDataDesc(userId int,movieId int,rating double) stored as parquet")
    hc.sql("load data inpath '/tmp/trainingDataDesc' overwrite into table trainingDataDesc")

    val trainingData = hc.sql(s"select * from trainingDataAsc limit $trainingdatacount")
    trainingData.write.mode(SaveMode.Overwrite).parquet("/tmp/trainingData")
    hc.sql("drop table if exists trainingData")
    hc.sql("create table if not exists trainingData(userId int,movieId int,rating double) stored as parquet")
    hc.sql("load data inpath '/tmp/trainingData' overwrite into table trainingData")

    val testData = hc.sql(s"select * from trainingDataDesc limit $testdatacount")
    testData.write.mode(SaveMode.Overwrite).parquet("/tmp/testData")
    hc.sql("drop table if exists testData")
    hc.sql("create table if not exists testData(userId int,movieId int,rating double) stored as parquet")
    hc.sql("load data inpath '/tmp/testData' overwrite into table testData")

  }
}
