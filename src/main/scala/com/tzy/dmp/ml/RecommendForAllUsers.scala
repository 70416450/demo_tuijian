package com.tzy.dmp.ml


import com.tzy.dmp.caseclass.Result
import com.tzy.dmp.commonds.AppConf
import org.apache.spark._
import org.apache.spark.mllib.recommendation._
import org.apache.spark.sql._

object RecommendForAllUsers extends AppConf {
  //在集群中提交这个main进行运行的时候，需要通过--jars来把mysql的驱动jar包所在的路径添加到classpath
  //请大家按照pom.xml中指定的版本，安装hbase1.2.6以及phoenix4.9
  //如果需要写入到Phoenix,则也需要添加一些相关的jar包添加到classpath
  //推荐大家通过maven assembly:assembly的方式打成jar包，然后在集群中运行
  def main(args: Array[String]) {
    val users = hc.sql("select distinct(userId) from trainingData order by userId asc")
    val allusers = users.rdd.map(_.getInt(0)).toLocalIterator

    //方法1，可行，但是效率不高
    val modelpath = "/tmp/BestModel/0.8508154745493134"
    val model = MatrixFactorizationModel.load(sc, modelpath)
    while (allusers.hasNext) {
      val rec = model.recommendProducts(allusers.next(), 5)
      writeRecResultToMysql(rec, sqlContext, sc)
      //writeRecResultToSparkSQL(rec)，写入到SPARK-SQL-Hive
      writeRecResultToHbase(rec, sqlContext, sc)
    }


    def writeRecResultToMysql(uid: Array[Rating], sqlContext: SQLContext, sc: SparkContext) {
      val uidString = uid.map(x => x.user.toString() + "|"
        + x.product.toString() + "|" + x.rating.toString())

      import sqlContext.implicits._
      val uidDFArray = sc.parallelize(uidString)
      val uidDF = uidDFArray.map(_.split("|")).map(x => Result(x(0).trim().toInt, x(1).trim.toInt, x(2).trim().toDouble)).toDF
      uidDF.write.mode(SaveMode.Overwrite).jdbc(jdbcURL, recResultTable, prop)
    }

    //把推荐结果写入到phoenix+hbase,通过DF操作，不推荐。
    def writeRecResultToHbase(uid: Array[Rating], sqlContext: SQLContext, sc: SparkContext) {
      val uidString = uid.map(x => x.user.toString() + "|"
        + x.product.toString() + "|" + x.rating.toString())
      import sqlContext.implicits._
      val uidDF = sc.parallelize(uidString).map(_.split("|")).map(x => Result(x(0).trim().toInt, x(1).trim.toInt, x(2).trim().toDouble)).toDF
      //zkUrl需要大家按照自己的hbase配置的zookeeper的url来设置
      uidDF.save("org.apache.phoenix.spark", SaveMode.Overwrite, Map("table" -> "phoenix_rec", "zkUrl" -> "localhost:2181"))
    }
  }
}