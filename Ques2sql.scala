package lab
/*

import org.apache.spark._
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.WrappedArray
//import spark.implicits._
import org.apache.spark.sql.functions._
*/
object Ques2sql {
/*
  def main(args: Array[String]) {

    val sc = SparkSession.builder.master("local").appName("Ques2sql").getOrCreate()
    import sc.implicits._
    //val sc = sc.SparkContext.SparkContext("local[*]", "Q1")
    val lines = sc.sparkContext.textFile(args(0))
    val rdd = lines.map(parse).filter(line => line._1 != "-1")
    val rddPair = rdd.flatMap(x => x._2._2.map(y => if (x._1.toLong < y.toLong) {
      ((x._1, y), x._2)
    } else {
      ((y, x._1), x._2)
    }))
    val reduceRdd = rddPair.reduceByKey((x, y) => ((x._1 + y._1), x._2.intersect(y._2))).filter(v => v._2._1 != 1)
    val reduce_rddPair_DF = reduceRdd.map(r => (r._1._1,r._1._2,r._2._2)).filter(l => l._2 != List()).toDF("FriendA","FriendB", "Mutual")
    //reduce_rddPair.show()
    reduce_rddPair_DF.createOrReplaceTempView("soc")
    val userData = sc.sparkContext.textFile(args(1))
    val userdataPairs = userData.map(line => line.split(',')).map(line => (line(0),line.slice(1, 8).mkString(",").replace(',','\t'))).toDF("userid","details")
    userdataPairs.createOrReplaceTempView("userdata")
    val sqlDF = sc.sql("SELECT size(Mutual),A.details,B.details FROM soc f INNER JOIN userdata A INNER JOIN userdata B  where f.FriendA = A.userid and f.FriendB = B.userid order by size(Mutual) desc limit 10").collect()
    val finalRDD=sc.sparkContext.parallelize(sqlDF).map(col=>col(0)+"\t"+col(1)+"\t"+col(2))
    finalRDD.saveAsTextFile(args(2))


  }

  def parse(line: String): (String, (Long, List[String])) = {
    val fields = line.split("\t")
    if (fields.length == 2)
    {
      val user = fields(0)
      val friendsList = fields(1).split(",").toList
      return (user, (1L, friendsList))
    }
    else
    {
      return ("-1", (-1L, List()))
    }
  }
  * 
  */
}