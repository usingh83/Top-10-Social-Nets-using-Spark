package lab

import org.apache.spark._


object Ques2
{
  def main(args: Array[String])
  {
    val sc = new SparkContext("local[*]", "Ques2")
    val lines = sc.textFile(args(0))
    val rdd = lines.map(parse).filter(line => line._1 != "-1")
    val rddPair = rdd.flatMap(x => x._2._2.map(y => if (x._1.toLong < y.toLong) {
      ((x._1, y), x._2)
    }
    else {
      ((y, x._1), x._2)
    }))
    val reduceRdd = rddPair.reduceByKey((x, y) => ((x._1 + y._1), x._2.intersect(y._2))).filter(v => v._2._1 != 1)
    val reduce_rddPair = reduceRdd.map(r => (r._1, r._2._2)).filter(l=>l._2!=List())
    val output=reduce_rddPair.map(r => (r._1, r._2.size)).sortBy(_._2, false).take(10)
    val outputParallelize = sc.parallelize(output)
    val keysAB = outputParallelize.keys.toString()

    val userData = sc.textFile(args(1))

    val userdataPairs = userData.map(line => line.split(',')).map(line => (line(0),line.slice(1, 8).mkString(",")))
    val rdd1Broadcast = sc.broadcast(userdataPairs.collectAsMap())
    val joined = outputParallelize.mapPartitions({ iter =>
      val m = rdd1Broadcast.value
      for {
        ((t, w), u) <- iter
        if (m.contains(t) && m.contains(w))
      } yield (u.toString+"\t"+ m.get(t).get.replace(',','\t')+"\t"+m.get(w).get.replace(',','\t'))
    }, preservesPartitioning = true)
    joined.saveAsTextFile(args(2))
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
}