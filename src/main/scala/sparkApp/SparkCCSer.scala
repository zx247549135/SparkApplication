package sparkApp

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by root on 15-10-9.
 */
object SparkCCSer {
  def main(args:Array[String]){

    val sparkConf = new SparkConf().setAppName(args(4))
    val ctx = new SparkContext(sparkConf)
    val lines = ctx.textFile(args(0),args(1).toInt)
    val iterations = args(2).toInt

    val edges = lines.map{ s =>
      val parts = s.split("\\s+")
      (parts(0).toInt, parts(1).toInt)
    }

    val g = edges.groupByKey().persist(StorageLevel.MEMORY_AND_DISK_SER)

    var messages = g.map( eMsg => {
      (eMsg._1,eMsg._1)
    })

    for( i <- 1 to iterations){

      val newVertices = g.join(messages).values.flatMap( value => {
        value._1.map(vtx => (vtx,math.min(vtx,value._2)))
      })

      messages = newVertices.reduceByKey((v1,v2) => math.min(v1,v2))

    }

    messages.saveAsTextFile(args(3))
    val result = messages.map(_._2).distinct.count()
    println("the count of connected components is "+result)

  }
}
