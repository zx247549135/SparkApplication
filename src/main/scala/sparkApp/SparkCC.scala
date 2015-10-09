package sparkApp

import org.apache.spark.{SparkConf, SparkContext}


/**
 * Created by root on 15-10-8.
 */

object SparkCC {

  def main(args:Array[String]){

    val sparkConf = new SparkConf().setAppName(args(4))
    val ctx = new SparkContext(sparkConf)
    val lines = ctx.textFile(args(0),args(1).toInt)
    val iterations = args(2).toInt

    val edges1 = lines.map{ s =>
      val parts = s.split("\\s+")
      (parts(0).toInt, parts(1).toInt)
    }
    val edges2 = edges1.map(eMsg => (eMsg._2,eMsg._1))
    val edges = (edges1 ++ edges2).distinct()

    val g = edges.groupByKey().cache()

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
