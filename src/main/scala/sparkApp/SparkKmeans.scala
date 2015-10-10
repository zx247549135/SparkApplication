package sparkApp

import breeze.linalg.{Vector,DenseVector, squaredDistance}
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel

/**
 * Created by root on 15-10-10.
 */
object SparkKmeans {

  def parseVector(line: String): Vector[Double] = {
    DenseVector(line.split("\\s+").map(_.toDouble))
  }

  def closestPoint(p: Vector[Double], centers: Array[Vector[Double]]): Int = {
    var bestIndex = 0
    var closest = Double.PositiveInfinity

    for (i <- 0 until centers.length) {
      val tempDist = squaredDistance(p, centers(i))
      if (tempDist < closest) {
        closest = tempDist
        bestIndex = i
      }
    }

    bestIndex
  }

  def main(args: Array[String]) {

    if (args.length < 3) {
      System.err.println("Usage: SparkKMeans <file> <k> <convergeDist>")
      System.exit(1)
    }

    val sparkConf = new SparkConf().setAppName(args(3))
    val sc = new SparkContext(sparkConf)
    val lines = sc.textFile(args(0))
    val data = lines.map(parseVector _).persist(StorageLevel.MEMORY_AND_DISK)
    val K = args(1).toInt
    val convergeDist = args(2).toDouble

    val kPoints = data.takeSample(withReplacement = false, K, 42).toArray
    var tempDist = 1.0
    var step = 1

    while(tempDist > convergeDist) {
      val closest = data.map (p => (closestPoint(p, kPoints), (p, 1)))

      val pointStats = closest.reduceByKey{case ((x1, y1), (x2, y2)) => (x1 + x2, y1 + y2)}

      val newPoints = pointStats.map {pair =>
        (pair._1, pair._2._1 * (1.0 / pair._2._2))}.collectAsMap()

      tempDist = 0.0
      for (i <- 0 until K) {
        tempDist += squaredDistance(kPoints(i), newPoints(i))
      }

      for (newP <- newPoints) {
        kPoints(newP._1) = newP._2
      }
      println("Finished iteration "+step+" (delta = " + tempDist + ")")
      step += 1
    }

    println("Final centers:")
    kPoints.foreach(println)
    sc.stop()
  }
}
