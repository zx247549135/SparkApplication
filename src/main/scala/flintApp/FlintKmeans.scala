package flintApp

import java.io.{DataOutputStream, ByteArrayOutputStream}

import org.apache.hadoop.io.WritableComparator
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkContext, SparkConf}


/**
 * Created by root on 15-10-10.
 */

class KmeansChunk(numDest:Int) extends ByteArrayOutputStream() with Serializable{ self =>

  def getOriginal()= new Iterator[Array[Double]]{
    var offset = 0

    override def hasNext = offset < self.count

    override def next():Array[Double]={
      if(!hasNext) Iterator.empty.next()
      else{
        val result = new Array[Double](numDest)
        for(i <- 0 until numDest){
          result.update(i,WritableComparator.readDouble(buf,offset))
          offset += 8
        }
        result
      }
    }
  }

  def getPointStats(K:Int,kPoints:Array[Array[Double]]):Iterator[Array[(Array[Double],Int)]]={
    var offset = 0
    val result = new Array[(Array[Double],Int)](K)

    for(i <- 0 until K){
      result.update(i,(new Array[Double](numDest),0))
    }

    def squaredDistance(startOffset:Int,second:Array[Double]): Double ={
      var tmpOffset = startOffset
      var result = 0.0
      for( i <- 0 until second.length){
        result += (WritableComparator.readDouble(buf,tmpOffset) - second(i))*(WritableComparator.readDouble(buf,tmpOffset)-second(i))
        tmpOffset += 8
      }
      result
    }

    def computeClosest(startOffset:Int):Int={
      var bestIndex = 0
      var minDistance = Double.PositiveInfinity

      for( i <- 0 until kPoints.length){
        val tempDist = squaredDistance(startOffset,kPoints(i))
        if(tempDist < minDistance){
          minDistance = tempDist
          bestIndex = i
        }
      }
      bestIndex
    }

    while(offset < self.count){
      val closestPoint = computeClosest(offset)
      for(i <- 0 until numDest){
        result(closestPoint)._1(i) += WritableComparator.readDouble(buf,offset)
        offset += 8
      }
      result.update(closestPoint,(result(closestPoint)._1,result(closestPoint)._2+1))
    }

    Iterator(result)

  }

}

object FlintKmeans {

  def getSquarDistance(first:Array[Double],second:Array[Double]):Double={
    var result = 0.0
    for(i <- 0 until first.length){
      result += (first(i)-second(i))*(first(i)-second(i))
    }
    result
  }

  def main(args: Array[String]) {

    if (args.length < 3) {
      System.err.println("Usage: SparkKMeans <file> <k> <convergeDist>")
      System.exit(1)
    }

    val sparkConf = new SparkConf().setAppName(args(3))
    val sc = new SparkContext(sparkConf)
    val lines = sc.textFile(args(0))
    val K = args(1).toInt
    val convergeDist = args(2).toDouble
    val numDest = args(4).toInt

    val data = lines.mapPartitions(dIter => {
      val chunk = new KmeansChunk(numDest)
      val dos = new DataOutputStream(chunk)
      while(dIter.hasNext){
        val line = dIter.next()
        val parts = line.split("\\s+")
        parts.map(t => dos.writeDouble(t.toDouble))
      }
      Iterator(chunk)
    }).persist(StorageLevel.MEMORY_AND_DISK)

    data.foreach(_ => Unit)

    var kPoints = data.mapPartitions( dIter => {
      val chunk = dIter.next()
      chunk.getOriginal()
    }).takeSample(withReplacement = false, K, 42).toArray

    var terminate = 1.0
    var step = 1

    while(terminate > convergeDist){

      val pointStats = data.mapPartitions( dIter => {
        val chunk = dIter.next()
        chunk.getPointStats(K,kPoints)
      }).reduce((v1,v2) => {
        val result = new Array[(Array[Double],Int)](K)
        for(i <- 0 until K){
          for(j <- 0 until numDest){
            result(i)._1.update(i,v1(i)._1(i)+v2(i)._1(i))
          }
          result.update(i,(result(i)._1,v1(i)._2+v2(i)._2))
        }
        result
      })
      val newPoints = pointStats.map( pair => {
        for(i <- 0 until numDest){
          pair._1(i) = pair._1(i) / pair._2
        }
        pair._1
      })

      terminate = 0.0
      for(i <- 0 until K){
        terminate += getSquarDistance(newPoints(i),kPoints(i))
      }

      kPoints = newPoints

      println("Finished iteration "+step+" (delta = " + terminate + ")")
      step += 1
    }

    println("Final centers:")
    kPoints.foreach(t => println(t.mkString(";")))
    sc.stop()
  }

}
