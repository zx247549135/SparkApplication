package flintApp

import java.io.{DataOutputStream, ByteArrayOutputStream}

import org.apache.hadoop.io.WritableComparator
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by root on 15-9-26.
 */

class WordChunk extends ByteArrayOutputStream{ self =>
  def getFilter(regex:String)= new Iterator[String]{
    var offset = 0
    val regexLength = regex.length
    val regexChars = regex.toCharArray
    var over = false
    var changeVertex = false
    var lastVertex = false
    var resultVertext = ""
    var keepVertext = ""
    var first = true

    override def hasNext = offset < self.count || !lastVertex

    override def next()= {

      if (!hasNext) Iterator.empty.next()
      else {
        resultVertext = keepVertext
        if(first) {
          var resutl = new String("")
          changeVertex = false
          while (!over && !changeVertex) {
            val length = WritableComparator.readInt(buf, offset)
            offset += 4
            var matched = true
            if (length < regexLength)
              offset += length
            else {
              val regxoffset = offset + (length - regexLength)
              for (i <- 0 until regexLength if matched) {
                if (regexChars(i) != (buf(regxoffset + i).asInstanceOf[Char]))
                  matched = false
              }
              if (matched != false) {
                for (i <- 0 until length if matched) {
                  resutl += buf(offset + i).asInstanceOf[Char]
                }
                changeVertex = true
              }
              matched = true
              offset += length
            }
            if (offset >= self.count) over = true
          }
          if (resultVertext == "") resultVertext = resutl
          first = false
        }
        if(!lastVertex) {
          var nextresutl = new String("")
          changeVertex = false
          while (!over && !changeVertex) {
            val length = WritableComparator.readInt(buf, offset)
            offset += 4
            var matched = true
            if (length < regexLength)
              offset += length
            else {
              val regexoffset = offset + (length - regexLength)
              for (i <- 0 until regexLength if matched) {
                if (regexChars(i) != (buf(regexoffset + i).asInstanceOf[Char]))
                  matched = false
              }
              if (matched != false) {
                for (i <- 0 until length if matched) {
                  nextresutl += buf(offset + i).asInstanceOf[Char]
                }
                changeVertex = true
              }
              matched = true
              offset += length
            }
            if (offset >= self.count) over = true
          }
          if (nextresutl == "") lastVertex = true
          else keepVertext = nextresutl
        }
        resultVertext
      }
    }
  }
}

object FlintFilter {
  def main(args:Array[String]){
    val sparkConf = new SparkConf().setAppName(args(3))
    val sparkContext = new SparkContext(sparkConf)
    val lines = sparkContext.textFile(args(0))
    val chunkWords = lines.mapPartitions(iter => {
      val chunk = new WordChunk
      val dos = new DataOutputStream(chunk)
      for( line <- iter){
        val parts = line.split("\\s+")
        parts.foreach(t => {
          dos.writeInt(t.length)
          dos.writeBytes(t)
        })
      }
      Iterator(chunk)
    })
    val finalWords = chunkWords.mapPartitions( iter => {
      val chunk = iter.next()
      chunk.getFilter(args(2))
    })

    finalWords.saveAsTextFile(args(1))
    //finalWords.foreach(t => println(t))
  }
}
