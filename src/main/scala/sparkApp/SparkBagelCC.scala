package sparkApp

import com.esotericsoftware.kryo.Kryo
import org.apache.spark.bagel.{Combiner, Vertex, Message, Bagel}
import org.apache.spark.serializer.KryoRegistrator
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{HashPartitioner, SparkContext, SparkConf}

/**
 * Created by root on 15-10-9.
 */

class CCUtils extends Serializable {
  def computeWithCombiner(numVertices: Long, epsilon: Double)(
    self: CCVertex, messageSum: Option[Int], superstep: Int
    ): (CCVertex, Array[CCMessage]) = {
    val newValue = messageSum match {
      case Some(msgSum) => math.min(self.value,msgSum)
      case _ => self.value
    }

    val terminate = superstep >= 10

    val outbox: Array[CCMessage] =
      if (!terminate) {
        self.outEdges.map(targetId => new CCMessage(targetId, math.min(targetId,newValue)))
      } else {
        Array[CCMessage]()
      }
    (new CCVertex(newValue, self.outEdges, !terminate), outbox)
  }
}

class CCCombiner extends Combiner[CCMessage, Int] with Serializable {
  def createCombiner(msg: CCMessage): Int =
    msg.value
  def mergeMsg(combiner: Int, msg: CCMessage): Int =
    math.min(combiner,msg.value)
  def mergeCombiners(a: Int, b: Int): Int =
    math.min(a,b)
}

class CCVertex() extends Vertex with Serializable {
  var value: Int = _
  var outEdges: Array[Int] = _
  var active: Boolean = _

  def this(value: Int, outEdges: Array[Int], active: Boolean = true) {
    this()
    this.value = value
    this.outEdges = outEdges
    this.active = active
  }

  override def toString(): String = {
    "CCVertex(value=%d, outEdges.length=%d, active=%s)"
      .format(value, outEdges.length, active.toString)
  }
}

class CCMessage() extends Message[Int] with Serializable {
  var targetId: Int = _
  var value: Int = _

  def this(targetId: Int, value: Int) {
    this()
    this.targetId = targetId
    this.value = value
  }

  override def toString():String={
    "PRMessage(targetId="+targetId+", value="+value+")"
  }
}

class CCKryoRegistrator extends KryoRegistrator {
  def registerClasses(kryo: Kryo) {
    kryo.register(classOf[CCVertex])
    kryo.register(classOf[CCMessage])
  }
}

object SparkBagelCC {

  def main(args:Array[String]): Unit ={

    val sparkConf = new SparkConf().setAppName(args(3)).
      set("spark.serializer","org.apache.spark.serializer.KryoSerializer").
      set("spark.kryo.registrator",classOf[PRKryoRegistrator].getName)

    val threshold = 0.1
    val numPartitions = args(1).toInt
    val usePartitioner = true

    val sc = new SparkContext(sparkConf)

    // Parse the Wikipedia page data into a graph
    val input = sc.textFile(args(0),numPartitions)

    println("Counting vertices...")
    val numVertices = 1001
    println("Done counting vertices.")

    println("Spark:Parsing input file...")

    var vertices = input.map( s => {
      val fields = s.split("\\s+")
      val title = fields(0).toInt
      val target = fields(1).toInt
      (title,target)
    }).groupByKey().map(lines => {
      val id = lines._1
      val list = lines._2.toArray
      (id,new CCVertex(id, list))
    })
    if (usePartitioner) {
      vertices = vertices.partitionBy(new HashPartitioner(numPartitions)).persist(StorageLevel.MEMORY_AND_DISK)
    } else {
      vertices = vertices.persist(StorageLevel.MEMORY_AND_DISK)
    }
    println("Done parsing input file.")

    // Do the computation
    val epsilon = 0.01 / numVertices
    val messages = sc.parallelize(Array[(Int, CCMessage)]())
    val utils = new CCUtils
    val result =
      Bagel.run(
        sc, vertices, messages, combiner = new CCCombiner(),
        numPartitions = numPartitions)(
          utils.computeWithCombiner(numVertices, epsilon))

    result.mapValues(_.value).saveAsTextFile(args(2))
    val count = result.mapValues(_.value).values.distinct().count()
    println("the count of connected components is "+count)
    //result.foreach(t => println(t.toString()))
  }

}
