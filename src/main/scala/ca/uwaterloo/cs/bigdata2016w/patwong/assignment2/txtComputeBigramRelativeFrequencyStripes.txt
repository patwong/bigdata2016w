package ca.uwaterloo.cs.bigdata2016w.patwong.assignment2

//import com.sun.istack.internal.logging.Logger

import ca.uwaterloo.cs.bigdata2016w.patwong.assignment2.Tokenizer
import collection.mutable.HashMap

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.{Partitioner, SparkContext, SparkConf}
import org.rogach.scallop._

class Conf5(args: Seq[String]) extends ScallopConf(args) with Tokenizer {
  mainOptions = Seq(input, output, reducers)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  val reducers = opt[Int](descr = "number of reducers", required = false, default = Some(1))
  val imc = opt[Boolean](descr = "use in-mapper combining", required = false)
}

object ComputeBigramRelativeFrequencyStripes extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())

  def staradd(s: String): (String, String) = {
    val v = (s, "*")
    return v
  }

  def main(argv: Array[String]) {
    val args = new Conf5(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Number of reducers: " + args.reducers())
    log.info("Use in-mapper combining: " + args.imc())

    val conf = new SparkConf().setAppName("ComputeBigramRelativeFrequencyStripes")
    val sc = new SparkContext(conf)

    val textFile = sc.textFile(args.input())
    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    var margs1= 0.0f

    class hadoopPart(numParts: Int) extends Partitioner {
      def numPartitions: Int = numParts
      def getPartition(key: Any): Int = {
        //need to cast key as a tuple
        val k12 = key match {
          case tuple @ (a: String, b: String) => (a, b)
        }
        return k12._1.hashCode() & Int.MaxValue % numParts
      }
    }

    //stripes: the Map of Maps
    //a_stripe: a Map of stripes
    var stripes:Map[String, Map[String, Int]] = Map()

    val counts = textFile
      .flatMap(line => {
        val tokens = tokenize(line)
/*        if (tokens.length > 1) {
//          val a2 = tokens.sliding(2).map(a => (a(0), a(1))).toList
//          val a1 = tokens.map(p => staradd(p)).toList.dropRight(1)
//          a1 ::: a2
        } else {
          List()
        }*/
        if(tokens.length > 1) {
          for(cc1 <- 1 to (tokens.length - 1)) {
            val prev = tokens(cc1-1)
            val curr = tokens(cc1)
            if(stripes.contains(prev)) {
              var a_stripe = stripes(prev)
              if (a_stripe.contains(cur)) {
                val curs_val = a_stripe.apply(curr) + 1.0f
                a_stripe += (curr -> curs_val)
                stripes += (prev -> a_stripe)
              } else {
                a_stripe += (curr -> 1.0f)
                stripes += (prev -> a_stripe)
              }
            } else {
              val a_stripe = Map(curr -> 1.0f)
              stripes += (prev -> a_stripe)
            }
          }
          stripes
        }
      })
      .map(bigram => (bigram, 1))
      .reduceByKey(_ + _)
      .sortByKey(true, args.reducers())
      .partitionBy(new hadoopPart(args.reducers()))
      .map(kayvee => {
        val k1 = kayvee._1
        if (k1._2 == "*") {
          margs1 = kayvee._2
          kayvee
        } else {
          (kayvee._1, kayvee._2.toFloat / margs1)
        }
      })
    counts.saveAsTextFile(args.output())
  }
}
