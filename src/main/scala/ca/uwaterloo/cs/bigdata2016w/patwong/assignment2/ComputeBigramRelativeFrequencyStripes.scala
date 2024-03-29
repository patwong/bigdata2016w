package ca.uwaterloo.cs.bigdata2016w.patwong.assignment2

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

    //stripes: the Map of Maps
    //a_stripe: a Map of stripes
    val counts = textFile
      .flatMap(line => {
        var stripes:Map[String, Map[String, Float]] = Map()
        val tokens = tokenize(line)
        if(tokens.length > 1) {
          //self-note: can only modify vars, not val in Scala
          // map += (k,v) only if map is var
          for(cc1 <- 1 until tokens.length) {
            val prev = tokens(cc1-1)
            val curr = tokens(cc1)
            if(stripes.contains(prev)) {
              var a_stripe = stripes(prev)
              if (a_stripe.contains(curr)) {
                val curs_val = a_stripe(curr) + 1.0f
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
//          println(stripes)
          stripes.keys.map(vall => (vall, stripes(vall)))
        } else {
          List()
        }
      })
      //map all keys to their stripe values -  done above?
      //combiner?
      .reduceByKey( (x:Map[String,Float], y:Map[String,Float]) => {   //does the work of the combiner
        var a_stripe = y
        for(kees:String <- x.keys){
          if(y.contains(kees)) {
           a_stripe += (kees -> (x(kees) + y(kees)))
          } else {
            a_stripe += (kees -> x(kees))
          }
        }
        a_stripe
      })
      //(string, Map[String,Float])
      .map( x => {   //does the work of the combiner
        var sumOfKey = 0.0f
        var a_stripe:Map[String, Float] = Map()
        for(kees:String <- x._2.keys){
          sumOfKey = sumOfKey + x._2(kees)
        }
        for(kees <- x._2.keys){
            a_stripe += (kees -> (x._2(kees)/sumOfKey))
        }
      (x._1, a_stripe)
      })
    counts.saveAsTextFile(args.output())
  }
}
