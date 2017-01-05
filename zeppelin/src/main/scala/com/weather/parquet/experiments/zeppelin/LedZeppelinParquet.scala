package com.weather.parquet.experiments.zeppelin

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.parsing.json.JSON


case class LedZeppelinData(
                            Title: String,
                            Released: String,
                            Label: String,
                            PeakChart: Map[String, Int],
                            Certification: Map[String, String]
                          )
object LedZeppelinData {
  def apply(s: String): LedZeppelinData = {
    val m = JSON.parseFull(s).get.asInstanceOf[Map[String, Any]]

    println(m)

    LedZeppelinData (
      m("Title").toString,
      m("Released").toString,
      m("Label").toString,
      m("PeakChart").asInstanceOf[Map[String, Double]].map(kv => (kv._1, kv._2.toInt)),
      m("Certification").asInstanceOf[Map[String, String]]
    )
  }
}

object LedZeppelinParquet {

  val localMaster = "local[2]"
  val clusterMaster = ""

  val masterUsedHere = localMaster
  
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf ().setMaster (masterUsedHere).setAppName("cool")
    val sc = new SparkContext (conf)
    val spark = SparkSession.builder
      .config (conf = conf)
      .appName ("Led Zeppelin Data to Parquet")
      .getOrCreate ()

    import spark.implicits._

    val flatInput = "resources/LedZeppelin-FlatSchema.tsv"
    val nestedInput = "resources/LedZeppelin-NestedSchema.json"

    // read and write flat schema file
    val flatOutput = "output/raw-parquet/LedZeppelin-FlatSchema.parquet"

    val flatDF: DataFrame = spark.read.option("delimiter", "\t").option ("header", "true").csv(flatInput)
    flatDF.write.parquet(flatOutput)

    
    // read flat schema file, transform the rows to case classes
    val transformedOutput = "output/raw-parquet/LedZeppelin-TransformedSchema.parquet"
    val transformedRDD: RDD[LedZeppelinData] = spark.read.textFile(nestedInput).map(r => LedZeppelinData(r)).rdd
//    val transformedDF = jsonDF.map(json => LedZeppelinData(json.asInstanceOf[Map[String, Any]]))
    println(transformedRDD)
    transformedRDD.toDF.write.option("compression", "gzip").parquet (transformedOutput)

    
    

    // read and write nested schema file

    val nestedOutput = "output/raw-parquet/LedZeppelin-NestedSchema.parquet"

    val nestedDF: DataFrame = spark.read.json(nestedInput)
    flatDF.write.option("compression", "gzip").parquet (nestedOutput)


  }

}
