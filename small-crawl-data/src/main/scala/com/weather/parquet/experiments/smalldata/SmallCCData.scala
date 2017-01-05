package com.weather.parquet.experiments.smalldata

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark._

object SmallCCData {

  val localMaster = "local[2]"
  val clusterMaster = ""

  val masterUsedHere = localMaster

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf ().setMaster (masterUsedHere).setAppName("cool")
    val sc = new SparkContext (conf)
    val spark = SparkSession.builder
    .config (conf = conf)
    .appName ("Exploring A Sample of Common Crawl Data In Parquet Format")
    .getOrCreate ()

    val input = "resources/CC-Sample.json"
    val output = "output/raw-parquet/CC-Sample.parquet"

    val df: DataFrame = spark.read.json(input)
    df.write.option("compression", "gzip").parquet (output)
  }

}
