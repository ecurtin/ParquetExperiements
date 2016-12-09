package com.weather.parquet.experiments.smalldata

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark._

import scala.util.parsing.json.JSON


case class SeasameStreet(
                        name: String,
                        nickname: Option[String],
                        position: Option[String],
                        address: Map[String, Any]
                        )

object SeasameStreet {
  def apply(r: Row): SeasameStreet = {
    SeasameStreet(
      r.getString(0),
      getOptionally(r, 1),
      getOptionally(r, 2),
      JSON.parseFull(r.getString(3)).asInstanceOf[Map[String, Any]]
    )
  }

  def getOptionally(r: Row, i: Int): Option[String] = {
    r.getString(i) match {
      case null => None
      case "" => None
      case s => Some(s)
    }
  }
}

object SmallData {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf ().setMaster ("local[2]").setAppName ("my app")
    val sc = new SparkContext (conf)
    val spark = SparkSession.builder
    .config (conf = conf)
    .appName ("Exploring Small Data In Parquet Format")
    .getOrCreate ()

    import spark.implicits._


    val input = "resources/seasame-street.json"
    val output = "output/seasame-street.parquet"
//    val df: DataFrame = spark.read.option("delimiter", "\t").option ("header", "true").csv (input)

    val df: DataFrame = spark.read.json(input)

//    val parsed = df.map(r => SeasameStreet(r))

    val sample = df.head()

    df.write.option("compression", "gzip").parquet (output)
  }

}
