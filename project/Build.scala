import sbt._
import Keys._

object ParquetExperiementsBuild extends Build {
  lazy val root = Project(id = "hello",
                          base = file(".")) aggregate(smallData) dependsOn(smallData)

  lazy val smallData = Project(id = "smallData", base = file("small-data"))
}
