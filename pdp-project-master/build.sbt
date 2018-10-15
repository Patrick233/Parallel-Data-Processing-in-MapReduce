name := "project"

version := "1.0"

scalaVersion := "2.11.11"

libraryDependencies ++={
  val sparkVer = "2.2.0"
  Seq{
    "org.apache.spark" %% "spark-core" % sparkVer;
    "org.apache.spark" %% "spark-mllib" % sparkVer;
  }
}
    