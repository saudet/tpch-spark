name := "Spark TPC-H Queries"

version := "1.0"

scalaVersion := "2.12.10"

resolvers += "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"

libraryDependencies += "org.apache.spark" %% "spark-core" % "3.1.2"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.1.2"
libraryDependencies += "org.bytedeco" % "frovedis-platform-ve" % "1.0.0-1.5.7-SNAPSHOT"
