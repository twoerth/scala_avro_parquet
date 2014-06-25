name := "hello"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies ++= Seq(
//	"" %% "" % ""
	"org.apache.spark" %% "spark-core" % "1.0.0",
	"com.twitter" % "parquet-hadoop" % "1.5.0",
	"com.twitter" % "parquet-avro" % "1.5.0",
	"com.gensler" %% "scalavro" % "0.6.2"
)

seq( sbtavro.SbtAvro.avroSettings : _*)