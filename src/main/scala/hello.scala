package com.github.twoerth

import parquet.hadoop.{ParquetOutputFormat, ParquetInputFormat}
import parquet.hadoop.metadata.CompressionCodecName
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.hadoop.mapreduce.Job
import parquet.avro.{AvroParquetOutputFormat, AvroWriteSupport, AvroReadSupport}
import parquet.filter.{RecordFilter, UnboundRecordFilter}
import java.lang.Iterable
import parquet.column.ColumnReader
import parquet.filter.ColumnRecordFilter._
import parquet.filter.ColumnPredicates._
import com.google.common.io.Files
import java.io.File
import org.apache.avro.Schema.Parser
import com.zenfractal.AvroSerializable

object SparkParquetExample {

  def main(args: Array[String]) {
    val sc = new SparkContext("local", "ParquetExample")
	try {
	    val job = new Job( sc.hadoopConfiguration )

	    val tempDir = Files.createTempDir()
	    val outputDir = new File(tempDir, "output").getAbsolutePath
	    println(outputDir)

		val o = List.range(0,500000).map( i => ( null , new AvroSerializable[Event]( new Event( i, "test" + i, "longer text " + i ) ) ) )

	    // Configure the ParquetOutputFormat to use Avro as the serialization format
	    ParquetOutputFormat.setWriteSupportClass(job, classOf[AvroWriteSupport])
		ParquetOutputFormat.setCompression(job, CompressionCodecName.SNAPPY)

	    // You need to pass the schema to AvroParquet when you are writing objects but not when you
	    // are reading them. The schema is saved in Parquet file for future readers to use.

	    AvroParquetOutputFormat.setSchema(job, Event.SCHEMA$ )

	    // Create a PairRDD with all keys set to null and wrap each amino acid in serializable objects
	    val rdd = sc.makeRDD( o )
	    // Save the RDD to a Parquet file in our temporary output directory
	    rdd.saveAsNewAPIHadoopFile(outputDir, classOf[Void], classOf[Event],
	      classOf[ParquetOutputFormat[Event]], job.getConfiguration)

	    ParquetInputFormat.setReadSupportClass(job, classOf[AvroReadSupport[Event]])
	    val file = sc.newAPIHadoopFile(outputDir, classOf[ParquetInputFormat[Event]],
	      classOf[Void], classOf[Event], job.getConfiguration)
	    file.foreach(println)
	} finally {
		sc.stop()
	}
  }


}