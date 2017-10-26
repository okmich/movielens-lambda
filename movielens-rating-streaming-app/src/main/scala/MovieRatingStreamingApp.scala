

import kafka.serializer.StringDecoder

import org.apache.spark.SparkConf;
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext;
import org.apache.spark.streaming._

import org.apache.spark.streaming.kafka.KafkaUtils

import it.nerdammer.spark.hbase._

object MovieRatingStreamingApp extends java.io.Serializable {

	def main(args: Array[String]) : Unit = {

		if (args.length < 5){
			println("Usage: , <kafka_url> <kafka_topic> <duration> <zkHost> <zkClientPort>")
			System.exit(-1)
		} 

		val sparkConf = new SparkConf()
		//set the hbase properties to spark config
		sparkConf.set("hbase.zookeeper.quorum", args(3))
		sparkConf.set("hbase.zookeeper.property.clientPort", args(4))
		sparkConf.set("spark.hbase.host", args(3))
		//instantitate spark context
		val sparkCtx = new SparkContext(sparkConf)
		//instantiate streaming context
		val streamingCtx =  new StreamingContext(sparkCtx, Milliseconds(args(2).toInt))

		val topics = Set(args(1))

		//prepare kafka properties
		val kafkaParams = Map(
			"metadata.broker.list" -> args(0)) //broker url list

		val iDStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](streamingCtx, kafkaParams, topics)

		iDStream.foreachRDD((rdd: RDD[(String,String)]) => {
				val transformeRDD = rdd.map((arg : (String, String)) => transform(arg._2)).
						filter((i : (String, String, String, Float)) => !i._1.isEmpty)
				//save to hbase table - rating
				transformeRDD.toHBaseTable("rating")
				    .toColumns("uid", "mid", "rt")
				    .inColumnFamily("main")
				    .save()
				})

		//start the streaming context
		streamingCtx.start
		streamingCtx.awaitTermination
	}


	def transform(s:String) : (String, String, String, Float) = {
		val parts = s.split(",")
		val key = (-1 * System.currentTimeMillis).toString + "-" + parts(0)

		try {
			(key, parts(0), parts(1), parts(2).toFloat)
		} catch {
			case _: Throwable => ("", "", "", 0F)
		}

		
	}
}