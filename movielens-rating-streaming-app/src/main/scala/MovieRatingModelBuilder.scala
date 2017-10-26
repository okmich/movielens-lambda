
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.DataFrame;

import it.nerdammer.spark.hbase._

object MovieRatingModelBuilder {
	
	def main(args: Array[String]) = {
		if (args.length < 2){
			println("Usage: , <zkHost> <zkClientPort>")
			System.exit(-1)
		} 

		val sparkConf = new SparkConf()
		//set the hbase properties to spark config
		sparkConf.set("hbase.zookeeper.quorum", args(0))
		sparkConf.set("hbase.zookeeper.property.clientPort", args(1))
		sparkConf.set("spark.hbase.host", args(0))
		//instantitate spark context
		val sparkCtx = new SparkContext(sparkConf)
		//instantitate spark sql context
		val sqlContext = new SQLContext(sparkCtx)
		//import spark sql implicit
		import sqlContext.implicits._

		//read from hbase
		val hBaseRDD = sparkCtx.hbaseTable[(String, String, Float)]("rating")
			    .select("uid", "mid", "rt")
			    .inColumnFamily("main")

		// // cache the dataframe
		// val ratingDF : DataFrame = hBaseRDD.toDF.cache
	}

}