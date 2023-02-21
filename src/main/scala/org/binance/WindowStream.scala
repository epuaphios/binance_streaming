package org.binance

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.binance.data.Schema.arrayArraySchema

object WindowStream {
  def main(args: Array[String]): Unit = {

    val coinSizeSave = 100000;

    //    val vwapCombiner = new VWAPCombiner()
    val spark = SparkSession
      .builder
      .appName("BinanceStreaming")
      .config("spark.sql.caseSensitive", "True")
      .config("spark.mongodb.write.connection.uri", "mongodb://root:rootpassword@localhost:27017/")
      .config("spark.mongodb.write.database", "binance")
      .config("spark.mongodb.write.collection", "binance")
      .config("spark.jars.packages",
        "org.mongodb.spark:mongo-spark-connector:10.1.1")
      .config("spark.streaming.concurrentJobs", "2")

      //.config("spark.streaming.kafka.maxRatePerPartition",10)

      //.config("spark.sql.streaming.checkpointLocation","/tmp/blockchain-streaming/sql-streaming-checkpoint")
      .master("local[2]")
      .getOrCreate()

    //    val ssc = new StreamingContext(spark.sparkContext,Seconds(15))


    spark.sparkContext.setLogLevel("ERROR")

    val tradeStream = spark
      .readStream
      .format("kafka")
      .option("subscribe", "binance")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("startingOffsets", "earliest")
      .load
      .selectExpr("cast(value as string) as value", "timestamp")
      .select(from_json(col("value"), arrayArraySchema).alias("tmp"), col("timestamp")) //casting binary values into string


    import spark.implicits._

    val tradeStreamW = tradeStream.withColumn("vars", explode(arrays_zip($"tmp.bids", $"tmp.asks"))).select(
      $"timestamp", $"tmp.lastUpdateId", $"vars.bids".getItem(0).cast("float").alias("bids_p")
      , $"vars.bids".getItem(1).cast("float").alias("bids_q"), $"vars.asks".getItem(0).cast("float").alias("asks_p")
      , $"vars.asks".getItem(1).cast("float").alias("asks_q"))

    //
    val windowedCountsB = tradeStreamW.withWatermark("timestamp", "2 minutes").groupBy(window($"timestamp", "2 minutes", "1 minutes")
    ).sum("bids_q", "asks_q")


    //  tradeStreamW.where(col("asks_q") > coinSizeSave || col("bids_q") > coinSizeSave)
    //    .withColumn("status", when(col("asks_q") > coinSizeSave, "sell").when(col("bids_q") > coinSizeSave, "buy")).select(col("timestamp"), col("lastUpdateId"), col("status"), col("bids_p"), col("asks_p"), col("bids_q"), col("asks_q")).writeStream.format("mongodb").option("database", "binance").option("collection", "bigvolume").option("checkpointLocation", "/home/ogn/denemeler/big_data/binance_streaming/checkpoint").start().awaitTermination()

    windowedCountsB.writeStream.format("mongodb").option("database",
      "binance").option("collection", "window").option("checkpointLocation", "/home/ogn/denemeler/big_data/binance_streaming/checkpoint2")
      .start().awaitTermination()


  }
}