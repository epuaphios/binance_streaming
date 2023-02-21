package org.binance


import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.binance.data.Schema.arrayArraySchema

/**
  * //https://databricks.com/blog/2017/04/26/processing-data-in-apache-kafka-with-structured-streaming-in-apache-spark-2-2.html
  */
object StructuredStreaming {

//  def toConsole(df: DataFrame, intervalSeconds: Long) = {
//    df
//      .writeStream
//      //      .outputMode("complete")
//      .format("console")
//      .trigger(Trigger.ProcessingTime(intervalSeconds, TimeUnit.SECONDS))
//      .option("truncate",false)
//      .start()
//  }

//  def aggDfToConsole(df: DataFrame, intervalSeconds: Long, is_last: Boolean = false) = {
//
//    if (is_last) {
//      df
//        .writeStream
//        .outputMode("complete")
//        .format("console")
//        .trigger(Trigger.ProcessingTime(intervalSeconds, TimeUnit.SECONDS))
//        .option("truncate",false)
//        .start()
//        .awaitTermination()
//    } else {
//      df
//        .writeStream
//        .outputMode("complete")
//        .format("console")
//        .trigger(Trigger.ProcessingTime(intervalSeconds, TimeUnit.SECONDS))
//        .option("truncate",false)
//        .start()
//    }
//
//  }
  def main(args: Array[String]): Unit = {




    val coinSizeSave = 100000;

//    val vwapCombiner = new VWAPCombiner()
    val spark = SparkSession
      .builder
      .appName("BinanceStreaming")
      .config("spark.sql.caseSensitive" , "True")
      .config("spark.mongodb.write.connection.uri", "mongodb://root:rootpassword@localhost:27017/")
      .config("spark.mongodb.write.database", "binance")
      .config("spark.mongodb.write.collection", "binance")
      .config("spark.jars.packages",
      "org.mongodb.spark:mongo-spark-connector:10.1.1")
      .config("spark.streaming.concurrentJobs","2")

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
      .selectExpr("cast(value as string) as value","timestamp")
      .select(from_json(col("value"), arrayArraySchema).alias("tmp"),col("timestamp"))//casting binary values into string


  import spark.implicits._

  val tradeStreamW =tradeStream.withColumn("vars", explode(arrays_zip($"tmp.bids", $"tmp.asks"))).select(
    $"timestamp", $"tmp.lastUpdateId", $"vars.bids".getItem(0).cast("float").alias("bids_p")
    ,$"vars.bids".getItem(1).cast("float").alias("bids_q"), $"vars.asks".getItem(0).cast("float").alias("asks_p")
    ,$"vars.asks".getItem(1).cast("float").alias("asks_q"))



  val windowedCountsB = tradeStreamW.withWatermark("timestamp", "5 minutes").groupBy(window($"timestamp", "2 minutes", "1 minutes"),
        $"lastUpdateId"
    ).sum("bids_q","asks_q")

  tradeStreamW.withColumn("status",when(col("asks_p")>coinSizeSave,"sell").when(col("bids_p")>coinSizeSave,"buy")).select(col("timestamp"),col("lastUpdateId"),col("status"),col("bids_p"),col("asks_p"),col("bids_q"),col("asks_q")).writeStream.format("mongodb").option("database","binance").option("collection", "bigVolume").option("checkpointLocation", "/home/ogn/denemeler/big_data/binance_streaming/checkpoint").start()


  windowedCountsB.writeStream.format("mongodb").option("database",
     "binance").option("collection", "window").option("checkpointLocation", "/home/ogn/denemeler/big_data/binance_streaming/checkpoint")
     .start().awaitTermination()



//  val tradeStreamSell = tradeStream.select(from_json(col("value"), arrayArraySchema).alias("tmp"),col("timestamp")).select(col("tmp.lastUpdateId"), explode(col("tmp.asks")),col("timestamp")).select(col("lastUpdateId"), col("col").getItem(0).cast("float").alias("p"), col("col").getItem(1).cast("float").alias("q"),col("timestamp"))


//  tradeStream.withColumn("vars", explode(arrays_zip($"varA", $"varB"))).select(
//    $"userId", $"someString",
//    $"vars.varA", $"vars.varB").show


//  val windowedCountsB = tradeStreamBought.where(col("q")>=coinSizeSave).withWatermark("timestamp", "5 minutes").groupBy(window($"timestamp", "2 minutes", "1 minutes"),
//      $"p"
//  ).sum("q")//
//
//  val windowedCountsS = tradeStreamSell.where(col("q") >= coinSizeSave).withWatermark("timestamp", "5 minutes").groupBy(window($"timestamp", "2 minutes", "1 minutes"),
//    $"p"
//  ).sum("q") //


//  val query =  windowedCountsB.writeStream.format("mongodb").option("database",
//        "binance").option("collection", "bought").option("checkpointLocation", "/home/ogn/denemeler/big_data/binance_streaming/checkpoint")
//        .start()
//
//
//  val query1 = windowedCountsS.writeStream.format("mongodb").option("database",
//      "binance").option("collection", "sell").option("checkpointLocation", "/home/ogn/denemeler/big_data/binance_streaming/checkpoint")
//      .start().awaitTermination()
//


    //windowedCounts.show(false)
//
//  def myFunc(askDF: DataFrame, batchID: Long): Unit = {
//    askDF.persist()
//    askDF.write.format("mongodb").option("database", "binance").option("collection", "set").option("checkpointLocation", "/home/ogn/denemeler/big_data/binance_streaming/checkpoint")
//    askDF.unpersist()
//  }
//
//    windowedCounts.writeStream.foreachBatch(myFunc _).start().awaitTermination()

//
//          windowedCounts.writeStream.foreachBatch(
//            (outputDf: DataFrame, bid: Long) => {
//              outputDf.persist()
//              outputDf.write.format("mongodb").option("database", "people").option("collection", "contacts").option("checkpointLocation", "/home/ogn/denemeler/big_data/binance_streaming/checkpoint")
//              outputDf.unpersist()
//          }).start().awaitTermination()



//  def toConsole(df: DataFrame, intervalSeconds: Long) = {
//    df
//      .writeStream
//      //      .outputMode("complete")
//      .format("console")
//      .trigger(Trigger.ProcessingTime(intervalSeconds, TimeUnit.SECONDS))
//      .option("truncate", false)
//      .start()
//  }



//    val windowSpec = Window.partitionBy(tradeStream("lastUpdateId")).orderBy(tradeStream("q").desc)

//    val windowedCountsDF = tradeStream.withWatermark("eventTime", "2 minutes").groupBy("q", session_window("lastUpdateId", "1 minutes"))
//val windowedCounts = words
//  .withWatermark("timestamp", "10 minutes")
//  .groupBy(
//    window($"lastUpdateId", "10 minutes", "5 minutes"),
//    $"word")
//  .count()

//    tradeStream.select('id', dense_rank.over(window.orderBy('col') ).alias('group') ).show(truncate = False)

//     val tradeStream2= tradeStream.withColumn("max_q", first(tradeStream("q")).over(windowSpec).as("max_sq")).filter("max_sq = q").where(col("max_q")>=coinSizeSave)
//
//    val query =  tradeStream2.writeStream.format("mongodb").option("database",
//      "people").option("collection", "contacts").option("checkpointLocation", "/home/ogn/denemeler/big_data/binance_streaming/checkpoint")
//      .start()
//
//    query.awaitTermination()



    //    tradeStream.groupBy("lastUpdateId").agg(max(col("q"))).where(col("max(q)")>=1000).show(false)
//
//    tradeStream.


//
//
//    tradeStream.show(false)
//    tradeStream.printSchema()
//      .withColumn("p",  col("p").cast("double"))
//      .withColumn("q",  col("q").cast("double"))
//      .withColumn("pq", col("p") * col("q"))
//      .withColumn("T", from_unixtime(col("T").cast ("bigint")/1000).cast(TimestampType))
////      .withColumn("T", unix_timestampmp($"T", "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").cast(TimestampType))
//      .withWatermark("T", "5 seconds")
    //          .as[TradeStreams] //Enable to do any Dataset operations




//    val sql = "SELECT s as key, SUM(pq) as sum_pq, SUM(q) as sum_q FROM trade_stream GROUP BY s"
//
//    val vwapOverSqlStatement = spark
//      .sql(sql)
//      .withColumn("vwapOverSqlStatement",  col("sum_pq") /  col("sum_q"))
//      .withColumn("value",  col("vwapOverSqlStatement").cast("string"))
//
//    vwapOverSqlStatement.printSchema()
//
//    val vwap = tradeStream
//      .groupBy(
//        window(col("T"), "10 seconds", "5 seconds"),
//        col("s")
//      ).sum("pq", "q")

    //(run-main-1) org.apache.spark.sql.AnalysisException: Multiple streaming aggregations are not supported with streaming DataFrames/Datasets;;
    //.groupBy("s")
    //.sum("sum(pq)", "sum(q)")

//
//    tradeStream
//      .writeStream
//      .format("kafka")
//      .outputMode("complete")
//      .option("kafka.bootstrap.servers", "localhost:9092")
//      .option("topic","vwap")
//      .option("checkpointLocation", "/tmp/blockchain-streaming/sql-streaming-checkpoint/vwap/")
//      .start()
//
//    spark
//      .readStream
//      .format("kafka")
//      .option("subscribe","vwap")
//      .option("kafka.bootstrap.servers", "localhost:9092")
//      .load
//      .selectExpr("cast(value as string) as vwapFromSparkStreaming") //casting binary values into string
//      .writeStream
////      .outputMode("complete")
//      .format("console")
//      //.trigger(Trigger.Continuous(batchTimeInSeconds, TimeUnit.SECONDS))
//      .option("truncate",false)
//      .start()
//      .awaitTermination()
  }
}


