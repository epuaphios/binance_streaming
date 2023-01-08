package org.binance.data

import org.apache.spark.sql.types.{StringType, StructType, TimestampType}
import spray.json.DefaultJsonProtocol._
import spray.json._

object Schema {



  val tradeStreamsSchema = new StructType()
    .add("e", StringType)
    .add("E", StringType)
    .add("s", StringType)
    .add("t", StringType)
    .add("p", StringType)
    .add("q", StringType)
    .add("b", StringType)
    .add("a", StringType)
    .add("T", TimestampType)
    .add("m", StringType)
    .add("M", StringType)


  case class TradeStreams(e: String,
                          E: String,
                          s: String,
                          t: String,
                          p: Double,
                          q: Double,
                          b: String,
                          a: String,
                          T: String,
                          m: String,
                          M: String)



  def parseTradeStreams(line: String):TradeStreams = {

    val res = line
      .replace("{", "")
      .replace("}", "")
      .replace("\"", "")
      .split(",")
      .map(field => {
        val res = field.split(":")
        (res(0).toString, res(1).toString)
      })
      .toMap

    TradeStreams(e = res.getOrElse("e", "0"),
      E = res.getOrElse("E", "0"),
      s = res.getOrElse("s", "0"),
      t = res.getOrElse("t", "0"),
      p = res.getOrElse("p", "0").toDouble,
      q = res.getOrElse("q", "0").toDouble,
      b = res.getOrElse("b","0"),
      a = res.getOrElse("a", "0"),
      T = res.getOrElse("T", "0"),
      m = res.getOrElse("m", "0"),
      M = res.getOrElse("M", "0"))
  }

  //TODO check why it fails to parse E, for which the value is Long not String
  object TradeStreamsProtocol extends DefaultJsonProtocol {
    implicit val tradeStreamsFormat = jsonFormat11(TradeStreams)
  }




}
