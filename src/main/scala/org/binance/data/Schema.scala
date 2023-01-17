package org.binance.data

import org.apache.spark.sql.types.{ArrayType, StringType, StructType}
import spray.json.DefaultJsonProtocol._
import spray.json._

object Schema {

  val arrayArraySchema = new StructType().add("lastUpdateId", StringType)
    .add("bids", ArrayType(ArrayType(StringType)))
    .add("asks", ArrayType(ArrayType(StringType)))

}
