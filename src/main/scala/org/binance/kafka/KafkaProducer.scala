package com.binance.kafka

import akka.Done
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.ws.{Message, TextMessage, WebSocketRequest}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

import java.util.Properties
import scala.concurrent.Promise

/**
 * Modified version of
 *   https://github.com/imranshaikmuma/Websocket-Akka-Kafka-Spark
 */
object KafkaProducer extends App {

   implicit val system = ActorSystem()
    implicit val materializer = ActorMaterializer()
    import system.dispatcher


    val kafkaProducerProps: Properties = {
      val props = new Properties()
      props.put("bootstrap.servers", "localhost:9092")
      props.put("key.serializer", classOf[StringSerializer].getName)
      props.put("value.serializer", classOf[StringSerializer].getName)
      props
    }

    val producer = new KafkaProducer[String, String](kafkaProducerProps)


    val flow: Flow[Message, Message, Promise[Option[Message]]] =
      Flow.fromSinkAndSourceMat(
        Sink.foreach[Message](
          record => {
            //println(record.asInstanceOf[TextMessage].getStrictText)
            producer.send(new ProducerRecord[String, String]("binance", record.asInstanceOf[TextMessage].getStrictText))
          }
        ),
        Source.maybe[Message])(Keep.right)

//    //TODO check how this can achieved in other ways
//    val (xvgbtcResponse, _) =   Http().singleWebSocketRequest(
//      WebSocketRequest("wss://stream.binance.com:9443/ws/aptbusd@ticker"),
//      flow)

    val (cfxBusdResponse, _) =   Http().singleWebSocketRequest(
      WebSocketRequest("wss://stream.binance.com:9443/ws/cfxbusd@depth10@100ms"),
      flow)

    val (cfxUsdtResponse, _) = Http().singleWebSocketRequest(
      WebSocketRequest("wss://stream.binance.com:9443/ws/cfxusdt@depth10@100ms"),
      flow)


    val tradesResponse = List(cfxBusdResponse,cfxUsdtResponse) //Rude way for now!

    val connected = tradesResponse.map {
      response =>
        response.map {
          upgrade =>
            if (upgrade.response.status == StatusCodes.SwitchingProtocols) {
              Done
            } else {
              throw new RuntimeException(s"Connection failed: ${upgrade.response.status}")
            }
        }
    }

    connected.foreach(_.onComplete(println))


}
