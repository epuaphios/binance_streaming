# binance_streaming

sbt clean assembly

Binance websocket streaming to kafka example

scala -classpath target/scala-2.12/akka-jre-example-assembly-0.1.0-SNAPSHOT.jar com.binance.kafka.KafkaProducer

Kafka to MongoDB
./spark-submit --class org.binance.StructuredStreaming --master spark://localhost:8080 --deploy-mode cluster ./scala-2.12/akka-jre-example-assembly-0.1.0-SNAPSHOT.jar 


