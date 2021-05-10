package com.atguigu.hotitems_analysis

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

/**
  * 这个类为了测试使用：从文件读数据往kafka写，我们在HotItems类中直接从kafka读数据即可
  */
object KafkaProducerUtil {
  def main(args: Array[String]): Unit = {
    writeToKafka("hotitems")
  }
  def writeToKafka(topic: String): Unit ={
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("key.serializer",
      "org.apache.kafka.common.serialization.StringSerializer")
    properties.setProperty("value.serializer",
      "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](properties)

    // 从文件读取数据，逐行写入kafka
    val bufferedSource = io.Source.fromFile("D:\\Projects\\BigData\\UserBehaviorAnalysis\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv")

    for( line <- bufferedSource.getLines() ){
      val record = new ProducerRecord[String, String](topic, line)
      producer.send(record)
    }

    producer.close()
  }
}
