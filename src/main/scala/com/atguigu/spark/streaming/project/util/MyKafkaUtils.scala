package com.atguigu.spark.streaming.project.util

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.{KafkaUtils}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
/**
  * Created by shkstart on 2021/5/23.
  */
object MyKafkaUtils {


  val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> "hadoop202:9092,hadoop203:9092,hadoop204:9092",
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "sparkgroup1",
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> (true: java.lang.Boolean)
  )

  def getKafkaUtils(ssc:StreamingContext,topics:String*)=
    KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    ).map(_.value)
}
