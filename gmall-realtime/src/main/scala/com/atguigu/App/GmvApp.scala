package com.atguigu.App

import com.alibaba.fastjson.JSON
import com.atguigu.Util.MykafkaUtil2
import com.atguigu.GmallConstants
import com.atguigu.bean.{OrderInfo, StartupLog}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.phoenix.spark._
object GmvApp {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("gmall2019")
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(5))
    //println("-----------***********************************--------------------------")

    val startupStream: InputDStream[ConsumerRecord[String, String]] = MykafkaUtil2.getKafkaStream(GmallConstants.GMALL_TOPIC_ORDER_INFO, ssc)

    //3.将每一行数据转换为样例类:给日期及小时字段重新赋值,给联系人手机号脱敏
    val orderInfoDStream: DStream[OrderInfo] = startupStream.map(record => {

      //a.转换为样例类对象
      val orderInfo: OrderInfo = JSON.parseObject(record.value(), classOf[OrderInfo])

      //b.给日期及小时字段重新赋值
      val create_time: String = orderInfo.create_time //2020-08-18 04:24:04
      val timeArr: Array[String] = create_time.split(" ")
      orderInfo.create_date = timeArr(0)
      orderInfo.create_hour = timeArr(1).split(":")(0)

      //c.给联系人手机号脱敏
      val telTuple: (String, String) = orderInfo.consignee_tel.splitAt(4)
      orderInfo.consignee_tel = telTuple._1 + "*******"

      //d.返回数据
      orderInfo
    })

    orderInfoDStream.cache()
    orderInfoDStream.print()
    orderInfoDStream.print()
    orderInfoDStream.foreachRDD(rdd => {

      rdd.saveToPhoenix("GMALL2020_ORDER_INFO",
        classOf[OrderInfo].getDeclaredFields.map(_.getName.toUpperCase),
        HBaseConfiguration.create(),
        Some("hadoop202,hadoop203,hadoop204:2181"))
      println("s2ss")
    })
    ssc.start()
    ssc.awaitTermination()
  }
}
