package com.atguigu.App


import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.bean.StartupLog
import com.atguigu.GmallConstants
import com.atguigu.handler.DauHandler
import com.atguigu.Util.MykafkaUtil2
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.phoenix.spark._

object DauApp {

  def main(args: Array[String]): Unit = {

    //1.创建SparkConf
    val sparkConf: SparkConf = new SparkConf().setAppName("DauApp").setMaster("local[*]")

    //2.创建StreamingContext
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    //3.读取Kafka Start主题的数据创建流
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MykafkaUtil2.getKafkaStream(GmallConstants.GMALL_TOPIC_START, ssc)
    //val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.GMALL_TOPIC_START, ssc)

    //println(kafkaDStream)
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH")
    val startLogDStream: DStream[StartupLog] = kafkaDStream.map(record => {

      //a.取出Value
      val value: String = record.value()

      //b.转换为样例类对象
      val StartupLog: StartupLog = JSON.parseObject(value, classOf[StartupLog])

      //c.取出时间戳字段解析给logDate和logHour赋值
      val ts: Long = StartupLog.ts
      val dateHour: String = sdf.format(new Date(ts))
      val dateHourArr: Array[String] = dateHour.split(" ")
      StartupLog.logDate = dateHourArr(0)
      StartupLog.logHour = dateHourArr(1)

      //d.返回值
      StartupLog
    })

    startLogDStream.count().print()
    //5根据redis中保存的数据进行跨批次去重
    val filterByRedisDStream: DStream[StartupLog] = DauHandler.filterByRedis(startLogDStream, ssc.sparkContext)

    //6对第一次去重的数据做同批次去重

    val filterByGroupDStream: DStream[StartupLog] = DauHandler.filterByGroup(filterByRedisDStream)

    filterByGroupDStream.cache()
    //7将两次去重后的数据写入Redis（密度）
    DauHandler.saveMidToRedis(startLogDStream)
    //8将数据保存到hbase
    filterByGroupDStream.foreachRDD(rdd => {
      rdd.saveToPhoenix("GMALL2020_DAU",
        classOf[StartupLog].getDeclaredFields.map(_.getName.toUpperCase()),
        HBaseConfiguration.create(),
        Some("hadoop202,hadoop203,hadoop204:2181"))
    })

    //9启动任务
    //9.启动任务
    ssc.start()
    ssc.awaitTermination()

  }
}
