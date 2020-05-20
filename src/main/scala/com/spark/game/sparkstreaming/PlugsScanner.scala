package com.spark.game.sparkstreaming

import kafka.serializer.StringDecoder
import org.apache.commons.lang3.time.FastDateFormat
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object PlugsScanner {
  def main(args: Array[String]): Unit = {
    val Array(zkQuorum, group, topics, numThreads) = Array("node1:2181,node2:2181,node3:2181", "g0", "gamelogs", "1")
    val dateFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")
    val conf = new SparkConf().setAppName("PluginsScanner").setMaster("local[4]")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Milliseconds(10000))
    sc.setCheckpointDir("D://testsparkdata//checkout2")

    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    val kafkaParams = Map[String, String](
      "zookeeper.connect" -> zkQuorum,
      "group.id" -> group,
      "auto.offset.reset" -> "smallest"
    )
    val dstream = KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicMap, StorageLevel.MEMORY_AND_DISK_SER)
    val lines = dstream.map(_._2)
    val splitedLines = lines.map(_.split("\t"))
    val filteredLines = splitedLines.filter(f => {
      val et = f(3)
      val item = f(8)
      et == "11" && item == "强效太阳水"
    })
    val groupedWindow: DStream[(String, Iterable[Long])] = filteredLines.map(f => (f(7), dateFormat.parse(f(12)).getTime)).groupByKeyAndWindow(Milliseconds(30000), Milliseconds(20000))
    val filtered: DStream[(String, Iterable[Long])] = groupedWindow.filter(_._2.size >= 5)
    val itemAvgTime = filtered.mapValues(it => {
      val list = it.toList.sorted
      val size = list.size
      val first = list(0)
      val last = list(size - 1)
      val cha: Double = last - first
      cha / size
    })
    val badUser: DStream[(String, Double)] = itemAvgTime.filter(_._2 < 10000)
    badUser.foreachRDD(rdd => {
      rdd.foreachPartition(it => {
        val connection = JedisConnectionPool.getConnection()
        it.foreach(t => {
          val user = t._1
          val avgTime = t._2
          val currentTime = System.currentTimeMillis()
          connection.set(user + "_" + currentTime, avgTime.toString)
        })
        connection.close()
      })

    })
    filteredLines.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
