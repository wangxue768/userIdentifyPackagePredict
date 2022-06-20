package predict

import mam.{Dic, Utils}
import mam.Utils.{getJedis, printDf, sysParamSetting, udfLpad}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, collect_list, concat_ws, lit, row_number, sort_array, udf}
import org.apache.spark.sql.types.{DoubleType, FloatType, StringType}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket
import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

object SparkStreamingKafka {
  /**
   * Spark Streaming处理Kafka中的数据
   * 读取Redis中对应用户历史，进行推荐
   */
  val redis = getJedis
  val address = "10.102.32.131"  // 发送到服务器进行预测
  val port = 10004

  def main(args: Array[String]) {
    sysParamSetting

    val sparkConf = new SparkConf()
      .setAppName("SparkKafka")
      .setMaster("local[2]") // Spark Streaming要多于2个


    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("ERROR")
    print("正在接收数据........")
    val ssc = new StreamingContext(sc, Seconds(100)) // 设置批处理时间

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "10.102.0.195:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "test-consumer-group",
      "auto.offset.reset" -> "latest", //当前添加位置开始消费
      "enable.auto.commit" -> (true: java.lang.Boolean)
    )
    val topics = Array("playLog")
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    /**
     * 对接收到大Json数据 RDD[String] 转成DataSet 然后通过read.json转成DataFrame
     */

    stream.map(record => record.value()).foreachRDD(rdd => {
      val spark = SparkSession.builder()
        .config(rdd.sparkContext.getConf)
        .getOrCreate()

      import spark.implicits._

      if (rdd.isEmpty()) {
        println("Waiting data................")
      }

      while (!rdd.isEmpty()) { // 接收到数据再处理
        val df = spark.read.json(spark.createDataset(rdd))
        printDf("接收到数据", df)
        /**
         * 获取用户的会话列表
         * 形式 ： id, video_session_list, video_play_time_list
         */

        val win = Window.partitionBy(Dic.colUserId).orderBy(Dic.colPlayEndTime)

        val rowCount = df.count().toString.length

        val df_user_list = df
          .withColumn(Dic.colIndex, row_number().over(win))
          .withColumn("0", lit("0")) //要pad的字符
          .withColumn(Dic.colRank, udfLpad(col(Dic.colIndex), lit(rowCount), col("0"))) //拼接列
          .drop("0")
          .withColumn("tmp_column_vid", concat_ws(":", col(Dic.colRank), col(Dic.colVideoId)))
          .withColumn("tmp_column_vtime", concat_ws(":", col(Dic.colRank), col(Dic.colBroadcastTime)))

        printDf("df_user_list", df_user_list)


        val df_vid_list = df_user_list
          .groupBy(col(Dic.colUserId))
          .agg(
            collect_list(col("tmp_column_vid")).as("v_id")) //不能用collect_set 会去重
          .withColumn("tmp_column_vid_list", sort_array(col("v_id")))
          .withColumn(Dic.colVideoList, udfGetItemList(col("tmp_column_vid_list")))

        val df_vtime_list = df_user_list
          .groupBy(col(Dic.colUserId))
          .agg(
            collect_list(col("tmp_column_vtime")).as("v_time")) //不能用collect_set 会去重
          .withColumn("tmp_column_vtime_list", sort_array(col("v_time")))
          .withColumn(Dic.colPlayTimeList, udfGetItemList(col("tmp_column_vtime_list")))

        /**
         * 当前播放消息转换成Map
         */
        val map_vid = df_vid_list
          .select(Dic.colUserId, Dic.colVideoList)
          .rdd //Dataframe转化为RDD
          //RDD中每一行转化成u_id, v_session映射的key-value
          .map(row => row.getAs(Dic.colUserId).toString -> row.getAs(Dic.colVideoList).toString)
          .collectAsMap() //将key-value对类型的RDD转化成Map
          .asInstanceOf[mutable.Map[String, String]]

        val map_vtime = df_vtime_list
          .select(Dic.colUserId, Dic.colPlayTimeList)
          .rdd //Dataframe转化为RDD
          //RDD中每一行转化成u_id, v_session映射的key-value
          .map(row => row.getAs(Dic.colUserId).toString -> row.getAs(Dic.colPlayTimeList).toString)
          .collectAsMap() //将key-value对类型的RDD转化成Map
          .asInstanceOf[mutable.Map[String, String]]

        /**
         * 从Redis中批量读取当前批次用户历史数据,添加当前数据，并预测
         */
        val keys = df_user_list.select(Dic.colUserId).collect().map(_ (0)).map(ToString) // keys 是用户id
        val result = ArrayBuffer[Map[String, String]]()
        for (key <- keys) {
          val redis_value = redis.get(key).split("\\|")
          val v_session = redis_value(0) + map_vid.get(key)
          val v_time = redis_value(1) + map_vtime.get(key)
          val str = v_session + "|" + v_time
          val value1 = str.replace("]Some(WrappedArray(", ",[")
          val value = value1.replace("))", "]]")
          /**
           * 数据发送到模型服务器
           */
          val preRes = connModel(value) // 针对value进行预测
          result.append(Map(key -> preRes)) // id, 预测结果

          result.foreach(x => println(x))  // 当前批次预测结果
        }
        println("------------当前批次预测结束------------")
      }
    })
    redis.close() // 关闭Redis
    ssc.start
    ssc.awaitTermination


  }

  /**
   * 数据发送到模型服务器并返回预测结果
   */
  def connModel(data: String) = {
    val socket = new Socket(address, port)
    val outputStream = socket.getOutputStream
    val inputStream = socket.getInputStream
    // 向模型服务器发送数据
    outputStream.write(data.getBytes("utf-8"))
    outputStream.flush()
    // 从模型服务器接收预测结果
    val predict = new BufferedReader(new InputStreamReader(inputStream))
    val packages = predict.readLine()

//    println("------------Accept------------")
//    println(packages)
    packages // 预测结果

  }

  // 类型转换为String
  def ToString(s: Any): String = {
    s.toString
  }

  def udfGetItemList = udf(getItemList _)

  def getItemList(array: mutable.WrappedArray[String]) = {


    val result = new ListBuffer[String]()

    for (ele <- array)
      result.append(ele.split(":")(1))

    result.toList
  }


}
