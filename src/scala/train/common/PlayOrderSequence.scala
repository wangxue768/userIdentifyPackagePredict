package train.common

import mam.GetSaveData.{getProcessedOrder, getProcessedPlay, hdfsPath, saveProcessedData}
import mam.{Dic, SparkSessionInit}
import mam.SparkSessionInit.spark
import mam.Utils.{getData, printDf, sysParamSetting}
import org.apache.spark.sql.functions.{col, lag, lead, lit, row_number, sum, udf, unix_timestamp, when}
import train.common.UserLabel.orderProcessedPath
import train.userpay.userProfile.UserProfileGenerateOrderPart.playProcessedPath

/**
 * @ClassName PlayOrderSequence
 * @author wx
 * @Description TODO 播放历史和订单历史按照时间顺序排列成序列，并生成时间差
 * @createTime 2021年06月09日 10:31:00
 */
object PlayOrderSequence {
  val playMinTime = "2020-08-03 00:00:00"
  val play_order_seq_path = hdfsPath + "data/wx/play_order_seq"

  def main(args: Array[String]): Unit = {
    // TODO 1 Spark初始化
    sysParamSetting
    SparkSessionInit.init()

    //TODO 2 数据读取
    val df_plays = getData(spark, playProcessedPath)
    printDf("输入 df_plays", df_plays)


    val df_orders = getData(spark, orderProcessedPath)
    printDf("输入 df_orders", df_orders)


    // TODO 3 选取play和order,按照顺序排序 1是play 2是order
    val df_plays_part = df_plays.select(Dic.colUserId, Dic.colVideoId, Dic.colPlayStartTime, Dic.colPlayEndTime, Dic.colSessionId)
      .withColumnRenamed(Dic.colPlayStartTime, Dic.colCreationTime)
      .withColumnRenamed(Dic.colPlayEndTime, Dic.colEndTime)
      .withColumn(Dic.colIsOrder, lit(0))
      .withColumn(Dic.colOrderStatus, lit(-1)) // -1表示播放历史
      .withColumnRenamed(Dic.colVideoId, Dic.colItemId)
      .dropDuplicates()
      .select(Dic.colUserId, Dic.colItemId, Dic.colCreationTime, Dic.colEndTime, Dic.colIsOrder, Dic.colOrderStatus, Dic.colSessionId)

    printDf("df_plays_part", df_plays_part)



    val df_order_part = df_orders.select(Dic.colUserId, Dic.colResourceId, Dic.colCreationTime, Dic.colOrderStatus)
      .filter(col(Dic.colCreationTime) >= playMinTime) // 订单和播放历史不是一个时间段，选取同一个时间段的
      .withColumn(Dic.colEndTime, col(Dic.colCreationTime))
      .withColumn(Dic.colOrderStatus, when(col(Dic.colOrderStatus) >= 2, 1).otherwise(0)) // 订单状态为0和1的并没有付费
      .withColumn(Dic.colIsOrder, lit(1))
      .withColumn(Dic.colSessionId, lit(-1))
      .withColumnRenamed(Dic.colResourceId, Dic.colItemId) // 没有编码
      .dropDuplicates()
      .select(Dic.colUserId, Dic.colItemId, Dic.colCreationTime, Dic.colEndTime, Dic.colIsOrder, Dic.colOrderStatus, Dic.colSessionId)


    printDf("df_order_part", df_order_part)


    val df_play_order = df_plays_part.union(df_order_part) //要保证列的位置是一样的
      .orderBy(col(Dic.colUserId), col(Dic.colCreationTime))

    printDf("df_play_order", df_play_order)


    saveProcessedData(df_play_order, play_order_seq_path)


  }

}
