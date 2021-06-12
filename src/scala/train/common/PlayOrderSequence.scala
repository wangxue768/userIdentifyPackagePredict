package train.common

import mam.GetSaveData.{getProcessedOrder, getProcessedPlay, hdfsPath, saveProcessedData}
import mam.{Dic, SparkSessionInit}
import mam.SparkSessionInit.spark
import mam.Utils.{printDf, sysParamSetting}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, lag, lead, lit, sum, udf, unix_timestamp, when}


/**
 * @ClassName PlayOrderSequence
 * @author wx
 * @Description TODO 播放历史和订单历史按照时间顺序排列成序列，并生成时间差
 * @createTime 2021年06月09日 10:31:00
 */
object PlayOrderSequence {

  val play_order_seq_path = hdfsPath + "data/common/playOrderSeq"

  def main(args: Array[String]): Unit = {
    // TODO 1 Spark初始化
    sysParamSetting
    SparkSessionInit.init()

    //TODO 2 数据读取
    val df_orders = getProcessedOrder(spark)
    printDf("输入 df_orders", df_orders)

    val df_plays = getProcessedPlay(spark)
    printDf("输入 df_plays", df_plays)


    // TODO 3 选取play和order,按照顺序排序 1是play 2是order
    val df_plays_part = df_plays.select(Dic.colUserId, Dic.colVideoId, Dic.colPlayStartTime)
      .withColumnRenamed(Dic.colPlayStartTime, Dic.colTime)
      .withColumnRenamed(Dic.colVideoId, Dic.colItemId)
      .withColumn(Dic.colIsOrder, lit(0))
      .withColumn(Dic.colOrderStatus, lit(-1)) // -1表示播放历史
      .dropDuplicates()

    printDf("df_plays_part", df_plays_part)


    val df_order_part = df_orders.select(Dic.colUserId, Dic.colResourceId, Dic.colCreationTime, Dic.colOrderStatus)
      .withColumnRenamed(Dic.colResourceId, Dic.colItemId)
      .withColumnRenamed(Dic.colCreationTime, Dic.colTime)
      .withColumn(Dic.colIsOrder, lit(1))
      .withColumn(Dic.colOrderStatus, when(col(Dic.colOrderStatus) > 2, 1).otherwise(0))  // 订单状态为0和1的并没有付费
      .dropDuplicates()

    printDf("df_order_part", df_order_part)


    val df_play_order = df_plays_part.union(df_order_part)

    printDf("df_play_order", df_play_order)


    // TODO 4 获取order和play时间差
    //获得同一用户下一个action的时间
    val win1 = Window.partitionBy(Dic.colUserId).orderBy(Dic.colTime)

    val df_action_gap = df_play_order
      //同一个用户下一个行为的开始时间
      .withColumn(Dic.colStartTimeLeadItem, lead(Dic.colTime, 1).over(win1)) //下一个time
      .withColumn(Dic.colTimeGap,
        unix_timestamp(col(Dic.colStartTimeLeadItem)) - unix_timestamp(col(Dic.colTime)))
      .na.fill(Map((Dic.colTimeGap, 0))) //填充移动后产生的空值

    saveProcessedData(df_action_gap, play_order_seq_path)

    printDf("df_action_gap", df_action_gap)


  }

}
