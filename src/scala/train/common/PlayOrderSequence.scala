package train.common

import mam.GetSaveData.{getProcessedOrder, getProcessedPlay}
import mam.{Dic, SparkSessionInit}
import mam.SparkSessionInit.spark
import mam.Utils.{printDf, sysParamSetting}
import org.apache.spark.sql.functions.{col, lit, udf}

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

/**
 * @ClassName PlayOrderSequence
 * @author wx
 * @Description TODO 播放历史和订单历史按照时间顺序排列成序列
 * @createTime 2021年06月09日 10:31:00
 */
object PlayOrderSequence {
  def main(args: Array[String]): Unit = {
    // TODO 1 Spark初始化
    sysParamSetting
    SparkSessionInit.init()

    //TODO 2 数据读取
    val df_orders = getProcessedOrder(spark)
    printDf("输入 df_orders", df_orders)

    val df_plays = getProcessedPlay(spark)
    printDf("输入 df_plays", df_plays)


    // TODO 3 选取play和order,按照顺序排序
    val df_plays_part = df_plays.select(Dic.colUserId, Dic.colVideoId, Dic.colPlayStartTime)
      .withColumnRenamed(Dic.colPlayStartTime, Dic.colTime)
      .withColumnRenamed(Dic.colVideoId, Dic.colItemId)
      .withColumn(Dic.colPlayOrder, lit(1))
      .dropDuplicates()

    printDf("df_plays_part", df_plays_part)


    val df_order_part = df_orders.select(Dic.colUserId, Dic.colResourceId, Dic.colCreationTime)
      .withColumnRenamed(Dic.colResourceId, Dic.colItemId)
      .withColumnRenamed(Dic.colCreationTime, Dic.colTime)
      .withColumn(Dic.colPlayOrder, lit(2))
      .dropDuplicates()

    printDf("df_order_part", df_order_part)


    val df_play_order = df_plays_part.union(df_order_part)
      .orderBy(col(Dic.colUserId), col(Dic.colTime))


    printDf("df_play_order", df_play_order)

  }

}
