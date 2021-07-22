package train.common

import mam.GetSaveData.{getProcessedMedias, getRawPlays, orderProcessedPath, saveProcessedData}
import mam.SparkSessionInit.spark
import mam.Utils.{getData, printDf, sysParamSetting, udfLongToDateTime}
import mam.{Dic, SparkSessionInit}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import train.userpay.userProfile.UserProfileGenerateOrderPart.playProcessedPath

/**
 * @author wx
 * @param
 * @return
 * @describe 对 play划分 session 并对session内视频不合并进行合并
 */

object PlaysUserInOrderProcess {

  val timeMaxLimit = 48000 //12h
  val timeMinLimit = 30
  val SessionGap = 1800 //session间隔时间  30min * 60min/s

  def main(args: Array[String]): Unit = {

    // 1 SparkSession init
    sysParamSetting()
    SparkSessionInit.init()

    // 2 數據讀取
    val df_play_raw_all = getRawPlays(spark)
    printDf("输入 df_play_raw_all", df_play_raw_all)

    val df_order = getData(spark, orderProcessedPath)

    val df_play_raw = df_play_raw_all.join(df_order, Seq(Dic.colUserId), "inner")
    printDf("输入 df_play_raw", df_play_raw)

    val df_medias_processed = getProcessedMedias(spark)
    printDf("输入 df_medias_processed", df_medias_processed)

    // 3 數據處理
    val df_plays_processed = playsProcessBySpiltSession(df_play_raw, df_medias_processed)

    // 4 Save Processed Data
    saveProcessedData(df_plays_processed, playProcessedPath) // 修改路径

    printDf("输出 playProcessed", df_plays_processed)
    println("播放数据处理完成！")
  }

  def playsProcessBySpiltSession(df_play_raw: DataFrame, df_medias_processed: DataFrame): DataFrame = {
    /**
     * 转换数据类型
     */
    val df_play = df_play_raw
      .select(
        when(col(Dic.colUserId) === "NULL", null).otherwise(col(Dic.colUserId)).as(Dic.colUserId),
        when(col(Dic.colPlayEndTime) === "NULL", null).otherwise(col(Dic.colPlayEndTime)).as(Dic.colPlayEndTime),
        when(col(Dic.colVideoId) === "NULL", null).otherwise(col(Dic.colVideoId)).as(Dic.colVideoId),
        when(col(Dic.colBroadcastTime) === "NULL", null).otherwise(col(Dic.colBroadcastTime)).as(Dic.colBroadcastTime)
      ).na.drop("any")
      .filter(col(Dic.colBroadcastTime) > timeMinLimit and col(Dic.colBroadcastTime) < timeMaxLimit)

    /**
     * 删除不在medias中的播放数据
     */

    val df_video_id = df_medias_processed.select(Dic.colVideoId, Dic.colPackageId, Dic.colVideoOneLevelClassification, Dic.colVideoTime).distinct()

    val df_play_in_medias = df_play.join(df_video_id, Seq(Dic.colVideoId), "inner")

    printDf("play数据中video存在medias中的数据", df_play_in_medias)


    /**
     * 计算开始时间 start_time
     */

    //end_time转换成long类型的时间戳    long类型 10位 单位 秒   colBroadcastTime是Int类型的 需要转化
    val df_play_start_time = df_play_in_medias.withColumn(Dic.colConvertTime, unix_timestamp(col(Dic.colPlayEndTime)))
      //计算开始时间并转化成时间格式
      .withColumn(Dic.colPlayStartTime, udfLongToDateTime(col(Dic.colConvertTime) - col(Dic.colBroadcastTime).cast("Long")))
      .drop(Dic.colConvertTime)

    /**
     * 划分session
     */

    //获得同一用户下一条play数据的start_time
    val win1 = Window.partitionBy(Dic.colUserId).orderBy(Dic.colPlayStartTime)

    val df_play_gap = df_play_start_time
      //同一个用户视频的开始时间
      .withColumn(Dic.colStartTimeLeadItem, lead(Dic.colPlayStartTime, 1).over(win1)) //下一个start_time
      .withColumn(Dic.colTimeGapLeadItem,
        unix_timestamp(col(Dic.colStartTimeLeadItem)) - unix_timestamp(col(Dic.colPlayEndTime))
      )
      .withColumn(Dic.colTimeGap30minSign,
        when(col(Dic.colTimeGapLeadItem) < SessionGap, lit(0)) //相同视频播放时间差30min之内
          .otherwise(1)) //0和1不能反
      .withColumn(Dic.colTimeGap30minSignLag, lag(Dic.colTimeGap30minSign, 1).over(win1))
      // 填充null 并选取 StartTimeLeadSameVideo 在 end_time之后的
      .na.fill(Map((Dic.colTimeGapLeadItem, 0), (Dic.colTimeGap30minSignLag, 0))) //填充移动后产生的空值
      //划分session
      .withColumn(Dic.colSessionId, sum(Dic.colTimeGap30minSignLag).over(win1))
      .na.fill(Map((Dic.colSessionId, 0)))
      .filter(col(Dic.colTimeGapLeadItem) >= 0) //筛选正确时间间隔的数据

    printDf("df_play_gap", df_play_gap)


    val df_play_processed = df_play_gap
      .select(
        Dic.colUserId, Dic.colVideoId, Dic.colPlayStartTime, Dic.colPlayEndTime, Dic.colBroadcastTime,
        Dic.colVideoOneLevelClassification, Dic.colVideoTime, Dic.colPackageId, Dic.colSessionId
      ).dropDuplicates(Dic.colVideoId)

    df_play_processed


  }

}