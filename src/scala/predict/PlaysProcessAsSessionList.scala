package predict

import mam.{Dic, SparkSessionInit}
import mam.GetSaveData.{getProcessedMedias, getRawPlays, getRawPlays2, saveProcessedData}
import mam.SparkSessionInit.spark
import mam.Utils.{printDf, sysParamSetting, udfLongToDateTime, udfLpad}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.StringType
import train.common.UserLabel.udfGetHistorySeq
import train.userpay.userProfile.UserProfileGenerateOrderPart.playProcessedPath

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.reflect.io.File.separator

/**
 * @author wx
 * @param
 * @return
 * @describe 对 play划分 session 成 list形式
 */

object PlaysProcessAsSessionList {

  val timeMaxLimit = 48000 //12h
  val timeMinLimit = 30
  val SessionGap = 1800 //session间隔时间  30min * 60min/s

  def main(args: Array[String]): Unit = {

    // 1 SparkSession init
    sysParamSetting()
    SparkSessionInit.init()

    // 2 數據讀取
    val df_play_raw = getRawPlays2(spark)
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
      .dropDuplicates()

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
    val df_play_start_time = df_play_in_medias
      .withColumn(Dic.colConvertTime, unix_timestamp(col(Dic.colPlayEndTime)))
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
        Dic.colUserId, Dic.colVideoId, Dic.colBroadcastTime, Dic.colPackageId, Dic.colSessionId, Dic.colPlayEndTime
      ).dropDuplicates(Dic.colVideoId)

    /**
     * 生成原始处理后的播放数据
     */
    df_play_processed.show(5)

    /**
     * 生成用户和其对应会话列表
     * user_id : [[(video_id, broadcast_time, package_id), (video_id, broadcast_time, package_id) ....],[(video_id, broadcast_time, package_id)],....,[(video_id, broadcast_time, package_id)]]
     */

    import org.apache.spark.sql.functions._
    val df_merge = df_play_processed
      .select(
        col(Dic.colUserId),
        mergeColsUDF(struct(Dic.colVideoId, Dic.colBroadcastTime, Dic.colPackageId)).as("Session_Value"),
        col(Dic.colPlayEndTime),
        col(Dic.colSessionId)
      )

    /**
     * 获取用户的会话列表
     */

    val win = Window.partitionBy(Dic.colUserId).orderBy(Dic.colSessionId, Dic.colPlayEndTime)

    val rowCount2 = df_merge.count().toString.length

    val df_sess_list = df_merge
      .withColumn(Dic.colIndex, row_number().over(win))
      .withColumn("0", lit("0")) //要pad的字符
      .withColumn(Dic.colRank, udfLpad(col(Dic.colIndex), lit(rowCount2), col("0"))) //拼接列
      .drop("0")
      .withColumn("tmp_column", concat_ws(":", col(Dic.colRank), col("Session_Value")))

    printDf("df_sess_list", df_sess_list)


    val df_all_sess = df_sess_list
      .groupBy(col(Dic.colUserId))
      .agg(
        collect_list(col("tmp_column")).as("tmp_column")) //不能用collect_set 会去重
      .withColumn("tmp_column_1", sort_array(col("tmp_column")))
      .withColumn(Dic.colSessionList, udfGetHistorySeq(col("tmp_column_1")))
      .select(
        Dic.colUserId,
        Dic.colSessionList
      )

    // user_id : [[(video_id, broadcast_time, package_id)], [(video_id, broadcast_time, package_id) ....]]

    printDf("df_all_sess",df_all_sess)

    df_all_sess
  }


  val mergeColsUDF = udf(mergeCols _)
// 合并多个列
  def mergeCols(row: Row): String = {
    row.toSeq.foldLeft("")(_ + separator + _).substring(1)
  }


  def udfGetHistorySeq = udf(getHistorySeq _)

  def getHistorySeq(array: mutable.WrappedArray[String]) = {


    val result = new ListBuffer[String]()

    for (ele <- array)
      result.append(ele.split(":")(1))

    result.toList.toString()
  }
}