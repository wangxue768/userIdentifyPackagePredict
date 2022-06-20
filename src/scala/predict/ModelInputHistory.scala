package predict

import mam.GetSaveData.{getRawPlays, getRawPlays2}
import mam.{Dic, SparkSessionInit}
import mam.SparkSessionInit.spark
import mam.Utils.{printDf, sysParamSetting, udfLpad}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, collect_list, concat_ws, lit, row_number, sort_array, udf}
import predict.PlaysProcessAsSessionList.udfGetHistorySeq

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object ModelInputHistory {
  /**
   * 输入模型格式测试
   *
   * @param args
   */

  def main(args: Array[String]): Unit = {
    // 1 SparkSession init
    sysParamSetting()
    SparkSessionInit.init()

    // 2 數據讀取
    val df_play_raw = getRawPlays2(spark)
    printDf("输入 df_play_raw", df_play_raw)

    /**
     * 获取用户的会话列表
     * 形式 ： id, video_session_list, video_play_time_list
     */

    val win = Window.partitionBy(Dic.colUserId).orderBy(Dic.colPlayEndTime)

    val rowCount = df_play_raw.count().toString.length

    val df_user_list = df_play_raw
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


    val df_input = df_vid_list.join(df_vtime_list, Seq(Dic.colUserId), "inner")
      .select(
        Dic.colUserId,
        Dic.colVideoList,
        Dic.colPlayTimeList
      )

    printDf("df_input", df_input)

  }
  def udfGetItemList = udf(getItemList _)

  def getItemList(array: mutable.WrappedArray[String]) = {


    val result = new ListBuffer[String]()

    for (ele <- array)
      result.append(ele.split(":")(1))

    result.toList
  }
}



