package train.common

import mam.GetSaveData.{getProcessedOrder, hdfsPath, saveProcessedData}
import mam.{Dic, SparkSessionInit}
import mam.SparkSessionInit.spark
import mam.Utils.{getData, printDf, sysParamSetting, udfLpad}
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, collect_list, concat_ws, lag, lead, length, lit, max, min, row_number, sort_array, udf, unix_timestamp, when}
import train.common.PlayOrderSequence.play_order_seq_path
import train.userpay.userProfile.UserProfileGenerateOrderPart.playProcessedPath

import java.text.SimpleDateFormat
import java.util.Calendar
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object UserLabel {

  val orderProcessedPath = hdfsPath + "data/wx/train/common/processed/orders_basic_process"
  val play_index_path = hdfsPath + "data/wx/train/common/processed/play_index"
  val orderIndexPath = hdfsPath + "data/wx/train/common/processed/orders_basic_process_index"


  val package_list = Array("100201", "100202", "101301", "101601", "101701", "101801", "101803", "101810", "101850")
  val order_path = hdfsPath + "data/wx/train/common/processed/orders_basic_process"

  val user_label_path = hdfsPath + "data/wx/user_label"

  def main(args: Array[String]): Unit = {

    println(this.getClass.getName)

    // TODO 1 Spark初始化
    sysParamSetting
    SparkSessionInit.init()

    //TODO 2 数据读取

    val df_play = getData(spark, playProcessedPath)
    printDf("df_play", df_play)


    val df_play_order = getData(spark, play_order_seq_path)
    printDf("df_play_order", df_play_order)


    val df_order = getData(spark, orderProcessedPath) // 没有编码
    printDf("df_order", df_order)


    //TODO 3 获取用户最后播放后 24h 购买订单
    // 购买的为正样本

    val df_pos = getUserOrderLabel(df_play, df_play_order, df_order)
    printDf("df_pos", df_pos)


    //负样本用户
    val df_neg_user = df_play
      .select(Dic.colUserId).dropDuplicates()
      .except(df_pos.select(Dic.colUserId).dropDuplicates()) //正样本
      .limit(10 * df_pos.count().toInt)
      .withColumn(Dic.colPackIndex, lit(0)) // pack index

    // 全部选择的用户
    val df_select_user = df_pos.union(df_neg_user)

    printDf("df_select_user", df_select_user)


    //TODO 4 获取用户session序列
    val df_user_session = getUserSessionList(df_select_user, df_play_order)
    printDf("df_user_session", df_user_session)


    val df_dataset = df_user_session.join(df_select_user, Seq(Dic.colUserId), "inner")

    printDf("df_dataset", df_dataset)

    saveProcessedData(df_dataset, user_label_path)


    /**
     * 根据 7 ： 2 ： 1 的比例设置 train validate test
     */
    // Train Data
    val df_train_pos = df_dataset
      .filter(col(Dic.colPackIndex) =!= 0) // 正样本
      .sample(false, 0.7) // 不放回 抽取样本的0.7

    val df_train_neg = df_dataset
      .filter(col(Dic.colPackIndex) === 0) // 负样本
      .sample(false, 0.7) // 不放回 抽取样本的0.7

    val df_train_data = df_train_pos.union(df_train_neg).sample(1)

    saveProcessedData(df_train_data, user_label_path + "_train")


    // Validate Data
    val df_validate_pos = df_dataset
      .filter(col(Dic.colPackIndex) =!= 0) // 正样本
      .except(df_train_pos)
      .sample(false, 0.2)

    val df_validate_neg = df_dataset
      .filter(col(Dic.colPackIndex) === 0)
      .except(df_train_neg)
      .sample(false, 0.2)

    val df_validate_data = df_validate_pos.union(df_validate_neg).sample(1)

    saveProcessedData(df_validate_data, user_label_path + "_valid")


    // Test Data
    val df_test_pos = df_dataset
      .filter(col(Dic.colPackIndex) =!= 0) // 正样本
      .except(df_train_pos)
      .except(df_validate_pos)
      .sample(false, 0.1) //

    val df_test_neg = df_dataset
      .filter(col(Dic.colPackIndex) === 0) // 正样本
      .except(df_train_neg)
      .except(df_validate_neg)
      .sample(false, 0.1) //


    val df_test_data = df_test_pos.union(df_test_neg).sample(1)

    saveProcessedData(df_test_data, user_label_path + "_test")

    println("User Label Done")

  }


  def getUserSessionList(df_select_user: DataFrame, df_play_order: DataFrame) = {

    //  选取成功付费的数据
    val df_paid_filter = df_play_order
      .join(df_select_user, Seq(Dic.colUserId), "inner") //选择的部分用户
      .filter(
        col(Dic.colOrderStatus) =!= 0 // 付费的订单和播放数据
      )

    printDf("df_paid_filter", df_paid_filter)


    println("Amount of play and paid order in package list: ", df_paid_filter.count())


    /**
     * 获取order和play时间差
     * 获得同一用户下一个action的时间
     */

    val win1 = Window.partitionBy(Dic.colUserId).orderBy(Dic.colCreationTime)

    val df_action_gap = df_paid_filter
      .withColumn(Dic.colStartTimeLeadItem, lead(Dic.colCreationTime, 1).over(win1)) //下一个time
      .withColumn(Dic.colStartTimeLastItem, lag(Dic.colEndTime, 1).over(win1)) //上一个time
      .withColumn(Dic.colTimeGapLeadItem,
        unix_timestamp(col(Dic.colStartTimeLeadItem)) - unix_timestamp(col(Dic.colEndTime))
      )
      .withColumn(Dic.colTimeGapLastItem,
        unix_timestamp(col(Dic.colEndTime)) - unix_timestamp(col(Dic.colStartTimeLastItem))
      )
      .withColumn(Dic.colLeadSession, lead(Dic.colSessionId, 1).over(win1))
      .na.fill(
      Map(
        (Dic.colTimeGapLeadItem, -1),
        (Dic.colTimeGapLastItem, -1)
      )) //填充移动后产生的空值

    printDf("df_action_gap", df_action_gap)


    /**
     * 将每个session内的video id进行整合
     */

    import org.apache.spark.sql.expressions.Window
    val win2 = Window.partitionBy(Dic.colUserId, Dic.colSessionId).orderBy(Dic.colCreationTime)

    val rowCount = df_action_gap.count().toString.length

    val df_play_action = df_action_gap
      .filter(col(Dic.colSessionId) >= 0) //只选取播放历史  订单的session id 是 -1
      .withColumnRenamed(Dic.colItemId, Dic.colVideoId)


    /**
     * 对 video id进行编码
     */
    val indexer = new StringIndexer().
      setInputCol(Dic.colVideoId).
      setOutputCol(Dic.colPlayIndex)

    val df_plays_index = indexer
      .fit(df_play_action)
      .transform(df_play_action)
      .withColumn(Dic.colItemId, col(Dic.colPlayIndex) + 1) // 编码后的 video id 从 1开始


    printDf("df_plays_index", df_plays_index)

    printDf("编码后最大的video id ", df_plays_index.select(max(Dic.colItemid)))

    //  TODO 存储
    saveProcessedData(df_plays_index.select(Dic.colVideoId, Dic.colItemid), play_index_path)


    val df_play_session_list = df_plays_index
      .withColumn(Dic.colIndex, row_number().over(win2))
      .withColumn("0", lit("0")) //要pad的字符
      .withColumn(Dic.colRank, udfLpad(col(Dic.colIndex), lit(rowCount), col("0"))) //拼接列
      .drop("0")
      .withColumn("tmp_column", concat_ws(":", col(Dic.colRank), col(Dic.colItemId)))


    printDf("df_play_session_list", df_play_session_list)

    val df_session_seq = df_play_session_list
      .groupBy(col(Dic.colUserId), col(Dic.colSessionId))
      .agg(
        collect_list(col("tmp_column")).as("tmp_column")) //collect_set 会去重
      .withColumn("tmp_column_1", sort_array(col("tmp_column")))
      .withColumn(Dic.colPlayList, udfGetHistorySeq(col("tmp_column_1")))
      .select(
        Dic.colUserId,
        Dic.colSessionId,
        Dic.colPlayList
      )

    printDf("df_session_seq", df_session_seq)

    /**
     * 对同一用户全部session进行整合
     */

    val win3 = Window.partitionBy(Dic.colUserId).orderBy(Dic.colSessionId)

    val rowCount2 = df_session_seq.count().toString.length

    val df_play_session_seq = df_session_seq
      .withColumn(Dic.colIndex, row_number().over(win3))
      .withColumn("0", lit("0")) //要pad的字符
      .withColumn(Dic.colRank, udfLpad(col(Dic.colIndex), lit(rowCount2), col("0"))) //拼接列
      .drop("0")
      .withColumn("tmp_column", concat_ws(":", col(Dic.colRank), col(Dic.colPlayList)))

    printDf("df_play_session_seq", df_play_session_seq)


    val df_all_session = df_play_session_seq
      .groupBy(col(Dic.colUserId))
      .agg(
        collect_list(col("tmp_column")).as("tmp_column")) //collect_set 会去重
      .withColumn("tmp_column_1", sort_array(col("tmp_column")))
      .withColumn(Dic.colSessionList, udfGetHistorySeq(col("tmp_column_1")))
      .select(
        Dic.colUserId,
        Dic.colSessionList
      )

    // 用户历史序列
    // user, [[session_1],[session_2],...,[session_n]]

    df_all_session
  }


  def udfGetHistorySeq = udf(getHistorySeq _)

  def getHistorySeq(array: mutable.WrappedArray[String]) = {


    val result = new ListBuffer[String]()

    for (ele <- array)
      result.append(ele.split(":")(1))

    result.toList.toString()
  }


  def getUserOrderLabel(df_play: DataFrame, df_play_order: DataFrame, df_order: DataFrame) = {

    val df_play_max = df_play
      .groupBy(col(Dic.colUserId))
      .agg(max(col(Dic.colPlayEndTime)).as(Dic.colMaxPlayEndTime)) // 获取用户的最后播放时间

    printDf("df_play_max", df_play_max)



    val df_paid = df_order
      .filter(
        col(Dic.colOrderStatus) > 1 //购买订单历史
          && col(Dic.colResourceId).isin(package_list: _ *)
      )
      .withColumnRenamed(Dic.colCreationTime, Dic.colOrderCreationTime)

    /**
     * 对 package id进行编码
     */
    val indexer2 = new StringIndexer().
      setInputCol(Dic.colResourceId).
      setOutputCol(Dic.colItemId)

    val df_order_paid = indexer2
      .fit(df_paid)
      .transform(df_paid)
      .withColumn(Dic.colPackIndex, col(Dic.colItemId) + 1) // 编码后的 video id 从 1开始

    //  TODO 存储
    saveProcessedData(df_order_paid, orderIndexPath)

    printDf("df_order_paid", df_order_paid)

    printDf("编码后最大的package id ", df_order_paid.select(max(Dic.colPackIndex)))

    /**
     *  获取用户播放未来24小时的订单购买情况
     */
    val df_play_paid = df_play_max
      .join(df_order_paid, Seq(Dic.colUserId), "inner")
      .filter(
        col(Dic.colOrderCreationTime) >= col(Dic.colMaxPlayEndTime) //订单时间大于播放最后时间
          && col(Dic.colOrderCreationTime) <= udfGetLeadTime(col(Dic.colMaxPlayEndTime), lit(1)) // 订单时间小于播放时间未来24小时
      )

    printDf("df_play_paid", df_play_paid)

    val df_paid_min = df_play_paid
      .groupBy(col(Dic.colUserId))
      .agg(
        min(Dic.colOrderCreationTime).as(Dic.colOrderCreationTime)
      )

    printDf("df_paid_min", df_paid_min)


    val df_user_pack = df_play_paid
      .join(df_paid_min, Seq(Dic.colUserId, Dic.colOrderCreationTime), "inner")
      .orderBy(Dic.colUserId, Dic.colMaxPlayEndTime, Dic.colPackIndex) // ORDER BY默认从小到大排序
      .dropDuplicates() // 默认保留第一条 重复用户使用package id 小的进行去重
      .select(Dic.colUserId, Dic.colPackIndex) // 编码后的订单

    printDf("df_user_pack", df_user_pack)

    df_user_pack


  }

  def udfGetLeadTime = udf(getLeadTime _)

  def getLeadTime(now: String, lead_day: Int) = {
    /**
     * 计算未来lead_day的时间
     */

    val time_format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    val cal1 = Calendar.getInstance()
    cal1.setTime(time_format.parse(now))
    cal1.add(Calendar.DATE, lead_day)

    time_format.format(cal1.getTime())

  }


}
