package train.common

import mam.GetSaveData.{hdfsPath, saveProcessedData}
import mam.{Dic, SparkSessionInit}
import mam.SparkSessionInit.spark
import mam.Utils.{getData, printDf, sysParamSetting, udfLpad}
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, collect_list, concat_ws, lit, max, row_number, sort_array}
import train.common.PlayOrderSequence.play_order_seq_path
import train.common.UserLabel.{getUserSessionList, orderIndexPath, order_path, package_list, udfGetHistorySeq, udfGetLeadTime}
import train.userpay.userProfile.UserProfileGenerateOrderPart.playProcessedPath

object GetPackageList {
  val user_label_path = hdfsPath + "data/wx/user_label"

  val user_paid_hist_path = hdfsPath + "data/wx/user_package_hist"
  def main(args: Array[String]): Unit = {
    println(this.getClass.getName)

    // TODO 1 Spark初始化
    sysParamSetting
    SparkSessionInit.init()

    //TODO 2 数据读取

    val df_play = getData(spark, playProcessedPath)
    val df_orders = getData(spark, order_path)
    val df_dataset = getData(spark, user_label_path)

    val df_select_user = df_dataset.select(Dic.colUserId).dropDuplicates()
    printDf("df_select_user", df_select_user)


    // 已经选择用户的播放历史
    val df_play_users = df_play.join(df_select_user, Seq(Dic.colUserId), "inner")
    printDf("df_play_users", df_play_users)

    // 获取用户的最后播放时间
    val df_play_max = df_play_users
      .groupBy(col(Dic.colUserId))
      .agg(max(col(Dic.colPlayEndTime)).as(Dic.colMaxPlayEndTime))

    printDf("df_play_max", df_play_max)


    val df_paid = df_orders
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


    val df_user_paid = df_order_paid.join(df_select_user, Seq(Dic.colUserId), "inner") // 只选择部分用户

    //  TODO 获取用户的付费列表
    val df_user_paid_list = getUserPackageList(df_play_max, df_user_paid)
    printDf("df_user_paid_list", df_user_paid_list)

    //  TODO 存储
    val df_all_user_paid = df_select_user.join(df_user_paid_list, Seq(Dic.colUserId), "left")

    saveProcessedData(df_all_user_paid, user_paid_hist_path)

  }


  def getUserPackageList(df_play_max: DataFrame,df_user_paid: DataFrame ) = {


    /**
     * 获取用户播放之前的订单历史
     * 订单是从5月份开始的
     */
    val df_paid_his = df_play_max
      .join(df_user_paid, Seq(Dic.colUserId), "inner")
      .filter(
        col(Dic.colOrderCreationTime) < col(Dic.colMaxPlayEndTime) //订单时间大于播放最后时间
      )


    /**
     * 获取用户的购买序列
     */

    val win3 = Window.partitionBy(Dic.colUserId).orderBy(Dic.colOrderCreationTime)

    val rowCount2 = df_paid_his.count().toString.length

    val df_paid_list = df_paid_his
      .withColumn(Dic.colIndex, row_number().over(win3))
      .withColumn("0", lit("0")) //要pad的字符
      .withColumn(Dic.colRank, udfLpad(col(Dic.colIndex), lit(rowCount2), col("0"))) //拼接列
      .drop("0")
      .withColumn("tmp_column", concat_ws(":", col(Dic.colRank), col(Dic.colPackIndex)))

    printDf("df_paid_list", df_paid_list)


    val df_all_paid = df_paid_list
      .groupBy(col(Dic.colUserId))
      .agg(
        collect_list(col("tmp_column")).as("tmp_column")) //collect_set 会去重
      .withColumn("tmp_column_1", sort_array(col("tmp_column")))
      .withColumn(Dic.colPackageList, udfGetHistorySeq(col("tmp_column_1")))
      .select(
        Dic.colUserId,
        Dic.colPackageList
      )

    // user, [pack_index]

    df_all_paid

  }




}
