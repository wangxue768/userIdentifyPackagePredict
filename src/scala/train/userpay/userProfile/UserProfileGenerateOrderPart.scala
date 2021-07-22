package train.userpay.userProfile

import mam.GetSaveData.{getProcessedOrder, getTrainUser, hdfsPath, saveDataSet, saveProcessedData, saveUserProfileOrderPart}
import mam.SparkSessionInit.spark
import mam.Utils.{calDate, getData, printDf, sysParamSetting, udfCalDate, udfGetDays}
import mam.{Dic, SparkSessionInit}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import train.common.UserLabel.{order_path, user_label_path}

/**
 * @ClassName UserProfileGenerateOrderPartForUserpay
 * @author wx
 * @Description TODO
 * @createTime 2021年06月09日 12:08:00
 */
object UserProfileGenerateOrderPart {
  val timeWindow = 30
  val playProcessedPath = hdfsPath + "data/wx/train/common/processed/play_session"

  def main(args: Array[String]): Unit = {

    println(this.getClass.getName)


    // 1 SparkSession init
    sysParamSetting()
    SparkSessionInit.init()

    // 2 Get Data

    val df_orders = getData(spark, order_path)
    printDf("输入 df_orders", df_orders)

    val df_plays = getData(spark, playProcessedPath)
    printDf("输入 df_plays", df_plays)


    val df_train_users = getData(spark, user_label_path)
    printDf("输入 df_train_users", df_train_users)

    // 3 Process Data
    val df_user_profile_order = userProfileGenerateOrderPart(df_orders, df_plays, df_train_users)

    // 4 Save Data
    saveUserProfileOrderPart(df_user_profile_order, "train")
    printDf("输出  df_user_profile_order", df_user_profile_order)

    println("用户画像订单部分生成完毕。")


  }

  def userProfileGenerateOrderPart(df_orders: DataFrame, df_play: DataFrame, df_train_users: DataFrame): DataFrame = {


    val df_train_id = df_train_users.select(Dic.colUserId)

    val joinKeysUserId = Seq(Dic.colUserId)

    val df_play_max = df_play.groupBy(Dic.colUserId).agg(max(Dic.colPlayEndTime).as(Dic.colMaxPlayEndTime))

    val df_play_order = df_train_id
      .join(df_play_max, joinKeysUserId, "inner")
      .join(df_orders, joinKeysUserId, "inner")

    printDf("df_play_order", df_play_order)


    val df_train_order = df_play_order
      .filter(
        col(Dic.colCreationTime) <= col(Dic.colMaxPlayEndTime)
          and (col(Dic.colCreationTime) >= udfCalDate(col(Dic.colMaxPlayEndTime), lit(-timeWindow * 3))))
      .join(df_train_id, Seq(Dic.colUserId), "inner")

    printDf("df_train_order", df_train_order)


    /**
     * 已支付套餐数量 金额总数 最大金额 最小金额 平均金额 并对金额进行标准化
     */
    val df_order_part_1 = df_train_order
      .filter(
        col(Dic.colResourceType).>(0)
          && col(Dic.colOrderStatus).>(1) //已支付
      )
      .groupBy(col(Dic.colUserId))
      .agg(
        count(col(Dic.colUserId)).as(Dic.colNumberPackagesPurchased),
        sum(col(Dic.colMoney)).as(Dic.colTotalMoneyPackagesPurchased),
        max(col(Dic.colMoney)).as(Dic.colMaxMoneyPackagePurchased),
        min(col(Dic.colMoney)).as(Dic.colMinMoneyPackagePurchased),
        avg(col(Dic.colMoney)).as(Dic.colAvgMoneyPackagePurchased)
      )

    printDf("df_order_part_1", df_order_part_1)

    /**
     * 单点视频
     */
    val df_order_part_2 = df_train_order
      .filter(
        col(Dic.colResourceType).===(0)
          && col(Dic.colOrderStatus).>(1)
      )
      .groupBy(col(Dic.colUserId))
      .agg(
        count(col(Dic.colUserId)).as(Dic.colNumberSinglesPurchased)
      )

    printDf("df_order_part_2", df_order_part_2)


    /**
     * 已购买的所有订单金额
     */
    val df_order_part_3 = df_train_order
      .filter(
        col(Dic.colOrderStatus).>(1)
      )
      .groupBy(col(Dic.colUserId))
      .agg(
        sum(col(Dic.colMoney)).as(Dic.colTotalMoneyConsumption)
      )

    printDf("df_order_part_3", df_order_part_3)


    /**
     * 距离上次购买最大天数
     */

    val df_order_part_6 = df_train_order
      .filter(
        col(Dic.colResourceType).>(0)
          && col(Dic.colOrderStatus).>(1)
      )
      .groupBy(col(Dic.colUserId))
      .agg(
        udfGetDays(max(col(Dic.colCreationTime)), max(col(Dic.colMaxPlayEndTime))).as(Dic.colDaysSinceLastPurchasePackage)
      )

    printDf("df_order_part_6", df_order_part_6)

    val df_user_profile_order_1 = df_train_id.join(df_order_part_1, joinKeysUserId, "left")
      .join(df_order_part_2, joinKeysUserId, "left")
      .join(df_order_part_3, joinKeysUserId, "left")
      .join(df_order_part_6, joinKeysUserId, "left")

    printDf("df_user_profile_order_1", df_user_profile_order_1)


    /**
     * 距离上次点击套餐最大天数
     */
    val df_order_part_7 = df_train_order
      .filter(
        col(Dic.colResourceType).>(0)
      )
      .groupBy(col(Dic.colUserId))
      .agg(
        udfGetDays(max(col(Dic.colCreationTime)), max(col(Dic.colMaxPlayEndTime))).as(Dic.colDaysSinceLastClickPackage)
      )

    printDf("df_order_part_7", df_order_part_7)

    /**
     * 30天前产生的订单总数
     */
    val df_order_part_8 = df_train_order
      .filter(
        col(Dic.colCreationTime) >= udfCalDate(col(Dic.colMaxPlayEndTime), lit(-30))
      )
      .groupBy(col(Dic.colUserId))
      .agg(
        count(col(Dic.colUserId)).as(Dic.colNumbersOrdersLast30Days)
      )

    printDf("df_order_part_8", df_order_part_8)

    /**
     * 30天前支付的订单数
     */
    val df_order_part_9 = df_train_order
      .filter(
        col(Dic.colCreationTime) >= udfCalDate(col(Dic.colMaxPlayEndTime), lit(-30))
          && col(Dic.colOrderStatus).>(1)
      )
      .groupBy(col(Dic.colUserId))
      .agg(
        count(col(Dic.colUserId)).as(Dic.colNumberPaidOrdersLast30Days)
      )


    val df_user_profile_order_2 = df_user_profile_order_1
      .join(df_order_part_7, joinKeysUserId, "left")
      .join(df_order_part_8, joinKeysUserId, "left")
      .join(df_order_part_9, joinKeysUserId, "left")

    printDf("df_user_profile_order_2", df_user_profile_order_2)


    val df_order_part_10 = df_train_order
      .filter(
        col(Dic.colCreationTime) >= udfCalDate(col(Dic.colMaxPlayEndTime), lit(-30))
          && col(Dic.colOrderStatus).>(1)
          && col(Dic.colResourceType).>(0)
      )
      .groupBy(col(Dic.colUserId))
      .agg(
        count(col(Dic.colUserId)).as(Dic.colNumberPaidPackageLast30Days)
      )

    val df_order_part_11 = df_train_order
      .filter(
        col(Dic.colCreationTime) >= udfCalDate(col(Dic.colMaxPlayEndTime), lit(-30))
          && col(Dic.colOrderStatus).>(1)
          && col(Dic.colResourceType).===(0)
      )
      .groupBy(col(Dic.colUserId))
      .agg(
        count(col(Dic.colUserId)).as(Dic.colNumberPaidSingleLast30Days)
      )


    /**
     * 仍然有效的套餐
     */
    val df_order_part_12 = df_train_order
      .filter(
        col(Dic.colOrderEndTime) > col(Dic.colMaxPlayEndTime)
          && col(Dic.colResourceType).>(0)
          && col(Dic.colOrderStatus).>(1)
      )
      .groupBy(col(Dic.colUserId))
      .agg(
        udfGetDays(max(col(Dic.colOrderEndTime)), max(col(Dic.colMaxPlayEndTime))).as(Dic.colDaysRemainingPackage)
      )

    printDf("df_order_part_12", df_order_part_12)

    val df_user_profile_order_3 = df_user_profile_order_2
      .join(df_order_part_10, joinKeysUserId, "left")
      .join(df_order_part_11, joinKeysUserId, "left")
      .join(df_order_part_12, joinKeysUserId, "left")

    df_user_profile_order_3
  }


}
