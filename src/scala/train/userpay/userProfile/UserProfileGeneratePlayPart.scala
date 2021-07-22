package train.userpay.userProfile

import mam.GetSaveData.{getProcessedMedias, getProcessedPlay, getTrainUser, saveUserProfileOrderPart, saveUserProfilePlayPart}
import mam.SparkSessionInit.spark
import mam.Utils.{calDate, getData, printDf, sysParamSetting, udfCalDate, udfGetDays}
import mam.{Dic, SparkSessionInit}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import train.common.UserLabel.{package_list, user_label_path}
import train.userpay.userProfile.UserProfileGenerateOrderPart.playProcessedPath

/**
 * @ClassName UserProfileGeneratePlayPartForUserpay
 * @author wx
 * @Description TODO
 * @createTime 2021年06月09日 12:08:00
 */
object UserProfileGeneratePlayPart {

  def main(args: Array[String]): Unit = {


    println(this.getClass.getName)


    // 1 SparkSession init
    sysParamSetting()
    SparkSessionInit.init()


    // 2 Get Data

    val df_plays = getData(spark, playProcessedPath)
    printDf("输入 df_plays", df_plays)

    val df_medias = getProcessedMedias(spark)
    printDf("输入 df_medias", df_medias)


    val df_train_users = getData(spark, user_label_path)
    printDf("输入 df_train_users", df_train_users)

    // 3 Process Data
    val df_user_profile_play = userProfileGeneratePlayPart(df_plays, df_train_users, df_medias)

    // 4 Save Data

    saveUserProfilePlayPart(df_user_profile_play, "train")

    printDf("输出 df_user_profile_play", df_user_profile_play)

    println("用户画像play部分生成完毕。")


  }

  def userProfileGeneratePlayPart(df_plays: DataFrame, df_train_users: DataFrame, df_medias: DataFrame): DataFrame = {


    val df_train_id = df_train_users.select(Dic.colUserId)

    val joinKeysUserId = Seq(Dic.colUserId)

    val df_play_max = df_plays
      .groupBy(Dic.colUserId).agg(max(Dic.colPlayEndTime).as(Dic.colMaxPlayEndTime))


    val df_train_plays = df_plays
      .join(df_play_max, joinKeysUserId, "inner")
      .join(df_train_id, joinKeysUserId, "inner")
      .withColumn(Dic.colPlayDate, col(Dic.colPlayEndTime).substr(1, 10))

    printDf("df_train_plays", df_train_plays)


    /**
     * 用户画像play part 时间类相关信息
     * 时长类转换成分钟
     *
     * 最近活跃时间
     */
    val df_play_part_1 = df_train_plays
      .filter(
        col(Dic.colPlayEndTime) < col(Dic.colMaxPlayEndTime) // 不能选择等于
          && col(Dic.colPlayEndTime) >= (udfCalDate(col(Dic.colMaxPlayEndTime), lit(-30))))
      .groupBy(col(Dic.colUserId))
      .agg(
        countDistinct(col(Dic.colPlayDate)).as(Dic.colActiveDaysLast30Days),
        sum(col(Dic.colBroadcastTime)).as(Dic.colTotalTimeLast30Days),
        udfGetDays(max(col(Dic.colPlayEndTime)), max(col(Dic.colPlayEndTime))).as(Dic.colDaysFromLastActive),
        udfGetDays(min(col(Dic.colPlayEndTime)), max(col(Dic.colPlayEndTime))).as(Dic.colDaysSinceFirstActiveInTimewindow))
      .withColumn(Dic.colTotalTimeLast30Days, round(col(Dic.colTotalTimeLast30Days) / 60, 0))


    val df_play_part_2 = df_train_plays
      .filter(
        col(Dic.colPlayEndTime) < col(Dic.colMaxPlayEndTime)
          && col(Dic.colPlayEndTime) >= (udfCalDate(col(Dic.colMaxPlayEndTime), lit(-14))))
      .groupBy(col(Dic.colUserId))
      .agg(
        countDistinct(col(Dic.colPlayDate)).as(Dic.colActiveDaysLast14Days),
        sum(col(Dic.colBroadcastTime)).as(Dic.colTotalTimeLast14Days)
      ).withColumn(Dic.colTotalTimeLast14Days, round(col(Dic.colTotalTimeLast14Days) / 60, 0))


    val df_play_part_3 = df_train_plays
      .filter(
        col(Dic.colPlayEndTime) < col(Dic.colMaxPlayEndTime)
          && col(Dic.colPlayEndTime) >= (udfCalDate(col(Dic.colMaxPlayEndTime), lit(-7))))
      .groupBy(col(Dic.colUserId))
      .agg(
        countDistinct(col(Dic.colPlayDate)).as(Dic.colActiveDaysLast7Days),
        sum(col(Dic.colBroadcastTime)).as(Dic.colTotalTimeLast7Days)
      )
      .withColumn(Dic.colTotalTimeLast7Days, round(col(Dic.colTotalTimeLast7Days) / 60, 0))

    val df_play_part_4 = df_train_plays
      .filter(
        col(Dic.colPlayEndTime) < col(Dic.colMaxPlayEndTime)
          && col(Dic.colPlayEndTime) >= udfCalDate(col(Dic.colMaxPlayEndTime), lit(-7)))
      .groupBy(col(Dic.colUserId))
      .agg(
        countDistinct(col(Dic.colPlayDate)).as(Dic.colActiveDaysLast3Days),
        sum(col(Dic.colBroadcastTime)).as(Dic.colTotalTimeLast3Days)
      )
      .withColumn(Dic.colTotalTimeLast3Days, round(col(Dic.colTotalTimeLast3Days) / 60, 0))


    val df_play_time = df_train_id.join(df_play_part_1, joinKeysUserId, "left")
      .join(df_play_part_2, joinKeysUserId, "left")
      .join(df_play_part_3, joinKeysUserId, "left")
      .join(df_play_part_4, joinKeysUserId, "left")

    /**
     * 付费类视频的播放相关信息
     */
    val joinKeyVideoId = Seq(Dic.colVideoId)
    val df_plays_medias = df_train_plays.select(Dic.colUserId, Dic.colVideoId, Dic.colPlayEndTime, Dic.colBroadcastTime, Dic.colMaxPlayEndTime)
      .join(df_medias, joinKeyVideoId, "inner")

    val df_play_medias_part_11 = df_plays_medias
      .filter(
        col(Dic.colPlayEndTime) < col(Dic.colMaxPlayEndTime)
          && col(Dic.colPlayEndTime) >= udfCalDate(col(Dic.colMaxPlayEndTime), lit(-30))
          && (col(Dic.colIsPaid).===(1) || col(Dic.colPackageId) > 0))
      .groupBy(col(Dic.colUserId))
      .agg(
        sum(col(Dic.colBroadcastTime)).as(Dic.colTotalTimePaidVideosLast30Days)
      )
      .withColumn(Dic.colTotalTimePaidVideosLast30Days, round(col(Dic.colTotalTimePaidVideosLast30Days) / 60, 0))

    val df_play_medias_part_12 = df_plays_medias
      .filter(
        col(Dic.colPlayEndTime) < col(Dic.colMaxPlayEndTime)
          && col(Dic.colPlayEndTime) >= udfCalDate(col(Dic.colMaxPlayEndTime), lit(-14))
          && (col(Dic.colIsPaid).===(1) || col(Dic.colPackageId) > 0))
      .groupBy(col(Dic.colUserId))
      .agg(
        sum(col(Dic.colBroadcastTime)).as(Dic.colTotalTimePaidVideosLast14Days)
      )
      .withColumn(Dic.colTotalTimePaidVideosLast14Days, round(col(Dic.colTotalTimePaidVideosLast14Days) / 60, 0))

    val df_play_medias_part_13 = df_plays_medias
      .filter(
        col(Dic.colPlayEndTime) < col(Dic.colMaxPlayEndTime)
          && col(Dic.colPlayEndTime) >= udfCalDate(col(Dic.colMaxPlayEndTime), lit(-7))
          && (col(Dic.colIsPaid).===(1) || col(Dic.colPackageId) > 0))
      .groupBy(col(Dic.colUserId))
      .agg(
        sum(col(Dic.colBroadcastTime)).as(Dic.colTotalTimePaidVideosLast7Days)
      )
      .withColumn(Dic.colTotalTimePaidVideosLast7Days, round(col(Dic.colTotalTimePaidVideosLast7Days) / 60, 0))

    val df_play_medias_part_14 = df_plays_medias
      .filter(
        col(Dic.colPlayEndTime) < col(Dic.colMaxPlayEndTime)
          && col(Dic.colPlayEndTime) >= udfCalDate(col(Dic.colMaxPlayEndTime), lit(-3))
          && (col(Dic.colIsPaid).===(1) || col(Dic.colPackageId) > 0))
      .groupBy(col(Dic.colUserId))
      .agg(
        sum(col(Dic.colBroadcastTime)).as(Dic.colTotalTimePaidVideosLast3Days)
      )
      .withColumn(Dic.colTotalTimePaidVideosLast3Days, round(col(Dic.colTotalTimePaidVideosLast3Days) / 60, 0))

    val df_play_medias_part_15 = df_plays_medias
      .filter(
        col(Dic.colPlayEndTime) < col(Dic.colMaxPlayEndTime)
          && col(Dic.colPlayEndTime) >= udfCalDate(col(Dic.colMaxPlayEndTime), lit(-1))
          && (col(Dic.colIsPaid).===(1) || col(Dic.colPackageId) > 0))
      .groupBy(col(Dic.colUserId))
      .agg(
        sum(col(Dic.colBroadcastTime)).as(Dic.colTotalTimePaidVideosLast1Days)
      )
      .withColumn(Dic.colTotalTimePaidVideosLast1Days, round(col(Dic.colTotalTimePaidVideosLast1Days) / 60, 0))

    val df_play_medias = df_play_time.join(df_play_medias_part_11, joinKeysUserId, "left")
      .join(df_play_medias_part_12, joinKeysUserId, "left")
      .join(df_play_medias_part_13, joinKeysUserId, "left")
      .join(df_play_medias_part_14, joinKeysUserId, "left")
      .join(df_play_medias_part_15, joinKeysUserId, "left")

    /**
     * 套餐内    我们要预测的那部分套餐
     */
    val df_play_medias_part_21 = df_plays_medias
      .filter(
        col(Dic.colPlayEndTime) < col(Dic.colMaxPlayEndTime)
          && col(Dic.colPlayEndTime) >= udfCalDate(col(Dic.colMaxPlayEndTime), lit(-30))
          && !isnan(col(Dic.colPackageId))
          && col(Dic.colPackageId).isin(package_list: _ *)
      )
      .groupBy(col(Dic.colUserId))
      .agg(
        sum(col(Dic.colBroadcastTime)).as(Dic.colTotalTimeInPackageVideosLast30Days),
        stddev(col(Dic.colBroadcastTime)).as(Dic.colVarTimeInPackageVideosLast30Days),
        countDistinct(col(Dic.colVideoId)).as(Dic.colNumberInPackagesVideosLast30Days)
      )
      .withColumn(Dic.colTotalTimeInPackageVideosLast30Days, round(col(Dic.colTotalTimeInPackageVideosLast30Days) / 60, 0))

    val df_play_medias_part_22 = df_train_plays
      .filter(
        col(Dic.colPlayEndTime) < col(Dic.colMaxPlayEndTime)
          && col(Dic.colPlayEndTime) >= udfCalDate(col(Dic.colMaxPlayEndTime), lit(-14))
          && !isnan(col(Dic.colPackageId))
          && col(Dic.colPackageId).isin(package_list: _ *)
      )
      .groupBy(col(Dic.colUserId))
      .agg(
        sum(col(Dic.colBroadcastTime)).as(Dic.colTotalTimeInPackageVideosLast14Days),
        stddev(col(Dic.colBroadcastTime)).as(Dic.colVarTimeInPackageVideosLast14Days),
        countDistinct(col(Dic.colVideoId)).as(Dic.colNumberInPackagesVideosLast14Days)
      )
      .withColumn(Dic.colTotalTimeInPackageVideosLast14Days, round(col(Dic.colTotalTimeInPackageVideosLast14Days) / 60, 0))

    val df_play_medias_part_23 = df_plays_medias
      .filter(
        col(Dic.colPlayEndTime) < col(Dic.colMaxPlayEndTime)
          && col(Dic.colPlayEndTime) >= udfCalDate(col(Dic.colMaxPlayEndTime), lit(-7))
          && !isnan(col(Dic.colPackageId))
          && col(Dic.colPackageId).isin(package_list: _ *)
      )
      .groupBy(col(Dic.colUserId))
      .agg(
        sum(col(Dic.colBroadcastTime)).as(Dic.colTotalTimeInPackageVideosLast7Days),
        stddev(col(Dic.colBroadcastTime)).as(Dic.colVarTimeInPackageVideosLast7Days),
        countDistinct(col(Dic.colVideoId)).as(Dic.colNumberInPackagesVideosLast7Days)
      )
      .withColumn(Dic.colTotalTimeInPackageVideosLast7Days, round(col(Dic.colTotalTimeInPackageVideosLast7Days) / 60, 0))

    val df_play_medias_part_24 = df_plays_medias
      .filter(
        col(Dic.colPlayEndTime) < col(Dic.colMaxPlayEndTime)
          && col(Dic.colPlayEndTime) >= udfCalDate(col(Dic.colMaxPlayEndTime), lit(-3))
          && !isnan(col(Dic.colPackageId))
          && col(Dic.colPackageId).isin(package_list: _ *)
      )
      .groupBy(col(Dic.colUserId))
      .agg(
        sum(col(Dic.colBroadcastTime)).as(Dic.colTotalTimeInPackageVideosLast3Days),
        stddev(col(Dic.colBroadcastTime)).as(Dic.colVarTimeInPackageVideosLast3Days),
        countDistinct(col(Dic.colVideoId)).as(Dic.colNumberInPackagesVideosLast3Days)
      )
      .withColumn(Dic.colTotalTimeInPackageVideosLast3Days, round(col(Dic.colTotalTimeInPackageVideosLast3Days) / 60, 0))

    val df_play_medias_part_25 = df_plays_medias
      .filter(
        col(Dic.colPlayEndTime) < col(Dic.colMaxPlayEndTime)
          && col(Dic.colPlayEndTime) >= udfCalDate(col(Dic.colMaxPlayEndTime), lit(-1))
          && !isnan(col(Dic.colPackageId))
          && col(Dic.colPackageId).isin(package_list: _ *)
      )
      .groupBy(col(Dic.colUserId))
      .agg(
        sum(col(Dic.colBroadcastTime)).as(Dic.colTotalTimeInPackageVideosLast1Days),
        stddev(col(Dic.colBroadcastTime)).as(Dic.colVarTimeInPackageVideosLast1Days),
        countDistinct(col(Dic.colVideoId)).as(Dic.colNumberInPackagesVideosLast1Days)
      )
      .withColumn(Dic.colTotalTimeInPackageVideosLast1Days, round(col(Dic.colTotalTimeInPackageVideosLast1Days) / 60, 0))

    val df_medias_play = df_play_medias.join(df_play_medias_part_21, joinKeysUserId, "left")
      .join(df_play_medias_part_22, joinKeysUserId, "left")
      .join(df_play_medias_part_23, joinKeysUserId, "left")
      .join(df_play_medias_part_24, joinKeysUserId, "left")
      .join(df_play_medias_part_25, joinKeysUserId, "left")


    val df_play_medias_part_31 = df_plays_medias
      .filter(
        col(Dic.colPlayEndTime) < col(Dic.colMaxPlayEndTime)
          && col(Dic.colPlayEndTime) >= udfCalDate(col(Dic.colMaxPlayEndTime), lit(-30))
          && col(Dic.colVideoOneLevelClassification).===("幼儿"))
      .groupBy(col(Dic.colUserId))
      .agg(
        sum(col(Dic.colBroadcastTime)).as(Dic.colTotalTimeChildrenVideosLast30Days),
        countDistinct(col(Dic.colVideoId)).as(Dic.colNumberChildrenVideosLast30Days)
      )
      .withColumn(Dic.colTotalTimeChildrenVideosLast30Days, round(col(Dic.colTotalTimeChildrenVideosLast30Days) / 60, 0))

    val df_play_medias_part_32 = df_plays_medias
      .filter(
        col(Dic.colPlayEndTime) < col(Dic.colMaxPlayEndTime)
          && col(Dic.colPlayEndTime) >= udfCalDate(col(Dic.colMaxPlayEndTime), lit(-14))
          && col(Dic.colVideoOneLevelClassification).===("幼儿"))
      .groupBy(col(Dic.colUserId))
      .agg(
        sum(col(Dic.colBroadcastTime)).as(Dic.colTotalTimeChildrenVideosLast14Days),
        countDistinct(col(Dic.colVideoId)).as(Dic.colNumberChildrenVideosLast14Days)
      )
      .withColumn(Dic.colTotalTimeChildrenVideosLast14Days, round(col(Dic.colTotalTimeChildrenVideosLast14Days) / 60, 0))

    val df_play_medias_part_33 = df_plays_medias
      .filter(
        col(Dic.colPlayEndTime) < col(Dic.colMaxPlayEndTime)
          && col(Dic.colPlayEndTime) >= udfCalDate(col(Dic.colMaxPlayEndTime), lit(-7))
          && col(Dic.colVideoOneLevelClassification).===("幼儿"))
      .groupBy(col(Dic.colUserId))
      .agg(
        sum(col(Dic.colBroadcastTime)).as(Dic.colTotalTimeChildrenVideosLast7Days),
        countDistinct(col(Dic.colVideoId)).as(Dic.colNumberChildrenVideosLast7Days)
      )
      .withColumn(Dic.colTotalTimeChildrenVideosLast7Days, round(col(Dic.colTotalTimeChildrenVideosLast7Days) / 60, 0))

    val df_play_medias_part_34 = df_plays_medias
      .filter(
        col(Dic.colPlayEndTime) < col(Dic.colMaxPlayEndTime)
          && col(Dic.colPlayEndTime) >= udfCalDate(col(Dic.colMaxPlayEndTime), lit(-3))
          && col(Dic.colVideoOneLevelClassification).===("幼儿"))
      .groupBy(col(Dic.colUserId))
      .agg(
        sum(col(Dic.colBroadcastTime)).as(Dic.colTotalTimeChildrenVideosLast3Days),
        countDistinct(col(Dic.colVideoId)).as(Dic.colNumberChildrenVideosLast3Days)
      )
      .withColumn(Dic.colTotalTimeChildrenVideosLast3Days, round(col(Dic.colTotalTimeChildrenVideosLast3Days) / 60, 0))

    val df_play_medias_part_35 = df_plays_medias
      .filter(
        col(Dic.colPlayEndTime) < col(Dic.colMaxPlayEndTime)
          && col(Dic.colPlayEndTime) >= udfCalDate(col(Dic.colMaxPlayEndTime), lit(-1))
          && col(Dic.colVideoOneLevelClassification).===("幼儿"))
      .groupBy(col(Dic.colUserId))
      .agg(
        sum(col(Dic.colBroadcastTime)).as(Dic.colTotalTimeChildrenVideosLast1Days),
        countDistinct(col(Dic.colVideoId)).as(Dic.colNumberChildrenVideosLast1Days)
      )
      .withColumn(Dic.colTotalTimeChildrenVideosLast1Days, round(col(Dic.colTotalTimeChildrenVideosLast1Days) / 60, 0))


    val df_user_profile_play = df_medias_play.join(df_play_medias_part_31, joinKeysUserId, "left")
      .join(df_play_medias_part_32, joinKeysUserId, "left")
      .join(df_play_medias_part_33, joinKeysUserId, "left")
      .join(df_play_medias_part_34, joinKeysUserId, "left")
      .join(df_play_medias_part_35, joinKeysUserId, "left")


    df_user_profile_play

  }


}
