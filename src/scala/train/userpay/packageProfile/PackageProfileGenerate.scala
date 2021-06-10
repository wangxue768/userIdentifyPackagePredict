package train.userpay.packageProfile

import mam.GetSaveData.getVideoProfile
import mam.SparkSessionInit.spark
import mam.Utils.{calDate, printDf, sysParamSetting}
import mam.{Dic, SparkSessionInit}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, count, mean, sum}

/**
 * @ClassName PackageProfileGenerate
 * @author wx
 * @Description TODO 套餐画像   需要按照时间进行多个视频画像的处理 生成当时订单的向量
 * @createTime 2021年04月19日 21:21:00
 */
object PackageProfileGenerate {


  def main(args: Array[String]): Unit = {
    // 1 SparkSession init
    sysParamSetting()
    SparkSessionInit.init()


    // 2 Get Data
    val now = args(0) + " " + args(1)
    val df_video_profile = getVideoProfile(spark, now, "predict")

    printDf("video_profile", df_video_profile)
    //3 Process Data
    val df_package_video_profile = packageProfileGenerate(now, df_video_profile)

    //    printDf("df_package_video_profile",df_package_video_profile)
    //4 Save Data
    //    savePackageVideoProfile(now,df_package_video_profile,"predict")
    printDf("输出 df_package_video_profile", df_package_video_profile)
    println("PackageProfileGenerate  over~~~~~~~~~~~")
  }

  //生成一个dataframe，使用video中的信息去制作一个（套餐id,是否包含火热视频）
  // --思路，video-id、package-id、counts
  //套餐画像函数定义
  def packageProfileGenerate(now: String, df_video_profile: DataFrame) = {

    val pre_30 = calDate(now, -30)
    val joinKeysPackageId = Seq(Dic.colPackageId)


    //套餐中视频播放的总次数、平均评分score、套餐中有多少电影和电视剧（一级分类）、最近一个月内套餐内新上映的电影和电视剧的个数
    val df_video_profile_new = df_video_profile
      .filter(
        col(Dic.colPackageId).isNotNull
      )
      .groupBy(col(Dic.colPackageId))
      .agg(
        mean(col(Dic.colScore)).as(Dic.colMeanPackageScore), //套餐平均分
        sum(col(Dic.colNumberOfPlaysIn30Days)).as(Dic.colNumberOfPackagePlay) //套餐一个月内被播放总次数
      )
      .select(col(Dic.colPackageId), col(Dic.colNumberOfPackagePlay), col(Dic.colMeanPackageScore))

    //套餐中电影的播放次数
    val df_video_profile_new_2 = df_video_profile
      .filter(
        col(Dic.colPackageId).isNotNull
          && col(Dic.colVideoOneLevelClassification).===("电影")
      )
      .groupBy(col(Dic.colPackageId))
      .agg(
        count(col(Dic.colVideoId)).as(Dic.colNumberOfMovies)
      )

    //套餐中电视剧的播放次数
    val df_video_profile_new_3 = df_video_profile
      .filter(
        col(Dic.colPackageId).isNotNull
          && col(Dic.colVideoOneLevelClassification).===("电视剧")
      )
      .groupBy(col(Dic.colPackageId))
      .agg(
        count(col(Dic.colVideoId)).as(Dic.colNumberOfTv)
      )
    //新入库的电影数
    val df_video_profile_new_4 = df_video_profile
      .filter(
        col(Dic.colPackageId).isNotNull
          && col(Dic.colVideoOneLevelClassification).===("电影")
          && col(Dic.colStorageTime).<(now) //获取播放时间在参数时间前一个月的数据
          && col(Dic.colStorageTime).>=(pre_30)
      )
      .groupBy(col(Dic.colPackageId))
      .agg(
        count(col(Dic.colVideoId)).as(Dic.colNumberOfMoviesIn30Days)
      )
    //新入库的电视剧数
    val df_video_profile_new_5 = df_video_profile
      .filter(
        col(Dic.colPackageId).isNotNull
          && col(Dic.colVideoOneLevelClassification).===("电视剧")
          && col(Dic.colStorageTime).<(now) //获取播放时间在参数时间前一个月的数据
          && col(Dic.colStorageTime).>=(pre_30)
      )
      .groupBy(col(Dic.colPackageId))
      .agg(
        count(col(Dic.colVideoId)).as(Dic.colNumberOfTvIn30Days)
      )

    val df_video_profile_new_final = df_video_profile_new
      .join(df_video_profile_new_2, joinKeysPackageId, "left")
      .join(df_video_profile_new_3, joinKeysPackageId, "left")
      .join(df_video_profile_new_4, joinKeysPackageId, "left")
      .join(df_video_profile_new_5, joinKeysPackageId, "left")
      .orderBy(col(Dic.colNumberOfPackagePlay).desc)

    df_video_profile_new_final
  }

}
