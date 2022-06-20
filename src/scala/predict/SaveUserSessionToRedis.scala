package predict

import mam.Dic
import mam.Utils.{getJedis, printDf}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, concat_ws}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object SaveUserSessionToRedis {
  /**
   * 读取用户历史数据，并存储到Redis中
   */
  val localPath = "/Hisense_4/data/wx/MUIN" // hdfs数据
  //  val localPath = "D:\\IdeaProjects\\userIdentifyPackagePredict\\data\\family_test.txt"

  def main(args: Array[String]): Unit = {
    val session = SparkSession
      .builder().appName(this.getClass.getSimpleName)
      .master("local[2]")
      .getOrCreate()

    // 读取数据
    val raw_data = readTxtToDataFrame(localPath, session)
    val test_data = raw_data.select(
      col(Dic.colUserId),
      concat_ws("|", col(Dic.colVideoList), col(Dic.colPlayTimeList)).cast(StringType).as("value")
    )
    printDf("test_data", test_data)

    /**
     * 存储到Redis
     */

    val redis = getJedis
    val rows = test_data.collect()
    for (a <- rows) {
      //保存到redis
      redis.set(a.get(0).toString, a.get(1).toString)
    }
    redis.close()
    println("------------------------存储数据结束------------------------")

  }

  /**
   * 读取数据
   *
   * @param filePath
   * @return
   */
  def readTxtToDataFrame(filePath: String, session: SparkSession) = {

    val schema = StructType(
      List(
        StructField(Dic.colUserId, StringType),
        StructField(Dic.colVideoList, StringType),
        StructField(Dic.colVideoTime, StringType), // 视频原有时长
        StructField(Dic.colPlayTimeList, StringType),
        StructField(Dic.colPackageId, StringType)))

    val df = session.read
      .option("delimiter", "|")
      .option("header", true)
      .schema(schema)
      .csv(localPath)

    df
  }

}
