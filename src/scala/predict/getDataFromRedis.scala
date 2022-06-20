package predict

import com.redislabs.provider.redis.toRedisContext
import mam.{Dic, SparkSessionInit}
import mam.SparkSessionInit.spark
import mam.Utils.{getJedis, printDf, sysParamSetting}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import predict.SparkStreamingKafka.ToString

import scala.collection.mutable.ArrayBuffer


object getDataFromRedis {
  def main(args: Array[String]): Unit = {

    sysParamSetting

    val sparkConf = new SparkConf()
      .setAppName("SparkGetDataFromRedis")
      //      .setMaster("local[2]")
      .setMaster("spark://10.102.0.195:7077")

    val spark = SparkSession.builder()
      .master("spark://10.102.0.195:7077")
      .getOrCreate()

    /**
     * 从Redis中批量读取当前批次用户历史数据
     * 并更新当前会话信息
     */
    val redis = getJedis
    val keys = Array("100148047", "100233168", "100027824", "100186543", "100221503")
    val result = ArrayBuffer[Map[String, String]]()
    for (key <- keys) {
      val value = redis.get(key)
      result.append(Map(key -> value)) // 获取当前批次用户的对应历史session
    }
    println(redis.get("100148047"))
    println()
    result.foreach(x => println(x))

    redis.close()



  }

}
