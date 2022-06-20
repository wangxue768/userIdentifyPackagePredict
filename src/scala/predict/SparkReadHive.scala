package predict

//import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object SparkReadHive {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\hadoop-2.7.1")

    val conf = new SparkConf()
      .setAppName(this.getClass.getSimpleName)
//      .setMaster("local[*]")
      .setMaster("spark://10.102.0.195:7077")


    val spark = SparkSession
      .builder()
      .master("spark://10.102.0.195:7077")
      .config(conf)
//      .config("dfs.client.use.datanode.hostname", "true") //以域名的方式返回 访问 相互通信
      .config("spark.sql.warehouse.dir", "hdfs://10.102.0.195:9000/user/hive/warehouse")
      .enableHiveSupport() //启动hive读取配置文件
      .getOrCreate()

    val data = spark.sql("select * from hisense.play")
    data.show()

//    spark.stop()

  }
}
