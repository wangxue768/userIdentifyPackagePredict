package mam

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import java.text.SimpleDateFormat
import java.util.Calendar
import scala.collection.immutable.ListMap
import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

object Utils {

  def sysParamSetting() = {

    System.setProperty("hadoop.home.dir", "c:\\winutils")

    Logger.getLogger("org").setLevel(Level.ERROR)
  }


  def printDf(df_name: String, df: DataFrame) = {

    println("_____________________\n" * 2)
    println(df_name)
    println("dataframe count ", df.count())
    println("_____________________\n")
    df.show(5, false)
    println("_____________________\n")
    df.printSchema()
    println("_____________________\n" * 2)
    println(df_name + " showing done...")

  }

  def printArray(array_name: String, array_self: Array[Row]) = {

    println("_____________________\n" * 2)
    println(array_name)
    println("_____________________\n")
    array_self.take(10).foreach(println)
    println("_____________________\n" * 2)

  }

  /**
    * @author wj
    * @param [spark, orderProcessedPath]
    * @return org.apache.spark.sql.Dataset<org.apache.spark.sql.Row>
    * @description 读取数据
    */
  def getData(spark: SparkSession, path: String) = {
    spark.read.format("parquet").load(path)
  }






  //orderProcess
  def udfChangeDateFormat = udf(changeDateFormat _) //实名函数的注册 要在后面加 _(
  def changeDateFormat(date: String) = {
    /**
     * @author wj
     * @param [date]
     * @return java.lang.String
     * @describe 修改日期时间的格式
     */
    if (date == "NULL") {
      "NULL"
    } else {
      //
      try {
        val sdf = new SimpleDateFormat("yyyyMMddHHmmSS")
        val dt: Long = sdf.parse(date).getTime()
        val new_time: String = new SimpleDateFormat("yyyy-MM-dd HH:mm:SS").format(dt)
        new_time
      }
      catch {
        case e: Exception => {
          "NULL"
        }
      }
    }

  }

  def udfLongToDateTime = udf(longToDateTime _)

  def longToDateTime(time: Long) = {
    /**
     * @description Long类型转换成时间格式
     * @author wx
     * @param [time]
     * @return {@link java.lang.String }
     * */
    val newTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(time * 1000)
    newTime
  }


  def udfLongToTimestamp = udf(longToTimestamp _)

  def longToTimestamp(time: String) = {
    /**
     * @author wj
     * @param [time]
     * @return java.lang.String
     * @describe 将long类型的时间戳，转化为yyyy-MM-dd HH:mm:ss的字符串
     */
    if (time == "NULL") {
      "NULL"
    }
    if (time.length < 10) {
      "NULL"
    }
    else {
      val time_long = time.toLong
      val new_time: String = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(time_long * 1000)
      new_time
    }
  }

  /**
    * @author wj
    * @param [time]
    * @return java.lang.String
    * @describe 将long类型的时间戳，转化为yyyy-MM-dd HH:mm:ss的字符串
    */
  def udfLongToTimestampV2 = udf(longToTimestampV2 _)

  def longToTimestampV2(time: String) = {

    if (time == "NULL") {
      null
    }
    if (time == null) {
      null
    }
    if (time.length != 10) {
      null
    }
    else {
      try {
        val time_long = time.toLong
        val new_time: String = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(time_long * 1000)
        new_time

      } catch {
        // 数据转换异常捕获
        case ex: NumberFormatException => {
          null
        }
      }
    }
  }

  def udfGetString = udf(getString _)

  def getString(features: Vector[Double]) = {
    /**
     * @author wj
     * @param [features]
     * @return java.lang.String
     * @describe 转化为字符串
     */
    features.mkString(",")
  }

  def udfAddOrderStatus = udf(addOrderStatus _)

  def addOrderStatus(arg: String) = {
    /**
     * @author wj
     * @param [arg]
     * @return int
     * @describe 主要在构造训练集的时候创建标签
     */
    if (arg.getClass.getName == "java.lang.String") 1 else 0
  }

  def udfAddSuffix = udf(addSuffix _)

  def addSuffix(playEndTime: String) = {
    /**
     * @author wj
     * @param [playEndTime]
     * @return java.lang.String
     * @describe 在日期后面添加后缀，主要用在PlayProcess文件里面
     */
    playEndTime + " 00:00:00"
  }


  //工具函数，计算一个日期加上几天后的日期
  def calDate(date: String, days: Int): String = {
    /**
     * @author wj
     * @param [date, days]
     * @return java.lang.String
     * @describe 非udf函数，计算一个日期加上几天后的日期
     */
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:SS")
    val dt = sdf.parse(date);
    val rightNow = Calendar.getInstance()
    rightNow.setTime(dt)
    rightNow.add(Calendar.DATE, days) //日期加天
    val dt1 = rightNow.getTime()
    val reStr = sdf.format(dt1)
    return reStr
  }


  //计算日期相差的天数
  def udfGetDays = udf(getDays _)

  def getDays(date: String, now: String) = {
    /**
     * @author wj
     * @param [date, now]
     * @return int
     * @describe 计算两个日期相差的天数
     */
    if (date == null) {
      -1
    } else {
      try {
        val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:SS")
        val d1 = sdf.parse(now)
        var d2 = sdf.parse(now)
        if (date.length < 19) {
          d2 = sdf.parse(date + " 00:00:00")
        } else {
          d2 = sdf.parse(date)
        }
        var daysBetween = 0
        if (now > date) {
          daysBetween = ((d1.getTime() - d2.getTime() + 1000000) / (60 * 60 * 24 * 1000)).toInt
        } else {
          daysBetween = ((d2.getTime() - d1.getTime() + 1000000) / (60 * 60 * 24 * 1000)).toInt
        }
        daysBetween
      } catch {
        case e: Exception => {
          -1
        }
      }
    }
  }

  //根据 time_validity 和 resource_type 填充order中 discount_description 为 null的数值
  def udfFillDiscountDescription = udf(fillDiscountDescription _)

  def fillDiscountDescription(resourceType: Double, timeValidity: Int): String = {
    /**
     * @description 订单打折描述填充
     * @author wx
     * @param [resourceType] 订单资源类型
     * @param [timeValidity] 订单有效时长
     * @return {@link java.lang.String }
     * */
    var dis = ""
    if (resourceType == 0.0) {
      dis = "单点"
    } else {
      if (timeValidity <= 31) {
        dis = "包月"
      } else if (timeValidity > 31 && timeValidity < 180) {
        dis = "包季"
      } else if (timeValidity >= 180 && timeValidity < 360) {
        dis = "包半年"
      } else if (timeValidity >= 360) {
        dis = "包年"
      }
    }
    return dis

  }

  /**
    * @description 统一有效时长
    * @author wx
    * @param [timeValidity] 订单有效时长
    * @param [resourceType] 订单资源类型
    * @return {@link int }
    * */
  def udfUniformTimeValidity = udf(uniformTimeValidity _)

  def uniformTimeValidity(timeValidity: Int, resourceType: Int): Int = {

    if (resourceType == 0 || resourceType >= 4) {
      return timeValidity
    }
    if (timeValidity >= 360) {
      365
    } else if (timeValidity >= 183) {
      183
    } else if (timeValidity >= 80) {
      92
    } else {
      30
    }
  }

  def udfGetKeepSign = udf(getKeepSign _)

  def getKeepSign(creationTime: String, startTime: String): Int = {
    /**
     * @description 创建时间与生效时间的计算 返回是否保留的标记
     * @author wx
     * @param [creationTime] 订单创建时间
     * @param [startTime] 订单开始生效时间
     * @return {@link int }
     * */
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:SS")
    val d1 = sdf.parse(creationTime)
    val d2 = sdf.parse(startTime)

    if (d1.getTime() <= d2.getTime() + 60000) { //创建时间小于生效时间加1min
      1
    } else {
      0
    }

  }


  def udfGetErrorMoneySign = udf(getErrorMoneySign _)

  def getErrorMoneySign(resourceType: Int, money: Double): Int = {
    /**
     * @description: sign order with money error
     * @param: resourceType
     * @param: money
     * @return: int
     * @author: wx
     * @Date: 2020/11/25
     */

    var sign = 0
    if (resourceType > 0 && resourceType < 4 && (money == 0 || money == 100))
      sign = 1

    sign
  }


  //类似于计算wordcount
  def udfGetLabelAndCount = udf(getLabelAndCount _)

  def getLabelAndCount(array: mutable.WrappedArray[String]) = {
    /**
     * @author wj
     * @param [array]
     * @return scala.collection.immutable.ListMap<java.lang.String,java.lang.Object>
     * @describe 类似于计算wordcount,计算每个用户看过的视频中每种一级分类和它对应的个数
     */
    val group_data = array.map(item => (item, 1)).groupBy(item => item._1)
    val res = group_data.map(tp => {
      val list: mutable.WrappedArray[(String, Int)] = tp._2
      val counts: mutable.WrappedArray[Int] = list.map(t => t._2)
      (tp._1, counts.sum)
    })
    //res
    ListMap(res.toSeq.sortWith(_._2 > _._2): _ *)
  }

  def udfGetLabelAndCount2 = udf(getLabelAndCount2 _)

  def getLabelAndCount2(array: mutable.WrappedArray[mutable.WrappedArray[String]]) = {
    /**
     * @author wj
     * @param [array]
     * @return scala.collection.immutable.ListMap<java.lang.String,java.lang.Object>
     * @describe 计算每个用户看过的视频中每种二级分类或者标签以及它对应的个数
     */
    //可变长数组
    var res: Array[String] = Array()
    for (a <- array) {
      //a.foreach(item=>res.addString(new StringBuilder(item)))
      res = res.union(a)
    }
    val group_data = res.map(item => (item, 1)).groupBy(item => item._1)
    val result = group_data.map(tp => {
      val list: mutable.WrappedArray[(String, Int)] = tp._2
      val counts: mutable.WrappedArray[Int] = list.map(t => t._2)
      (tp._1, counts.sum)
    })
    //result
    ListMap(result.toSeq.sortWith(_._2 > _._2): _ *)
  }


  def udfBreak = udf(break _)

  def break(array: Object, index: Int) = {
    /**
     * @author wj
     * @param [array, index]
     * @return double
     * @description 将一个vector拆分成多个列
     */
    val vectorString = array.toString
    vectorString.substring(1, vectorString.length - 1).split(",")(index).toDouble

  }

  def udfBreakList = udf(breakList _)

  def breakList(array: Object, index: Int) = {
    /**
     * @author wj
     * @param [array, index]
     * @return double
     * @description 将一个vector拆分成多个列
     */
    val vectorString = array.toString
    vectorString.substring(13, vectorString.length - 1).split(",")(index)

  }


  def udfFillPreference = udf(fillPreference _)

  def fillPreference(prefer: Map[String, Int], offset: Int) = {
    /**
     * @author wj
     * @param [prefer, offset]
     * @return java.lang.String
     * @description 新添加一列，将该列填充为prefer中的标签
     */
    if (prefer == null) {
      null
    } else {
      val mapArray = prefer.toArray
      if (mapArray.length > offset - 1) {
        mapArray(offset - 1)._1
      } else {
        null
      }

    }

  }

  def udfFillPreferenceIndex = udf(fillPreferenceIndex _)

  def fillPreferenceIndex(prefer: String, mapLine: String) = {
    /**
     * @author wj
     * @param [prefer, mapLine]
     * @return scala.Option<java.lang.Object>
     * @description 将udfFillPreference填充的标签，转化为标签对应的index
     */
    if (prefer == null) {
      null
    } else {
      var tempMap: Map[String, Int] = Map()
      var lineIterator1 = mapLine.split(",")
      //迭代打印所有行
      lineIterator1.foreach(m => tempMap += (m.split(" -> ")(0) -> m.split(" -> ")(1).toInt))
      tempMap.get(prefer)
    }
  }


  def udfArrayToString = udf(arrayToString _)

  def arrayToString(array: mutable.WrappedArray[String]) = {
    /**
     * @description: 对Array[String]转换成String并去掉类型的前缀
     * @param: array
     * @return: java.lang.String
     * @author: wx
     * @Date: 2020/11/25
     */
    val str = array.toString
    val subStr = str.substring(13, str.length - 1)
    subStr
  }


  def udfGetTopNHistory = udf(getTopNHistory _)

  def getTopNHistory(array: mutable.WrappedArray[String], topN: Int) = {
    /**
     * @description: Split List element by ":" to get tail of the element
     * @param: array : The history list
     * @param: topN : The number of element
     * @return: scala.collection.immutable.Vector<java.lang.String> : The topN elements' vectors of the history list
     * @author: wx
     * @Date: 2020/11/26
     */


    val result = new ListBuffer[String]()

    for (ele <- array)
      result.append(ele.split(":")(1))

    // list 的长度是从0开始的
    if (result.size >= topN) {
      result.take(topN).toVector //take(3)是选取前3个数据
    } else {
            for (i <- result.size to topN - 1) {
              result.append("0")
            }
      result.toVector
    }

  }


  def udfGetAllHistory = udf(getAllHistory _)

  def getAllHistory(array: mutable.WrappedArray[String]) = {

    /**
     * @description: Split List element by ":" to get tail of the element
     * @param: array
     * @return: scala.collection.immutable.Vector<java.lang.String> : All elements' vectors of the list which get after collect_list
     * @author: wx
     * @Date: 2020/11/26
     */


    val result = new ListBuffer[Double]()

    for (ele <- array)
      ele.split(":")(1).split(",").foreach(x => result.append(x.toDouble))

    result.toArray

  }


  def udfLpad = udf(lpad _)

  def lpad(str: String, num: Int, pad: String): String = {
    /**
     * @author wx
     * @param
     * @return java.lang.String
     * @describe 实现oracle lpad函数
     */
    var n_str = str;
    if (str == null)
      n_str = " ";
    for (i <- str.length() until num) {
      n_str = pad + n_str;
    }
    n_str;
  }


  def udfSplitArrayStrColumn = udf(splitArrayString _)

  def splitArrayString(array: mutable.WrappedArray[String], index: Int) = {
    /**
     * @description: 拆分一列
     * @param: str
     * @param: index
     * @return: double
     * @author: wx
     * @Date: 2020/11/25
     */

    val str = array.toString()
    if (str.size < 50) {
      0.0
    } else {
      str.substring(13, str.length - 1).split(",")(index).toDouble
    }

  }

  def udfLog = udf(getLog _)

  def getLog(num: Double) = {
    /**
     * @description: math.log to dataframe column
     * @param: num
     * @return: double
     * @author: wx
     * @Date: 2020/11/26
     */
    math.log(num)
  }


  def udfWeightVector = udf(weightVector _)

  def weightVector(vectorStr: String, weight: Double) = {
    /**
     * @description: vector * weight
     * @param: vectorStr vector but changed to string
     * @param: weight
     * @return: scala.collection.mutable.ArrayBuffer<java.lang.Object>
     * @author: wx
     * @Date: 2020/11/29
     */
    var wightVector = new ArrayBuffer[Double]()
    vectorStr.substring(1, vectorStr.size - 1).split(",").foreach(x =>
      wightVector.append(x.toDouble * weight)
    )
    wightVector.toString()
  }



  def mapIdToMediasVector(mediaMap: mutable.HashMap[String, String]) = udf((idList: mutable.WrappedArray[String]) =>

    /**
     * @description: Map video_id_list to medias video vector
     * @param: mediaMap : medias video vector Map( id -> Vector)
     * @return: org.apache.spark.sql.expressions.UserDefinedFunction
     * @author: wx
     * @Date: 2020/11/30
     */
    idList.map(item => mediaMap.getOrElse(item, "[0]"))
  )

}

