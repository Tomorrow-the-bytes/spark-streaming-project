package com.atguigu.spark.streaming.project.app

import com.atguigu.spark.streaming.project.bean.AdsInfo
import org.apache.spark.streaming.dstream.DStream


/**
  * Created by shkstart on 2021/5/23.
  */
object AreaTopApp extends App{


  override def doSomething(adsInfoStream: DStream[AdsInfo]): Unit = {
    //    adsInfoStream.print()

    /**
      * @note The first requirement
      * @note 每天每地区热门广告 Top3
      */


    //    1. 先计算每天每地区每广告的点击量
    //      ((day,area,ads), 1) => updateStateByKey


    val dayAreaGrouped: DStream[((String, String), Iterable[(String, Int)])] = adsInfoStream
      .map(info => {
        ((info.dayString, info.area, info.adsId), 1)
      })
      .updateStateByKey(
        (seq: Seq[Int], opt: Option[Int]) => {
          Some(seq.sum + opt.getOrElse(0))
        })

//      2. 按照每天每地区分组
      .map {
        case ((day, area, ads), value) => ((day, area), (ads, value))
      }
      .groupByKey()

//    3. 每组内排序, 取前3
    val result: DStream[((String, String), List[(String, Int)])] = dayAreaGrouped.map {
      case (key, it: Iterable[(String, Int)]) => (key, it.toList.sortBy(_._2)(Ordering.Int.reverse).take(3))
    }
//    result.print(1000)


//隐式类：Redis连接
    import com.atguigu.spark.streaming.project.util.RealUtil._
    result.saveToRedis()

  }

}
/*




5. 把数据写入到redis

数据类型:
    k-v 形式数据库(nosql 数据)
    K:  都是字符串
    V的数据类型:
        5大数据类型
         1. string
         2. set 不重复
         3. list 允许重复
         4. hash map, 存的是field-value
         5. zset
----
((2020-03-24,华中),List((3,14), (1,12), (2,8)))
((2020-03-24,华东),List((2,38), (4,33), (5,32)))
((2020-03-24,华南),List((4,37), (1,36), (5,29)))
((2020-03-24,华北),List((4,41), (3,34), (1,34)))
-----
选择什么类型的数据:
每天一个key
key                                     value
"area:ads:count" + day                  hash
                                        field       value
                                        area        json字符串
                                        "华中"      {3: 14, 1:12, 2:8}

 */