package com.atguigu.spark.streaming.project.bean

/**
  * Created by shkstart on 2021/5/23.
  */
import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Date

case class AdsInfo(ts: Long,
                   area: String,
                   city: String,
                   userId: String,
                   adsId: String,
                   var timestamp: Timestamp = null,
                   var dayString: String = null, // 2021-05-23
                   var hmString: String = null) { // 14:17

  timestamp = new Timestamp(ts)

  val date = new Date(ts)
  dayString = new SimpleDateFormat("yyyy-MM-dd").format(date)
  hmString = new SimpleDateFormat("HH:mm").format(date)
}

