package com.atguigu.spark.streaming.project.util

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

/**
  * Created by shkstart on 2021/5/23.
  */
object RedisUtil {
  private val conf = new JedisPoolConfig
  conf.setMaxTotal(100)
  conf.setMaxIdle(10)
  conf.setMinIdle(10)
  conf.setBlockWhenExhausted(true) // 忙碌是否等待
  conf.setMaxWaitMillis(10000) // 最大等待时间
  conf.setTestOnBorrow(true)
  conf.setTestOnReturn(true)

  val pool = new JedisPool(conf, "hadoop102", 6379)

  def getClient: Jedis = pool.getResource
}
/*
1. 使用连接池创建客户端

2. 直接创建客户端
 */
