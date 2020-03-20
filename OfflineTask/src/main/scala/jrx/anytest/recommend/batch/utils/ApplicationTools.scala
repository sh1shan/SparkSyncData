package jrx.anytest.recommend.batch.utils

import java.util.Properties

import com.mysql.jdbc.jdbc2.optional.MysqlDataSource
import redis.clients.jedis.{HostAndPort, Jedis, JedisCluster}
import java.util

import jrx.anytest.recommend.batch.monitor.TaskMonitorInfo
import org.apache.commons.dbutils.QueryRunner
import org.apache.commons.pool2.impl.GenericObjectPoolConfig

object ApplicationTools {


  val props = new Properties()
  val propsMonitor = new Properties()
  var environment: String = "dev"

  val dataSource = new MysqlDataSource()
  val dataSourceMonitor = new MysqlDataSource()
  //jedi是redis的java版本的客户端
  var jedis: JedisCluster = null


  def decode(str: String): String = {
    new String(new sun.misc.BASE64Decoder().decodeBuffer(str))
  }

  def encode(str: String): String = {
    new sun.misc.BASE64Encoder().encode(str.getBytes())
  }


  def loadConfig(env: String, redisMethod: Int = 1): Unit = {
    props.load(ApplicationTools.getClass.getResourceAsStream(("/config_" + env + ".properties")))
    dataSource.setURL(props.getProperty("dataSource.mysql.url"))
    dataSource.setUser(props.getProperty("datasource.mysql.username"))
    dataSource.setEncoding("ytf-8")
    dataSource.setPassword(decode(props.getProperty("datasource.mysql.password")))

    FileLogger.log("=======加载配置文件====,config_" + env + ".properties:" + propsMonitor.toString)
  }

  def redisCluster(): JedisCluster = {
    jedis
  }

  /**
    * 获取配置文件的值
    *
    * @param key key
    * @return value
    */
  def getValue(key: String): String = {
    props.getProperty(key)
  }

  def loadConfig3(env: String, redisMethod: Int): Unit = {
    environment = env
    props.load(ApplicationTools.getClass.getResourceAsStream("/config_" + env + ".properties"))

    val urls = props.getProperty("redis.cluster.urls")

    FileLogger.info("env: " + env + ",urls: " + urls + ",props:" + props.toString)
    if (redisMethod == 1) {
      if (null == jedis) {
        val redisClusterNodes = new util.HashSet[HostAndPort]()
        for (url <- urls.split(",")) {
          val u = url.split(":")
          redisClusterNodes.add(new HostAndPort(u(0), u(1).toInt))
        }

        val poolConfig = new GenericObjectPoolConfig()
        poolConfig.setMaxTotal(100)
        poolConfig.setMinIdle(20)
        poolConfig.setMaxIdle(80)
        poolConfig.setMaxWaitMillis(60 * 1000)
        jedis = new JedisCluster(redisClusterNodes, 5000, 50, poolConfig)
        FileLogger.info("JedisClusterConfig:" + redisClusterNodes.toArray.mkString)
      }
    }
  }

  //HBase 相关
  def hBaseScanTimeOut(): Long = {
    props.getProperty("hbase.client.timeout").toLong
  }

  //记录任务结果
  def logTask(taskInfo: TaskMonitorInfo): Unit = {
    val qr = new QueryRunner(dataSource)
    val sql = "insert into spark_task_monitor(task_name,start_time,end_time,total_count,process_count,args,info) values(?,?,?,?,?,?,?) "

    qr.update(sql, taskInfo.taskName, taskInfo.startTime, taskInfo.endTime, taskInfo.totalCount, taskInfo.processCount, taskInfo.argument, taskInfo.info)
  }

  def upd_f_monitorByDay(sign: Int, batch_dt: String): Unit = {
    val qr = new QueryRunner(dataSource)
    val sql = "update customer_f_monitor set state_flag = ? where batch_dt=?"
    qr.update(sql, Int.box(sign), batch_dt)
  }

  def loadConfig(env: String, redisMethod: Int, redisOfYxOrFk: Int): Unit = {
    environment = env
    props.load(ApplicationTools.getClass.getResourceAsStream("/config_" + env + ".properties"))
    val urls = if (redisOfYxOrFk == 1) {
      props.getProperty("redis.xjy.cluster.urls")
    } else {
      props.getProperty("redis.fk.cluster.urls")
    }

    FileLogger.info("env: " + env + ",redisOfYxOrFk==" + redisOfYxOrFk + ",urls:" + ",props:" + props.toString)
    FileLogger.log("env: " + env + ",redisOfYxOrFk==" + redisOfYxOrFk + ",urls:" + ",props:" + props.toString)

    if (redisMethod == 1) {
      if (null == jedis) {
        val redisClusterNodes = new util.HashSet[HostAndPort]
        for (url <- urls.split(",")) {
          val u = url.split(":")
          redisClusterNodes.add(new HostAndPort(u(0), u(1).toInt))
        }

        val poolConfig = new GenericObjectPoolConfig()
        poolConfig.setMaxTotal(100)
        poolConfig.setMinIdle(20)
        poolConfig.setMaxIdle(80)
        poolConfig.setMaxWaitMillis(60 * 1000)
        jedis = new JedisCluster(redisClusterNodes, 5000, 50, poolConfig)
        FileLogger.info("JedisClusterConfig:" + redisClusterNodes.toArray.mkString)
      }
    }
  }

  def loadConfigMonitor(env: String, redisMethod: Int, redisOfYxOrFk: Int): Unit = {
    environment = env

    propsMonitor.load(ApplicationTools.getClass.getResourceAsStream("/config_" + env + ".properties"))
    dataSourceMonitor.setURL(propsMonitor.getProperty("operate.mysql.url"))
    dataSourceMonitor.setUser(propsMonitor.getProperty("operate.mysql.username"))
    dataSourceMonitor.setEncoding("utf-8")
    dataSourceMonitor.setPassword(decode(propsMonitor.getProperty("operate.mysql.password")))

    props.load(ApplicationTools.getClass.getResourceAsStream("/config_" + env + ".properties"))

    val urls = if (redisOfYxOrFk == 1) {
      props.getProperty("redis.xjy.cluster.urls")
    } else {
      props.getProperty("redis.fk.cluster.urls")
    }

    FileLogger.info("env: " + env + ",redisOfYxOrFk==" + redisOfYxOrFk + ",urls:" + ",props:" + props.toString)
    FileLogger.log("env: " + env + ",redisOfYxOrFk==" + redisOfYxOrFk + ",urls:" + ",props:" + props.toString)

    if (redisMethod == 1) {
      if (null == jedis) {
        val redisClusterNodes = new util.HashSet[HostAndPort]
        for (url <- urls.split(",")) {
          val u = url.split(":")
          redisClusterNodes.add(new HostAndPort(u(0), u(1).toInt))
        }

        val poolConfig = new GenericObjectPoolConfig()
        poolConfig.setMaxTotal(100)
        poolConfig.setMinIdle(20)
        poolConfig.setMaxIdle(80)
        poolConfig.setMaxWaitMillis(60 * 1000)
        jedis = new JedisCluster(redisClusterNodes, 5000, 50, poolConfig)
        FileLogger.info("JedisClusterConfig:" + redisClusterNodes.toArray.mkString)
      }
    }
  }

  def redisSingle(redisOfYxOrFk: Int): Jedis = {
    val jedis = if (redisOfYxOrFk == 1) {
      new Jedis(props.getProperty("redis.yx.hostname"), props.getProperty("redis.yx.port").toInt)
    } else {
      new Jedis(props.getProperty("redis.yx.hostname"), props.getProperty("redis.yx.port").toInt)
    }
    jedis
  }
  def maLabelField1():String={
    props.getProperty("HBase.maLabel.Field1")
  }
  def maLabel(): String = {
    return "maLabelByDay"
  }


}
