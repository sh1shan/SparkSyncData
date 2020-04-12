package jrx.anytest.recommend.batch.utils

import java.util

import com.lambdaworks.redis.api.{StatefulRedisConnection, StatefulConnection}
import org.slf4j.{Logger, LoggerFactory}
import com.lambdaworks.redis.{RedisClient, RedisURI}
import com.lambdaworks.redis.resource.{ClientResources, DefaultClientResources}
import com.lambdaworks.redis.cluster.RedisClusterClient
import com.lambdaworks.redis.cluster.api.StatefulRedisClusterConnection
import jrx.anytest.recommend.batch.utils.ApplicationTools.getValue

object RedisUtils {
  val logger: Logger = LoggerFactory.getLogger(RedisUtils.getClass)

  private def clientResource: ClientResources = {
    DefaultClientResources.create()
  }

  /**
    * 初始化redis连接
    *
    * @return
    */
  def initRedis: StatefulRedisConnection[String, String] = {
    logger.info("初始化 redis standalone connection")
    RedisClient.create(
      clientResource,
      RedisURI.create("redis://" + getValue("redis.standalone.node"))
    ).connect()
  }

  private def initRedisCluster(redisNodes: String): RedisClusterClient = {
    val redisUris = redisNodes.split(",").map(uri => RedisURI.create("redis://" + uri))
    RedisClusterClient.create(clientResource, util.Arrays.asList(redisUris: _*))
  }

  def initSsyxRedisCluster(): StatefulRedisClusterConnection[String, String] = {
    logger.info("初始化 ssys redis cluster connection")
    initRedisCluster(getValue("redis.cluster.ssyx.nodes")).connect()
  }

  def close(connection: StatefulConnection[String, String]): Unit = {
    try {
      connection.close()
      logger.info("关闭redis connection")
    } catch {
      case e: Exception => logger.error(s"关闭redis connection 异常_${e.getMessage}", e)
    }
  }

  def isOk(result: String): Boolean = "OK".equalsIgnoreCase(result)
}
