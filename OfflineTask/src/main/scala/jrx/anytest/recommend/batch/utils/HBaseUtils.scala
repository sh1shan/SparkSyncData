package jrx.anytest.recommend.batch.utils

import jrx.anytest.recommend.batch.utils.ApplicationTools.getValue
import org.apache.commons.lang.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Put, Result}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants}
import org.slf4j.{Logger, LoggerFactory}

object HBaseUtils extends Serializable {
  val logger: Logger = LoggerFactory.getLogger(HBaseUtils.getClass)

  def getHConf: Configuration = {
    val conf = HBaseConfiguration.create
    conf.set(HConstants.ZOOKEEPER_QUORUM, getValue("hbase.zookeeper.quorum"))
    conf.set(HConstants.ZOOKEEPER_CLIENT_PORT, getValue("hbase.zookeeper.clientPort"))
    conf
  }

  def initHBase(): Connection = {
    initHBase(getHConf)

  }

  def initHBase(conf: Configuration): Connection = {
    logger.info("初始化HBase client")
    ConnectionFactory.createConnection(conf)
  }

  def close(conn: Connection): Unit = {
    try {
      conn.close()
      logger.info("关闭HBase Client")
    } catch {
      case e: Exception => logger.error(s"关闭HBase client异常_${e.getMessage}", e)
    }
  }

  def getString(result: Result, family: String, key: String): String = {
    Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes(key)))
  }

  def getString(result: Result, family: Array[Byte], key: Array[Byte]): String = {
    Bytes.toString(result.getValue(family, key))
  }

  /**
    * 倒装
    *
    * @param str key
    * @param r   长度
    * @return
    */
  def reverse(str: String, r: Int): String = {
    val len = str.length
    if (len <= r) str else str.substring(len - r, len) + str.substring(0, len - r)
  }

  /**
    * 加入ECFCU倒桩
    *
    * @param str key
    * @param r   长度
    * @return 倒转结果
    */
  def reverseSenAdd(str: String, r: Int): String = {
    val len = str.length
    if (len <= r) str else str.substring(len - r, len) + "ECFCU" + str.substring(0, len - r)
  }

  /**
    * 根据分区数获取分区键数组
    * 仅支持rowKey前length长度位数字
    *
    * @param region 想要切分的region个数，一个region 2-5G
    * @param length 按rowkey的前几位分，后四位倒装就填4
    * @return
    */
  def splitRegion(region: Int, length: Int): Array[Array[Byte]] = {
    val splitKeys = new Array[String](region - 1)
    val step = Math.ceil(StrictMath.pow(10, length) / region).toInt
    for (i<-1 until region){
      val spKey=String.valueOf(1*step)
      splitKeys(i-1)=StringUtils.leftPad(spKey,length,"0")+"|"
    }
    logger.info(s"根据rowKey前[$length]位划分位[$region]个region")
    logger.info(s"splitKeys:{}",splitKeys.mkString(","))
    splitKeys.map(Bytes.toBytes)
  }

}
