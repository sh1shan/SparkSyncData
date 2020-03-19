package jrx.anytest.recommend.batch.utils

import jrx.anytest.recommend.batch.utils.ApplicationTools.getValue
import org.apache.commons.lang.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Put, Result}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration, HColumnDescriptor, HConstants, HTableDescriptor, KeyValue, TableName}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat2, LoadIncrementalHFiles, TableInputFormat, TableOutputFormat}
import org.apache.spark.rdd.RDD
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.{Partitioner, SparkContext}
import org.apache.hadoop.fs.{FileSystem, Path}
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
    for (i <- 1 until region) {
      val spKey = String.valueOf(1 * step)
      splitKeys(i - 1) = StringUtils.leftPad(spKey, length, "0") + "|"
    }
    logger.info(s"根据rowKey前[$length]位划分位[$region]个region")
    logger.info(s"splitKeys:{}", splitKeys.mkString(","))
    splitKeys.map(Bytes.toBytes)
  }

  /**
    * 创建预分区表
    *
    * @param tableName  表名
    * @param familyName 列族
    * @param splitKeys  分区数
    * @param hBaseConn  HBase连接
    */
  def createTableSplitRegion(tableName: TableName, familyName: Array[Byte], splitKeys: Array[Array[Byte]], hBaseConn: Connection): Unit = {
    val tableDescriptor = new HTableDescriptor(tableName)
    tableDescriptor.addFamily(new HColumnDescriptor(familyName))
    hBaseConn.getAdmin.createTable(tableDescriptor, splitKeys)
  }

  /**
    * 创建分区表，不存在才创建
    *
    * @param tableName  表名
    * @param familyName 列族
    * @param splitKeys  分区数
    * @param hBaseConn  hBase连接
    * @return
    */
  def createTableIfNotExist(tableName: String, familyName: Array[Byte], splitKeys: Array[Array[Byte]], hBaseConn: Connection): Boolean = {
    try {
      val hBaseAdmin = hBaseConn.getAdmin
      val table = TableName.valueOf(tableName)
      if (hBaseAdmin.tableExists(table)) {
        logger.info(s"HBase表:${tableName}已存在，无需创建")
      } else {
        HBaseUtils.createTableSplitRegion(table, familyName, splitKeys, hBaseConn)
      }
      return true
    } catch {
      case e: Exception => logger.error(s"创建Hbase连接异常_${e.getMessage}", e)
    }
    false
  }

  def dropTableIfExist(tableName: String, hBaseConn: Connection): Boolean = {
    try {
      val hBaseAdmin = hBaseConn.getAdmin
      val table = TableName.valueOf(tableName)
      if (hBaseAdmin.tableExists(table)) {
        logger.info(s"Hbase表：${tableName}已存在")
        if (hBaseAdmin.isTableEnabled(table)) {
          hBaseAdmin.disableTable(table)
        }
        hBaseAdmin.deleteTable(table)
        logger.info(s"删除HBase表成功:$tableName")

      } else {
        logger.info(s"HBase表：${tableName}不存在,无需删除")
      }
      return true

    } catch {
      case e: Exception => logger.error(s"删除HBase表时异常_${e.getMessage}", e)
    }
    return false
  }

  def listTables(hBaseConn: Connection, namePrefix: String, batchDate: String, num: Int): Unit = {
    val hBaseAdmin = hBaseConn.getAdmin
    val tableDescriptor = hBaseAdmin.listTableNames()
    for (table <- tableDescriptor) {
      val hTable = table.getNameAsString
      //logger.info(s"集群所有的表名位${hTable},前缀为：${namePrefix}")
      if (hTable.startsWith(namePrefix) && hTable.length > 3
        && !"predicteddata".equals(hTable)
        && !"predicteddata2".equals(hTable)) {
        //根据需要删除表名截取后8位，和当前批次日期做判断
        val hTableSuffix = hTable.substring(hTable.length - 8, hTable.length)
        if (Integer.parseInt(batchDate) - Integer.parseInt(hTableSuffix) >= num) {
          logger.info(s"当前需要删除的HBase表为:$hTable")
          val table = TableName.valueOf(hTable)
          if (hBaseAdmin.tableExists(table)) {
            logger.info(s"HBase表${hTable}存在")
            if (hBaseAdmin.isTableEnabled(table)) {
              hBaseAdmin.disableTable(table)
            }
            hBaseAdmin.deleteTable(table)
            logger.info(s"删除HBase表成功:$hTable")
          } else {
            logger.info(s"HBase表:${hTable}不存在，不存在")
          }

        }
      }
    }
  }

  def createTableAndDropIfExist(tableName: String, family: Array[Byte], splitKeys: Array[Array[Byte]], hBaseConn: Connection): Boolean = {
    dropTableIfExist(tableName, hBaseConn) && createTableIfNotExist(tableName, family, splitKeys, hBaseConn)
  }

  /**
    * 读取HBase，生成HBase RDD
    *
    * @param tableName    表名
    * @param hBaseConf    配置
    * @param sparkContext context
    * @return
    */
  def hBaseRDD(tableName: String, hBaseConf: Configuration, sparkContext: SparkContext): RDD[(ImmutableBytesWritable, Result)] = {
    logger.info(s"读取HBase Table：$tableName")
    hBaseConf.set(TableInputFormat.INPUT_TABLE, tableName)
    sparkContext.newAPIHadoopRDD(
      hBaseConf,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result]
    )
  }

  /**
    * 配置Hadoop Job，用于rdd直接存入HBase
    * rdd:{ImmutableBytesWritable,Put}={...}
    * rdd.saveAsNewAPIHadoopDataset(job.getConfiguration)
    *
    * @param tableName 表名
    * @param hBaseConf 配置
    * @return
    */
  def confHadoopJod(tableName: String, hBaseConf: Configuration): Job = {
    hBaseConf.set(TableOutputFormat.OUTPUT_TABLE, tableName)
    val job = Job.getInstance(hBaseConf)
    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[Result])
    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])
    logger.info(s"Heartbeat -Hadoop job config,output table :$tableName")
    job
  }

  def confBulkJob(tableName: String, bulkPath: String, hBaseConn: Connection): Job = {
    confBulkJob(tableName, bulkPath, hBaseConn.getConfiguration, hBaseConn)
  }

  /**
    * 配置Hadoop Job，用于生成HFile，再调用doBulkLoad导入HBase
    * sparkConfig.set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    * .registerKryoClasses(Array(classOf([ImmutableBytesWritable]),classOf[KeyValue]))
    * ...
    * rdd.saveAsNewAPIHadoopDataset(job.getConfiguration)
    * HBaseUtil.doBulkLoad(...)
    *
    * @param tableName 表名
    * @param bulkPath  bulkFile地址
    * @param conf      配置
    * @param hBaseConn HBase连接
    * @return
    */
  def confBulkJob(tableName: String, bulkPath: String, conf: Configuration, hBaseConn: Connection): Job = {
    conf.set("mapreduce.output.fileoutputformat.outputdir", bulkPath)
    val job = Job.getInstance(conf)
    job.setMapOutputKeyClass(classOf[KeyValue])
    job.setMapOutputValueClass(classOf[ImmutableBytesWritable])
    val hTableName = TableName.valueOf(tableName)
    val hTable = hBaseConn.getTable(hTableName)
    HFileOutputFormat2.configureIncrementalLoad(job, hTable, hBaseConn.getRegionLocator(hTableName))
    logger.info(s"HeartBeat -bulk job config,mapreduce.output.fileoutputformat.outputdir:$bulkPath")
    logger.info(s"heartbeat - bulk job config,Hbase table$tableName")
    job
  }

  def doBulkLoad(tableName: String, bulkPath: String, hbaseConn: Connection): Unit = {
    doBulkLoad(tableName, bulkPath, hbaseConn.getConfiguration, hbaseConn)
  }

  /**
    * 将HFile导入HBase
    *
    * @param tableName 表名
    * @param bulkPath  HFile路径
    * @param hBaseConf 配置
    * @param hBaseConn HBase连接
    */
  def doBulkLoad(tableName: String, bulkPath: String, hBaseConf: Configuration, hBaseConn: Connection): Unit = {
    val hTableName = TableName.valueOf(tableName)
    logger.info(s"Heartbeat - bulk load ,HFile path :$bulkPath")
    logger.info(s"Heartbeat - bulk load ,HBase table :$tableName")
    val time = System.currentTimeMillis()
    new LoadIncrementalHFiles(hBaseConf)
      .doBulkLoad(
        new Path(bulkPath),
        hBaseConn.getAdmin,
        hBaseConn.getTable(hTableName),
        hBaseConn.getRegionLocator(hTableName)
      )
    logger.info("Heartbeat - bulk load finish,耗时：{} ms", System.currentTimeMillis() - time)

  }

  /**
    * 删除HDFS上的文件
    *
    * @param sparkContext sparkContext
    * @param hadoopPath   hadoop文件地址
    */
  def delHadoopFile(sparkContext: SparkContext, hadoopPath: String*): Unit = {
    val hdfs = FileSystem.get(sparkContext.hadoopConfiguration)
    hadoopPath.foreach(p => {
      val path = new Path(p)
      if (hdfs.exists(path)) {
        try {
          hdfs.delete(path, true)
          logger.info(s"删除hdfs路径：$p")
        } catch {
          case e: Exception => logger.error(s"删除hdfs目录[$p]时异常_${e.getMessage}", e)
        }
      } else {
        logger.info(s"hdfs路径不存在[$p],不需要删除")
      }
    })
  }

  /**
    * Put 转KeyValue数组
    * @param put 对象
    * @return
    */
  def putToKeyValue(put: Put): Array[OrderedKeyValue] = {
    put.getFamilyCellMap.asScala
      .flaMap(_._2.asScala)
      .map(cell => new OrderedKeyValue(cell.asInstanceOf[KeyValue]))
      .toArray
  }

  def savePutAsHadoopHFile(rdd: RDD[Put], partitioner: Partitioner, job: Job): Unit = {
    val time = System.currentTimeMillis()
    rdd.filter(_ != null)
      .flatMap(putToKeyValue)
      .map((_, null))
      .repartitionAndSortWithinPartitions(partitioner)
      .map(x => (x._1.toBytesWritable, x._1.getKeyValue))
      .saveAsNewAPIHadoopDataset(job.getConfiguration)
    logger.info(s"Heartbeat - 保存HFile到HDFS结束,耗时：${System.currentTimeMillis() - time}ms")
  }

  def saveKeyValueAsHadoopHFile(rdd: RDD[KeyValue], partitioner: Partitioner, job: Job): Unit = {
    val time = System.currentTimeMillis()
    rdd.filter(_ != null)
      .map(kv => (new OrderedKeyValue(kv), null))
      .repartitionAndSortWithinPartitions(partitioner)
      .map(x => (x._1.toBytesWritable, x._1.getKeyValue))
      .saveAsNewAPIHadoopDataset(job.getConfiguration)
    logger.info(s"Heartbeat - 保存HFile到HDFS结束,耗时：${System.currentTimeMillis() - time}ms")
  }

  def saveOrderedKeyValueAsHadoopFile(rdd: RDD[OrderedKeyValue], partitioner: Partitioner, job: Job): Unit = {
    val time = System.currentTimeMillis()
    rdd.filter(_ != null)
      .map(kv => (kv, null))
      .repartitionAndSortWithinPartitions(partitioner)
      .map(x => (x._1.toBytesWritable, x._1.getKeyValue))
      .saveAsNewAPIHadoopDataset(job.getConfiguration)
    logger.info(s"Heartbeat - 保存Hfile到HDFS结束，耗时：${System.currentTimeMillis() - time}ms")
  }

  def hFilePartitioner(conf: Configuration, tableName: String, hBaseConn: Connection, maxFilePerRegionPerFamily: Int): Partitioner = {
    hFilePartitioner(
      conf,
      hBaseConn.getRegionLocator(TableName.valueOf(tableName)).getStartKeys,
      maxFilePerRegionPerFamily
    )
  }

  def hFilePartitioner(conf: Configuration, splitKeys: Array[Array[Byte]], maxFileRegionPerFamily: Int): Partitioner = {
    logger.info("Heartbeat -HBase table splitKeys:", splitKeys.map(Bytes.toString).mkString(","))
    if (maxFileRegionPerFamily == 1) {
      logger.info("Heartbeat - One HFile per region per family")
      new SingleHFilePartitioner(splitKeys)
    } else {
      val fraction = 1 max maxFileRegionPerFamily min conf.getInt(LoadIncrementalHFiles.MAX_FILES_PER_REGION_PER_FAMILY, 32)
      logger.info("Heartbeat =[{}] HFiles per region per family")
      new MultiHFilePartitioner(splitKeys,fraction)
    }
  }

  /**
    * 可排序的 KeyValue
    *
    * @param keyValue keyValue
    */
  class OrderedKeyValue(var keyValue: KeyValue) extends Serializable with Ordered[OrderedKeyValue] {
    def getKeyValue: KeyValue = {
      this.keyValue
    }

    def cloneRow: Array[Byte] = {
      CellUtil.cloneRow(getKeyValue)
    }

    def toBytesWritable: ImmutableBytesWritable = {
      new ImmutableBytesWritable(cloneRow)
    }

    override def compare(that: OrderedKeyValue): Int = {
      KeyValue.COMPARATOR.compare(this.getKeyValue, that.getKeyValue)
    }
  }

  /**
    * 每个分区一个HFile
    * @param splitKeys 分区数
    */
  protected class SingleHFilePartitioner(splitKeys: Array[Array[Byte]]) extends Partitioner {
    override def numPartitions: Int = splitKeys.length

    override def getPartition(key: Any): Int = {
      val rowKey = key.asInstanceOf[OrderedKeyValue].cloneRow
      for (i <- 1 until splitKeys.length) {
        if (Bytes.compareTo(rowKey, splitKeys(i)) < 0) {
          return i - 1
        }
      }
      splitKeys.length - 1
    }
  }

  /**
    * 每个分区多个HFile
    * @param splitKeys 分区数
    * @param fraction 分区数
    */
  protected class MultiHFilePartitioner(splitKeys: Array[Array[Byte]], fraction: Int) extends Partitioner {
    override def numPartitions: Int = splitKeys.length * fraction

    override def getPartition(key: Any): Int = {
      val rowKey = key.asInstanceOf[OrderedKeyValue].cloneRow
      val h = (rowKey.hashCode() & Int.MaxValue) % fraction * splitKeys.length
      for (i <- 1 until splitKeys.length) {
        if (Bytes.compareTo(rowKey, splitKeys(i)) < 0) {
          return i - 1 + h
        }
      }
      splitKeys.length - 1 + h
    }
  }

}
