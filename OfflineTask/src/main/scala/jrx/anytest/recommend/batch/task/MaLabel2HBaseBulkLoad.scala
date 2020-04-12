package jrx.anytest.recommend.batch.task

import jrx.anytest.recommend.batch.utils.{ApplicationTools, HBaseUtils}
import jrx.anytest.recommend.batch.utils.HBaseUtils.OrderedKeyValue
import org.apache.commons.lang.StringUtils
import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.TextInputFormat
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext, TaskContext}
import org.slf4j.{Logger, LoggerFactory}

class MaLabel2HBaseBulkLoad(appName: String, master: String) extends BaseSparkTask(appName, master) {
  val logger: Logger = LoggerFactory.getLogger(MaLabel2HBaseBulkLoad.getClass)
  val FAMILY: Array[Byte] = Bytes.toBytes("inf")
  var colArray: Array[String] = new Array[String](0)

  for (i <- colArray.indices) {
    logger.info(s"第${i + 1}列位：${colArray(i)}")
  }
  val TASK_NAME = "TASK NAME [sync Ma标签表到HBase表: MaLabel]"
  //脚本参数
  var env: String = _
  var filePath: String = _
  var region: Int = _
  var currTaskDate: String = _
  var maxFilePerRegionPerFamily: Int = _
  //配置参数
  var tableName: String = _
  var bulkPath: String = _

  /**
    * 初始化执行参数列表
    * Spark任务配置，任务执行参数，都在这里初始化
    * @param args 参数列表
    */
  def initArgs(args: Array[String]): Unit = {
    env = args(0)
    filePath = args(1)
    currTaskDate = args(2)
    region = args(3).toInt
    maxFilePerRegionPerFamily = args(4).toInt
    ApplicationTools.loadConfig(env)
    logger.info(s"maLabelField:${ApplicationTools.maLabelField1()}")
    colArray = ApplicationTools.maLabelField1().split(",", -1)
    tableName = ApplicationTools.maLabel()
    bulkPath = s"/user/ssyx/MaLabel2HBaseBulkLoad/$currTaskDate"
    logger.info(s"本次任务日期：$currTaskDate，环境：$env")
    logger.info(s"当前代加工的表：$tableName¬")
  }

  /**
    * HBaseFileRDD 创建过程  BuildRdd -> HadoopFile->HBase
    * @param filePath HadoopFile  存放路径
    * @param sparkContext spark对象
    * @return HBaseRDD
    */
  def bulkRdd(filePath: String, sparkContext: SparkContext): RDD[Array[String]] = {
    val rdd = sparkContext.hadoopFile(filePath, classOf[TextInputFormat], classOf[LongWritable], classOf[Text])
      .map(a => new String(a._2.getBytes, 0, a._2.getLength, "utf-8"))
      .map(_.split("\033", -1))
      .filter(row => null != row
        && StringUtils.isBlank(row(0))
        && !row(0).equals("\\N")
        && row(0).length >= 4
        && row(0).trim.startsWith("ECFCU")
        && row.length > colArray.length
      )
    rdd
  }

  /**
    * 每4000行数据打印一次日志
    * @param count 循环次数
    * @return
    */
  def isLoggerEnable(count: Long): Boolean = {
    (count % 4000) == 1
  }

  /**
    * 迭代遍历
    *
    * @param it 迭代器
    * @return
    */
  def lineIterator(it: Iterator[Array[String]]): Iterator[Put] = {
    var counter: Long = 0L
    var counter_success: Long = 0L
    it.map(row => {
      counter += 1
      if (counter == 1) {
        logger.info(s"Heartbeat-partitionID [${TaskContext.get.partitionId}]")
      }
      if (isLoggerEnable(counter)) {
        logger.info(s"Heartbeat --开始处理第${counter}条，[$row]")
      }
      var put: Put = null
      val ecif = row(0).trim
      if (StringUtils.isBlank(ecif) || ecif.length < 4) {
        logger.warn("ecif为空或者长度小于4，跳过！")
      } else {
        val rowkey = HBaseUtils.reverse(ecif, 4)
        put = new Put(Bytes.toBytes(rowkey))
        for (i <- row.indices) {
          if (StringUtils.isBlank(row(i)) || "\\N".equals(row(i))) {
            row(i) = ""
          }
        }
        for (i <- 1 until colArray.length) {
          put.addColumn(FAMILY, Bytes.toBytes(colArray(i)), Bytes.toBytes(row(i)))
        }
        counter_success += 1
      }
      if (it.isEmpty) {
        logger.info(s"HeartBeat - mapPartitions[${TaskContext.get.partitionId}]处理完成，共${counter}条，" + s"成功处理[$counter_success]")
      }
      put
    })

  }

  /**
    * 任务主要处理过程
    * @param args 参数列表
    */
  override def runTask(args: Array[String]): Unit = {
    logger.info(s"begin -->$TASK_NAME")
    val taskStartTime = System.currentTimeMillis()
    initArgs(args)
    val sparkConf: SparkConf = new SparkConf()
      .setAppName(s"jrx.task.$appName-$currTaskDate")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .registerKryoClasses(Array(classOf[ImmutableBytesWritable], classOf[OrderedKeyValue], classOf[KeyValue]))
    val sc: SparkContext = new SparkContext(sparkConf)
    val hBaseConn = HBaseUtils.initHBase()
    HBaseUtils.createTableIfNotExist(tableName, FAMILY, HBaseUtils.splitRegion(region, 4), hBaseConn)
    HBaseUtils.delHadoopFile(sc, bulkPath)
    val job = HBaseUtils.confBulkJob(tableName, bulkPath, hBaseConn)
    val partitioner = HBaseUtils.hFilePartitioner(job.getConfiguration, tableName, hBaseConn, maxFilePerRegionPerFamily)

    //构造rdd RDD[row]
    logger.info(s"begin-->build rdd from:$filePath")
    val rdd0 = bulkRdd(filePath, sc)
    logger.info(s"rdd0.count:::${rdd0.count()}")
    logger.info(s"begin --> lineIterator")
    val rdd: RDD[Put] = rdd0.mapPartitions(lineIterator)
    logger.info(s"rdd.count:::${rdd.count()}")

    HBaseUtils.savePutAsHadoopHFile(rdd, partitioner, job)
    HBaseUtils.doBulkLoad(tableName, bulkPath, hBaseConn)
    HBaseUtils.delHadoopFile(sc, bulkPath)
    HBaseUtils.close(hBaseConn)

    logger.info(s"$currTaskDate 同步Ma标签表到HBase表：$tableName 完成！")
    logger.info(s"spark 任务结束，总耗时：${System.currentTimeMillis() - taskStartTime}ms")
    sc.stop()
  }

}

object MaLabel2HBaseBulkLoad {
  def main(args: Array[String]){
    new MaLabel2HBaseBulkLoad(this.getClass.getSimpleName, null).run(args)
  }
}
