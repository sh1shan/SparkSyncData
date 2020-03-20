package jrx.anytest.recommend.batch.task

import jrx.anytest.recommend.batch.utils.ApplicationTools
import org.apache.hadoop.hbase.util.Bytes
import org.slf4j.{Logger, LoggerFactory}

class MaLabel2HBaseBulkLoad(appName: String, master: String) extends BaseSparkTask(appName, master) {
  val logger: Logger = LoggerFactory.getLogger(MaLabel2HBaseBulkLoad.getClass)
  val FAMILY: Array[Byte] = Bytes.toBytes("inf")
  var colArray: Array[String] = new Array[String](0)

  for (i <- colArray.indices) {
    logger.info(s"第${i + 1}列位：${colArray(i)}")
  }
  val TASK_NAME = "TASK NAME [sync Ma 标签表到HBase表: MaLabel]"
  //脚本参数
  var env: String = _
  var filePath: String = _
  var region: Int = _
  var currTaskDate: String = _
  var maxFilePerRegionPerFamily: Int = _
  //配置参数
  var tableName: String = _
  var bulkPath: String = _

  def initArgs(args: Array[String]): Unit = {
    env = args(0)
    filePath = args(1)
    currTaskDate = args(2)
    region = args(3).toInt
    maxFilePerRegionPerFamily = args(4).toInt
    ApplicationTools.loadConfig(env)
    logger.info(s"maLabelField:${ApplicationTools.maLabelField1()}")
    colArray = ApplicationTools.maLabelField1().split(",", -1)
    tableName=ApplicationTools.maLabel()
    bulkPath=s"/user/ssyx/MaLabel2HBaseBulkLoad/$currTaskDate"
    logger.info(s"本次任务日期：$currTaskDate，环境：$env")
    logger.info(s"当前代加工的表：$tableName¬")

  }

  override def runTask(args: Array[String]): Unit = {
    logger.info(s"begin -->$TASK_NAME")
    val taskStartTime = System.currentTimeMillis()
    initArgs(args)

  }

}

object MaLabel2HBaseBulkLoad {
  def main(args: Array[String]): Unit = {
    new MaLabel2HBaseBulkLoad(this.getClass.getSimpleName, null).run(args)
  }
}
