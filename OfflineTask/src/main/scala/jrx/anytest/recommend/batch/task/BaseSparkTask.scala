package jrx.anytest.recommend.batch.task

import java.util.Date

import jrx.anytest.recommend.batch.monitor.TaskMonitorInfo
import jrx.anytest.recommend.batch.utils.FileLogger
import org.apache.commons.cli.{CommandLine, CommandLineParser, DefaultParser, Options}

abstract class BaseSparkTask(appName: String, master: String) extends Serializable {
  val appNames = appName
  val masters = master
  val taskMonitor = new TaskMonitorInfo(appNames, new Date())

  def runTask(args: Array[String]): Unit

  def run(args: Array[String]) = {
    FileLogger.log("start spark task:" + appName + ",args:" + args.toString)
    taskMonitor.arguments = args.mkString
    this.runTask(args: Array[String])

    taskMonitor.endTime = new Date()

    try {

    } catch {
      case e: Exception => {
        FileLogger.error("save taskInfo failed ,msg=" + taskMonitor.toString)
      }
    }
  }

  def buildArgParser(args: Array[String]): CommandLine = {
    val parser: CommandLineParser = new DefaultParser
    val options: Options = new Options

    options.addOption("v", "env", true, "环境变量：dev,prd,test")
    options.addOption("x", "max", true, "最大数据量id")
    options.addOption("m", "min", true, "最小数据量id")
    options.addOption("p", "part", true, "分区数")
    options.addOption("f", "ntfs", true, "ntfs文件目录")
    options.addOption("l", "local", false, "local master")
    options.addOption("a", "all", false, "是否全量数据")
    return parser.parse(options, args)
  }

}
