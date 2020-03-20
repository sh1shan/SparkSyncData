package jrx.anytest.recommend.batch.monitor

import java.util.Date

class TaskMonitorInfo(tName: String, sTime: Date) extends Serializable {
  var id: Int = 0
  var taskName = tName
  var startTime = sTime
  var endTime = new Date()
  var totalCount = Integer.valueOf(0)
  var processCount = Integer.valueOf(0)
  var arguments = ""
  var info = ""

  override def toString: String = s"TaskMonitorInfo($id,$taskName,$startTime,$endTime,$totalCount,$processCount,$arguments,$info)"

}
