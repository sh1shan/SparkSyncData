package jrx.anytest.recommend.batch.utils

import java.util.Properties

object ApplicationTools {
  val props = new Properties()
  val propsMonitor =new Properties()
  var environment:String ="dev"

  /**
    * 获取配置文件的值
    * @param key key
    * @return value
    */
  def getValue(key:String): String ={
    props.getProperty(key)
  }

}
