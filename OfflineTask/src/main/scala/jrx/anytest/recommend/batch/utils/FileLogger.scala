package jrx.anytest.recommend.batch.utils

import org.slf4j.LoggerFactory

object FileLogger {
  val logger = LoggerFactory.getLogger("file_log")

  def log(msg: String): Unit = {
    logger.info(msg)
  }

  def info(msg: String): Unit = {
    logger.info(msg)
  }

  def debug(msg: String): Unit = {
    logger.debug(msg)
  }

  def error(msg: String): Unit = {
    logger.error(msg)
  }

  def warn(msg: String): Unit = {
    logger.warn(msg)
  }

}
