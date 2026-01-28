package link.rdcn

import org.apache.logging.log4j.{LogManager, Logger}
import org.json.JSONObject


/**
 * @Author renhao
 * @Description:
 * @Data 2025/9/20 23:28
 * @Modified By:
 */
trait Logging {
  protected lazy val logger: Logger = LogManager.getLogger(getClass)

  private lazy val flowUnderlying: Logger = LogManager.getLogger("FLOW_LOGGER")

  protected def flowLogger(jobId: String, operatorId: String = ""): JobFlowLogger =
    new JobFlowLogger(jobId, operatorId, flowUnderlying)
}

final class JobFlowLogger(
                           jobId: String,
                           operatorId: String,
                           underlying: Logger
                         ) {

  private def log(level: String, message: String): Unit = {
    val json = new JSONObject()
      .put("jobId", jobId)
      .put("operatorId", operatorId)
      .put("level", level)
      .put("message", message)

    underlying.info(json.toString)
  }

  def info(message: String): Unit =
    log("INFO", message)

  def debug(message: String): Unit =
    log("DEBUG", message)

  def error(message: String): Unit =
    log("ERROR", message)
}

