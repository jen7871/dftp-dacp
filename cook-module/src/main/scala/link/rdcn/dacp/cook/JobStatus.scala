package link.rdcn.dacp.cook

import org.json.JSONObject

/**
 * @Author renhao
 * @Description:
 * @Data 2025/12/12 15:46
 * @Modified By:
 */
sealed trait JobStatus {
  def name: String
  def toJSON(): JSONObject = {
    new JSONObject().put("status", name)
  }
}


object JobStatus {
  case object RUNNING  extends JobStatus { val name = "RUNNING" }
  case object COMPLETE extends JobStatus { val name = "COMPLETE" }
  case class FAILED(cause: Throwable)  extends JobStatus {
    val name = "FAILED"

    override def toJSON(): JSONObject = {
      new JSONObject().put("status", name)
        .put("message", cause.getMessage)
    }
  }

  def fromJSON(json: JSONObject): JobStatus = {
    json.get("status") match {
      case "RUNNING" => RUNNING
      case "COMPLETE" => COMPLETE
      case "FAILED" => FAILED(new RuntimeException(json.getString("message")))
    }
  }
}

