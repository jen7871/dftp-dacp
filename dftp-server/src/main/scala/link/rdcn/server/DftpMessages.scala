package link.rdcn.server

import link.rdcn.struct.{Blob, DataFrame, DataFrameMetaData}
import link.rdcn.user.UserPrincipal
import org.json.JSONObject

import scala.collection.mutable

/**
 * @Author renhao
 * @Description:
 * @Date 2025/8/27 09:43
 * @Modified By:
 */
trait DftpRequest {
  val attributes = mutable.Map[String, Any]()

  def getUserPrincipal(): UserPrincipal
}

trait DftpActionRequest extends DftpRequest {
  def getActionName(): String
  val requestParameters: JSONObject
}

trait DftpPutStreamRequest extends DftpRequest {
  def getDataFrame(): DataFrame
}

trait DftpResponse {
  def sendError(errorCode: Int, message: String): Unit
}

trait DftpActionResponse extends DftpResponse {
  def sendRedirect(dataFrameResponse: DataFrameResponse)
  def sendRedirect(blobResponse: BlobResponse)
  def sendJsonString(json: String, code: Int = 200)
  def sendJsonObject(json: JSONObject, code: Int = 200) = sendJsonString(json.toString, code)
}

trait DftpPlainResponse extends DftpResponse {
  def sendData(data: Array[Byte])
}

trait DftpPutStreamResponse extends DftpPlainResponse

trait DataFrameResponse {
  def getDataFrameMetaData: DataFrameMetaData
  def getDataFrame: DataFrame
}

trait BlobResponse {
  def getBlob: Blob
}