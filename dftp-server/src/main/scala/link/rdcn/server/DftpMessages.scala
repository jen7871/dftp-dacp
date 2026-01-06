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

trait DftpGetStreamRequest extends DftpRequest {
  def getRequestPath(): String
  def getRequestURL(): String
}

trait DftpActionRequest extends DftpRequest {
  def getActionName(): String
  def getRequestParameters(): JSONObject
}

trait DftpPutStreamRequest extends DftpRequest {
  def getRequestParameters(): JSONObject
}

trait DftpPutDataFrameRequest extends DftpPutStreamRequest {
  def getDataFrame(): DataFrame
}

trait DftpPutBlobRequest extends DftpPutStreamRequest {
  def getBlob(): Blob
}

trait DftpResponse {
  def sendError(errorCode: Int, message: String): Unit
}

trait DftpGetStreamResponse extends DftpResponse {
  def sendDataFrame(dataFrame: DataFrame)
  def sendBlob(blob: Blob)
}

trait DftpActionResponse extends DftpResponse {
  def attachStream(dataFrameResponse: DataFrameResponse)
  def attachStream(blobResponse: BlobResponse)
  def sendPutDataFrameParameters(json: JSONObject, code: Int = 200)
  def sendPutBlobParameters(json: JSONObject, code: Int = 200)
  def sendJsonString(json: String, code: Int = 200)
  def sendJsonObject(json: JSONObject, code: Int = 200) = sendJsonString(json.toString, code)
}

trait DftpPutStreamResponse extends DftpResponse {
  def onNext(json: String)
  def onCompleted()
}

trait DataFrameResponse {
  def getDataFrameMetaData: DataFrameMetaData
  def getDataFrame: DataFrame
}

trait BlobResponse {
  def getBlob: Blob
}