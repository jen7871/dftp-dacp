package link.rdcn.server

import link.rdcn.struct.{Blob, DataFrame}
import link.rdcn.user.UserPrincipal
import org.json.JSONObject
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

class DftpMessagesTest {

  // --- Local Mocks ---

  // Mock Request implementor
  class MockActionRequest(params: JSONObject) extends DftpActionRequest {
    override def getActionName(): String = "mockAction"
    override def getRequestParameters(): JSONObject = params
    override def getUserPrincipal(): UserPrincipal = null
  }

  // Mock Response implementor with default method support
  class MockActionResponse extends DftpActionResponse {
    var sentJsonString: String = _
    var sentCode: Int = -1

    override def sendJSONString(json: String, code: Int): Unit = {
      sentJsonString = json
      sentCode = code
    }

    // Other methods mocked as empty
    override def attachStream(dataFrameResponse: DataFrameResponse): Unit = {}
    override def attachStream(blobResponse: BlobResponse): Unit = {}
    override def sendPutDataFrameParameters(json: JSONObject, code: Int): Unit = {}
    override def sendPutBlobParameters(json: JSONObject, code: Int): Unit = {}
    override def sendError(errorCode: Int, message: String): Unit = {}
  }

  // --- Tests ---

  @Test
  def testDftpRequest_Attributes(): Unit = {
    // DftpRequest trait has a mutable map 'attributes' by default
    val request = new MockActionRequest(new JSONObject())
    request.attributes.put("key", "value")

    assertEquals("value", request.attributes("key"), "Should be able to store and retrieve attributes")
  }

  @Test
  def testDftpActionResponse_SendJsonObject(): Unit = {
    val response = new MockActionResponse()
    val json = new JSONObject()
    json.put("status", "success")
    json.put("id", 123)

    // Call the default method sendJsonObject
    response.sendJSONObject(json, 202)

    // Verify it delegated to sendJsonString
    assertNotNull(response.sentJsonString, "sendJsonObject should delegate to sendJsonString")
    assertEquals(202, response.sentCode, "Status code should be passed through")

    val receivedJson = new JSONObject(response.sentJsonString)
    assertEquals("success", receivedJson.getString("status"), "JSON content should match")
    assertEquals(123, receivedJson.getInt("id"), "JSON content should match")
  }
}