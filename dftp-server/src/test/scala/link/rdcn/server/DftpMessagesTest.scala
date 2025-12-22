package link.rdcn.server

import link.rdcn.message.MapSerializer
import link.rdcn.operation.TransformOp
import link.rdcn.struct.DataFrame
import link.rdcn.user.UserPrincipal
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

class DftpMessagesTest {

  // --- Local Mocks to test Trait default methods ---

  class MockActionRequest(param: Array[Byte]) extends DftpActionRequest {
    override def getActionName(): String = "testAction"
    override def getParameter(): Array[Byte] = param
    override def getUserPrincipal(): UserPrincipal = null
  }

  class MockPlainResponse extends DftpPlainResponse {
    var sentData: Array[Byte] = _
    override def sendData(data: Array[Byte]): Unit = sentData = data
    override def sendError(errorCode: Int, message: String): Unit = {}
  }

  class MockPutStreamRequest(df: DataFrame) extends DftpPutStreamRequest {
    override def getDataFrame(): DataFrame = df
    override def getUserPrincipal(): UserPrincipal = null
  }

  // --- Tests ---

  @Test
  def testDftpActionRequest_GetParameterAsMap(): Unit = {
    val originalMap = Map("key1" -> "value1", "key2" -> 123)
    val encodedBytes = MapSerializer.encodeMap(originalMap)
    val request = new MockActionRequest(encodedBytes)

    val decodedMap = request.getParameterAsMap()

    assertEquals("value1", decodedMap("key1"), "Should decode String value correctly")
    assertEquals(123, decodedMap("key2"), "Should decode Int value correctly")
  }

  @Test
  def testDftpPlainResponse_SendDataMap(): Unit = {
    val response = new MockPlainResponse()
    val mapToSend = Map("status" -> "ok")

    // Call default method
    response.sendData(mapToSend)

    assertNotNull(response.sentData, "sendData(Map) should invoke sendData(Array[Byte])")

    // Verify encoding
    val decoded = MapSerializer.decodeMap(response.sentData)
    assertEquals("ok", decoded("status"), "Encoded data should match original map")
  }
}