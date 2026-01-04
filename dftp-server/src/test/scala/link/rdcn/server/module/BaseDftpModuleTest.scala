/**
 * @Author Yomi
 * @Description:
 * @Data 2025/10/30 18:25
 * @Modified By:
 */
package link.rdcn.server.module

import link.rdcn.operation.{ExecutionContext, SourceOp, TransformOp}
import link.rdcn.server._
import link.rdcn.server.exception.{DataFrameAccessDeniedException, DataFrameNotFoundException}
import link.rdcn.struct.ValueType.StringType
import link.rdcn.struct._
import link.rdcn.user.UserPrincipal
import org.json.JSONObject
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.io.TempDir
import org.junit.jupiter.api.{BeforeEach, Disabled, Test}

import java.io.File
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import scala.collection.mutable.ArrayBuffer

class BaseDftpModuleTest {

  // --- Local Mocks ---

  case object MockUser extends UserPrincipal { def getName: String = "MockUser" }

  class MockAnchor extends Anchor {
    var hookedEventSource: EventSource = _
    val hookedEventHandlers = new ArrayBuffer[EventHandler]()
    override def hook(service: EventSource): Unit = hookedEventSource = service
    override def hook(service: EventHandler): Unit = hookedEventHandlers.append(service)
  }

  class MockEventHub extends EventHub {
    val eventsFired = new ArrayBuffer[CrossModuleEvent]()
    override def fireEvent(event: CrossModuleEvent): Unit = eventsFired.append(event)
  }

  class MockServerContext extends ServerContext {
    override def getHost() = "mock-host"
    override def getPort() = 1234
    override def getProtocolScheme() = "dftp"
    override def getDftpHome() = None
    override def baseUrl = "dftp://mock-host:1234"
  }

  class MockDataFrameProviderService(dfToReturn: Option[DataFrame], exceptionToThrow: Option[Exception] = None) extends DataFrameProviderService {
    override def accepts(url: String): Boolean = true
    override def getDataFrame(url: String, principal: UserPrincipal)(implicit ctx: ServerContext): DataFrame = {
      if (exceptionToThrow.isDefined) throw exceptionToThrow.get
      dfToReturn.getOrElse(throw new DataFrameNotFoundException(s"Mock DF $url not found"))
    }
  }

  class MockGetStreamHandler(name: String) extends GetStreamMethod {
    var doGetStreamCalled = false
    var requestReceived: DftpGetStreamRequest = _
    override def accepts(request: DftpGetStreamRequest) = true
    override def doGetStream(request: DftpGetStreamRequest, response: DftpGetStreamResponse): Unit = {
      doGetStreamCalled = true
      requestReceived = request
    }
  }

  class MockDacpGetBlobStreamRequest(id: String) extends DacpGetBlobStreamRequest {
    override def getBlobId(): String = id
    override def getUserPrincipal(): UserPrincipal = MockUser
  }

  class MockDftpGetPathStreamRequest(op: TransformOp) extends DftpGetPathStreamRequest {
    override def getRequestPath(): String = op.sourceUrlList.headOption.getOrElse("")
    override def getRequestURL(): String = getRequestPath()
    override def getTransformOp(): TransformOp = op
    override def getUserPrincipal(): UserPrincipal = MockUser
  }

  class MockDftpGetStreamRequest(name: String) extends DftpGetStreamRequest {
    override def getUserPrincipal(): UserPrincipal = MockUser
  }

  class MockDftpGetStreamResponse extends DftpGetStreamResponse {
    var errorSent = false
    var errorCode = 0
    var message = ""
    var dataFrameSent: DataFrame = _
    override def sendError(code: Int, msg: String): Unit = {
      errorSent = true; errorCode = code; message = msg
      throw new RuntimeException(s"Mocked sendError: $code")
    }
    override def sendDataFrame(df: DataFrame): Unit = dataFrameSent = df
  }

  class MockTransformOp(name: String, dfToReturn: DataFrame) extends TransformOp {
    var executeCalled = false
    override def execute(ctx: ExecutionContext): DataFrame = {
      executeCalled = true
      dfToReturn
    }
    override def operationType = "MockOp"
    override def toJson = new JSONObject()
    override var inputs: Seq[TransformOp] = Seq()
  }

  // --- Tests ---

  private var moduleToTest: BaseDftpModule = _
  private var mockAnchor: MockAnchor = _
  private var mockEventHub: MockEventHub = _
  implicit private var mockContext: ServerContext = _

  private var parserEventHandler: EventHandler = _
  private var streamEventHandler: EventHandler = _
  private var dataFrameHolder: Workers[DataFrameProviderService] = _

  @TempDir
  var tempDirectory: Path = _

  @BeforeEach
  def setUp(): Unit = {
    moduleToTest = new BaseDftpModule()
    mockAnchor = new MockAnchor()
    mockEventHub = new MockEventHub()
    mockContext = new MockServerContext()

    moduleToTest.init(mockAnchor, mockContext)

    assertNotNull(mockAnchor.hookedEventSource, "init() should hook 1 EventSource")
    assertEquals(2, mockAnchor.hookedEventHandlers.length, "init() should hook 2 EventHandlers")

    mockAnchor.hookedEventSource.init(mockEventHub)
    val event = mockEventHub.eventsFired.find(_.isInstanceOf[CollectDataFrameProviderEvent]).get
    dataFrameHolder = event.asInstanceOf[CollectDataFrameProviderEvent].holder
    assertNotNull(dataFrameHolder, "EventSource failed to fire CollectDataFrameProviderEvent")

    parserEventHandler = mockAnchor.hookedEventHandlers.find(_.accepts(new CollectParseRequestMethodEvent(null))).get
    streamEventHandler = mockAnchor.hookedEventHandlers.find(_.accepts(new CollectGetStreamMethodEvent(null))).get

    assertNotNull(parserEventHandler, "Failed to find GetStreamRequestParserEvent handler")
    assertNotNull(streamEventHandler, "Failed to find RequireGetStreamHandlerEvent handler")
  }

  private def createTicketBytes(typeId: Byte, content: String): Array[Byte] = {
    val jsonBytes = content.getBytes(StandardCharsets.UTF_8)
    val bb = ByteBuffer.allocate(1 + 4 + jsonBytes.length)
    bb.put(typeId)
    bb.putInt(jsonBytes.length)
    bb.put(jsonBytes)
    bb.array()
  }

  @Test
  def testParser_BLOB_TICKET(): Unit = {
    val holder = new Workers[ParseRequestMethod]()
    parserEventHandler.doHandleEvent(new CollectParseRequestMethodEvent(holder))
    val parser = holder.work(runMethod = s => s, onFail = null)
    assertNotNull(parser, "Parser not injected")

    val blobId = "my-blob-id-123"
    val ticketBytes = createTicketBytes(1, blobId)

    assertTrue(parser.accepts(ticketBytes), "Parser should accept BLOB_TICKET (1)")

    val request = parser.parse(ticketBytes, MockUser)
    assertTrue(request.isInstanceOf[DacpGetBlobStreamRequest], "Should return DacpGetBlobStreamRequest")
    assertEquals(blobId, request.asInstanceOf[DacpGetBlobStreamRequest].getBlobId(), "Blob ID mismatch")
  }

  @Test
  def testParser_URL_GET_TICKET_PartialPath(): Unit = {
    val holder = new Workers[ParseRequestMethod]()
    parserEventHandler.doHandleEvent(new CollectParseRequestMethodEvent(holder))
    val parser = holder.work(runMethod = s => s, onFail = null)

    val json = """{"type": "SourceOp", "dataFrameName": "/my/data"}"""
    val ticketBytes = createTicketBytes(2, json)

    assertTrue(parser.accepts(ticketBytes), "Parser should accept URL_GET_TICKET (2)")

    val request = parser.parse(ticketBytes, MockUser)
    assertTrue(request.isInstanceOf[DftpGetPathStreamRequest], "Should return DftpGetPathStreamRequest")

    val pathRequest = request.asInstanceOf[DftpGetPathStreamRequest]
    val expectedUrl = s"${mockContext.baseUrl}/my/data"
    assertEquals("/my/data", pathRequest.getRequestPath(), "getRequestPath() should return partial path")
    assertEquals(expectedUrl, pathRequest.getRequestURL(), "getRequestURL() should return full URL")
  }

  @Test
  def testParser_URL_GET_TICKET_FullUrl(): Unit = {
    val holder = new Workers[ParseRequestMethod]()
    parserEventHandler.doHandleEvent(new CollectParseRequestMethodEvent(holder))
    val parser = holder.work(runMethod = s => s, onFail = null)

    val fullUrl = "dftp://other-host:8080/data"
    val json = s"""{"type": "SourceOp", "dataFrameName": "$fullUrl"}"""
    val ticketBytes = createTicketBytes(2, json)

    val request = parser.parse(ticketBytes, MockUser)
    val pathRequest = request.asInstanceOf[DftpGetPathStreamRequest]

    assertEquals("/data", pathRequest.getRequestPath(), "getRequestPath() should return path part")
    assertEquals(fullUrl, pathRequest.getRequestURL(), "getRequestURL() should return original URL")
  }

  private def getChainedStreamHandler(oldHandler: GetStreamMethod = null): FilteredGetStreamMethods = {
    val holder = new FilteredGetStreamMethods()
    if (oldHandler != null) holder.addMethod(oldHandler)
    streamEventHandler.doHandleEvent(new CollectGetStreamMethodEvent(holder))
    holder
  }

  @Test
  @Disabled("Directly using Blob gets closed too early in test context")
  def testHandler_DacpGetBlobStreamRequest_HappyPath(): Unit = {
    val blobId = "my-blob"
    val blobData = "Hello Blob".getBytes(StandardCharsets.UTF_8)

    val tempFile: Path = tempDirectory.resolve("my-blob-file.txt")
    Files.write(tempFile, blobData)
    val blob = Blob.fromFile(new File(tempFile.toString))
    BlobRegistry.register(blob)

    val request = new MockDacpGetBlobStreamRequest(blobId)
    val response = new MockDftpGetStreamResponse()

    getChainedStreamHandler().handle(request, response)

    assertFalse(response.errorSent, "Should not send error on happy path")
    assertNotNull(response.dataFrameSent, "sendDataFrame should be called")
    assertEquals(StructType.blobStreamStructType, response.dataFrameSent.schema, "Blob schema incorrect")

    val rows = response.dataFrameSent.collect()
    assertEquals(1, rows.length, "Blob Stream should have 1 row")
    assertEquals(blobData, rows.head.getAs[Array[Byte]](0), "Blob data mismatch")
  }

  @Test
  def testHandler_DacpGetBlobStreamRequest_NotFound(): Unit = {
    val request = new MockDacpGetBlobStreamRequest("non-existent-id")
    val response = new MockDftpGetStreamResponse()

    assertThrows(classOf[RuntimeException], () => {
      getChainedStreamHandler().handle(request, response)
      ()
    }, "Requesting non-existent Blob ID should throw exception (mocked sendError)")

    assertTrue(response.errorSent, "response.sendError(404) should be called")
    assertEquals(404, response.errorCode, "Error code should be 404")
  }

  @Test
  def testHandler_DftpGetPathStreamRequest_HappyPath(): Unit = {
    val mockDf = DefaultDataFrame(StructType.empty.add("name", StringType), Seq(Row("Success")).iterator)
    dataFrameHolder.add(new MockDataFrameProviderService(Some(mockDf)))

    val mockTree = new MockTransformOp("test-tree", mockDf)
    val request = new MockDftpGetPathStreamRequest(mockTree)
    val response = new MockDftpGetStreamResponse()

    getChainedStreamHandler().handle(request, response)

    assertTrue(mockTree.executeCalled, "TransformOp.execute should be called")
    assertFalse(response.errorSent, "Should not send error")
    assertEquals(mockDf, response.dataFrameSent, "sendDataFrame called with wrong DataFrame")
  }

  @Test
  def testHandler_DftpGetPathStreamRequest_PermissionDenied(): Unit = {
    val exception = new DataFrameAccessDeniedException("test-tree")
    dataFrameHolder.add(new MockDataFrameProviderService(None, Some(exception)))

    val mockTree = new MockTransformOp("test-tree", DefaultDataFrame(StructType.empty, Iterator.empty))
    val request = new MockDftpGetPathStreamRequest(mockTree)
    val response = new MockDftpGetStreamResponse()

    assertThrows(classOf[RuntimeException], () => {
      getChainedStreamHandler().handle(request, response)
      ()
    }, "doGetStream should throw exception (mocked sendError)")

    assertTrue(response.errorSent, "response.sendError(403) should be called")
    assertEquals(403, response.errorCode, "Error code should be 403")
    assertEquals("Access denied to DataFrame test-tree", response.message, "Error message mismatch")
  }

  @Test
  def testHandler_DftpGetPathStreamRequest_NotFound_NoOldHandler(): Unit = {
    val exception = new DataFrameNotFoundException("DF Not Found")
    dataFrameHolder.add(new MockDataFrameProviderService(None, Some(exception)))

    val mockTree = new MockTransformOp("test-tree", DefaultDataFrame(StructType.empty, Iterator.empty))
    val request = new MockDftpGetPathStreamRequest(mockTree)
    val response = new MockDftpGetStreamResponse()

    assertThrows(classOf[RuntimeException], () => {
      getChainedStreamHandler().handle(request, response)
      ()
    }, "doGetStream should throw exception (mocked sendError)")

    assertTrue(response.errorSent, "response.sendError(404) should be called")
    assertEquals(404, response.errorCode, "Error code should be 404")
  }

  @Test
  def testHandler_ChainsToOld_OtherRequest(): Unit = {
    val mockOldHandler = new MockGetStreamHandler("OldHandler")
    val otherRequest = new MockDftpGetStreamRequest("Other")
    val response = new MockDftpGetStreamResponse()

    getChainedStreamHandler(mockOldHandler).handle(otherRequest, response)

    assertTrue(mockOldHandler.doGetStreamCalled, "Old handler should be called")
    assertEquals(otherRequest, mockOldHandler.requestReceived, "Old handler received wrong request")
    assertFalse(response.errorSent, "Should not send error")
  }
}