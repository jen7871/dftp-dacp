/**
 * @Author Yomi
 * @Description:
 * @Data 2025/11/6 15:10
 * @Modified By:
 */
package link.rdcn.cook

import link.rdcn.dacp.cook.{DacpCookModule, DacpCookStreamRequest}
import link.rdcn.dacp.user.{DataOperationType, PermissionService, RequirePermissionServiceEvent}
import link.rdcn.operation.{ExecutionContext, SourceOp, TransformOp}
import link.rdcn.server._
import link.rdcn.server.exception.DataFrameNotFoundException
import link.rdcn.server.module._
import link.rdcn.struct.ValueType.StringType
import link.rdcn.struct.{DataFrame, DefaultDataFrame, Row, StructType}
import link.rdcn.user.{Credentials, UserPrincipal}
import org.json.JSONObject
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{BeforeEach, Test}

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import scala.collection.mutable.ArrayBuffer

class DacpCookModuleTest {

  // --- Local Mocks ---

  case object MockUser extends UserPrincipal { def getName: String = "MockUser" }

  class MockAnchor extends Anchor {
    var hookedEventSource: EventSource = _
    val hookedEventHandlers = new ArrayBuffer[EventHandler]()
    override def hook(service: EventSource): Unit = hookedEventSource = service
    override def hook(service: EventHandler): Unit = hookedEventHandlers.append(service)
  }

  class MockEventHub(handlers: Seq[EventHandler]) extends EventHub {
    val eventsFired = new ArrayBuffer[CrossModuleEvent]()
    override def fireEvent(event: CrossModuleEvent): Unit = {
      handlers.filter(_.accepts(event)).foreach(_.doHandleEvent(event))
      eventsFired.append(event)
    }
  }

  class MockServerContext extends ServerContext {
    override def getHost() = "mock"
    override def getPort() = 0
    override def getProtocolScheme() = "dftp"
    override def getDftpHome() = None
  }

  class MockGetStreamRequestParser extends ParseRequestMethod {
    var parseCalled = false
    override def accepts(token: Array[Byte]) = true
    override def parse(token: Array[Byte], p: UserPrincipal) = {
      parseCalled = true
      new MockDftpGetStreamRequest("OldRequest")
    }
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

  private var moduleToTest: DacpCookModule = _
  private var mockAnchor: MockAnchor = _
  private var mockEventHub: MockEventHub = _
  implicit private var mockContext: ServerContext = _
  private var getStreamParserHolder: Workers[ParseRequestMethod] = _

  @BeforeEach
  def setUp(): Unit = {
    moduleToTest = new DacpCookModule()
    mockAnchor = new MockAnchor()
    mockContext = new MockServerContext()
    moduleToTest.init(mockAnchor, mockContext)

    mockEventHub = new MockEventHub(mockAnchor.hookedEventHandlers)
    mockAnchor.hookedEventSource.init(mockEventHub)

    getStreamParserHolder = new Workers[ParseRequestMethod]
    mockEventHub.fireEvent(new CollectParseRequestMethodEvent(getStreamParserHolder))
  }

  @Test
  def testInit_FiresAndHooksEvents(): Unit = {
    assertEquals(4, mockEventHub.eventsFired.length, "EventSource.init() should fire 4 events")
    assertTrue(mockEventHub.eventsFired.exists(_.isInstanceOf[CollectDataFrameProviderEvent]), "CollectDataFrameProviderEvent missing")
  }

  @Test
  def testParser_ParsesCookTicket(): Unit = {
    val chainedParser = getStreamParserHolder.work(runMethod = s => s, onFail = null)

    val COOK_TICKET: Byte = 3
    val json = """{"type": "SourceOp", "dataFrameName": "/my/data"}"""
    val jsonBytes = json.getBytes(StandardCharsets.UTF_8)
    val bb = ByteBuffer.allocate(1 + 4 + jsonBytes.length)
    bb.put(COOK_TICKET).putInt(jsonBytes.length).put(jsonBytes)

    val request = chainedParser.parse(bb.array(), MockUser)

    assertTrue(request.isInstanceOf[DacpCookStreamRequest], "Should return DacpCookStreamRequest")
    assertEquals("/my/data", request.asInstanceOf[DacpCookStreamRequest].getTransformTree.asInstanceOf[SourceOp].dataFrameUrl, "Tree URL mismatch")
  }

  @Test
  def testParser_ChainsToOld(): Unit = {
    val mockOldParser = new MockGetStreamRequestParser()
    getStreamParserHolder.add(mockOldParser)

    val chainedParser = getStreamParserHolder.work(runMethod = s => s, onFail = null)
    val request = chainedParser.parse(Array[Byte](1, 0, 0, 0, 0), MockUser)

    assertTrue(mockOldParser.parseCalled, "Old parser should be called")
  }
}