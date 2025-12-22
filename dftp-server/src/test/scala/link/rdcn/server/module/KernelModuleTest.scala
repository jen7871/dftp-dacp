/**
 * @Author Yomi
 * @Description:
 * @Data 2025/11/6 18:28
 * @Modified By:
 */
package link.rdcn.server.module

import link.rdcn.server._
import link.rdcn.struct.DataFrame
import link.rdcn.user.{AuthenticationMethod, Credentials, UserPasswordAuthService, UserPrincipal, UserPrincipalWithCredentials, UsernamePassword}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{BeforeEach, Test}

import scala.collection.mutable.ArrayBuffer

class KernelModuleTest {

  // --- Local Mocks ---

  class MockEventHub extends EventHub {
    val eventsFired = new ArrayBuffer[CrossModuleEvent]()
    override def fireEvent(event: CrossModuleEvent): Unit = eventsFired.append(event)
  }

  class MockAnchor extends Anchor {
    var hookedEventSource: EventSource = _
    override def hook(service: EventSource): Unit = this.hookedEventSource = service
    override def hook(service: EventHandler): Unit = {}
  }

  class MockServerContext extends ServerContext {
    override def getHost() = "mock-host"
    override def getPort() = 1234
    override def getProtocolScheme() = "dftp"
    override def getDftpHome() = None
  }

  class MockActionHandler extends ActionMethod {
    var doActionCalled = false
    var requestCalledWith: DftpActionRequest = _
    override def accepts(request: DftpActionRequest) = true
    override def doAction(request: DftpActionRequest, response: DftpActionResponse): Unit = {
      doActionCalled = true
      requestCalledWith = request
    }
  }

  class MockGetStreamHandler extends GetStreamMethod {
    var doGetStreamCalled = false
    override def accepts(request: DftpGetStreamRequest) = true
    override def doGetStream(request: DftpGetStreamRequest, response: DftpGetStreamResponse): Unit = doGetStreamCalled = true
  }

  class MockPutStreamHandler extends PutStreamMethod {
    var doPutStreamCalled = false
    override def accepts(request: DftpPutStreamRequest) = true
    override def doPutStream(request: DftpPutStreamRequest, response: DftpPutStreamResponse): Unit = doPutStreamCalled = true
  }

  class MockGetStreamRequestParser extends ParseRequestMethod {
    var parseCalled = false
    val requestToReturn: DftpGetStreamRequest = new MockDftpGetStreamRequest()
    override def accepts(token: Array[Byte]) = true
    override def parse(token: Array[Byte], principal: UserPrincipal): DftpGetStreamRequest = {
      parseCalled = true
      requestToReturn
    }
  }

  class MockAuthenticationService extends UserPasswordAuthService {
    var authenticateCalled = false
    val userToReturn: UserPrincipal = MockUserPrincipal("MockAuthUser")
    override def accepts(credentials: Credentials) = true
    override def authenticate(credentials: Credentials): UserPrincipal = {
      authenticateCalled = true
      userToReturn
    }
  }

  object MockCredentials extends UsernamePassword("MockUser", "MockPass")
  case class MockUserPrincipal(name: String) extends UserPrincipal { def getName = name }

  class MockDftpActionResponse extends DftpActionResponse {
    var errorSent = false; var errorCode = 0; var message = ""
    override def sendError(code: Int, msg: String): Unit = { errorSent = true; errorCode = code; message = msg }
    override def sendData(data: Array[Byte]): Unit = {}
  }

  class MockDftpGetStreamResponse extends DftpGetStreamResponse {
    var errorSent = false; var errorCode = 0
    override def sendError(code: Int, msg: String): Unit = { errorSent = true; errorCode = code }
    override def sendDataFrame(dataFrame: DataFrame): Unit = {}
  }

  class MockDftpPutStreamResponse extends DftpPutStreamResponse {
    var errorSent = false; var errorCode = 0
    override def sendError(code: Int, msg: String): Unit = { errorSent = true; errorCode = code }
    override def sendData(data: Array[Byte]): Unit = {}
  }

  class MockDftpActionRequest(action: String = "test") extends DftpActionRequest {
    override def getActionName(): String = action
    override def getParameter(): Array[Byte] = Array.empty
    override def getUserPrincipal(): UserPrincipal = null
  }

  class MockDftpGetStreamRequest extends DftpGetStreamRequest {
    override def getUserPrincipal(): UserPrincipal = null
  }

  class MockDftpPutStreamRequest extends DftpPutStreamRequest {
    override def getDataFrame(): DataFrame = null
    override def getUserPrincipal(): UserPrincipal = null
  }


  // --- Tests ---

  private var kernelModule: KernelModule = _
  private var mockEventHub: MockEventHub = _

  private var authHolder: Workers[AuthenticationMethod] = _
  private var parseHolder: Workers[ParseRequestMethod] = _
  private var getHolder: FilteredGetStreamMethods = _
  private var actionHolder: Workers[ActionMethod] = _
  private var putHolder: Workers[PutStreamMethod] = _

  @BeforeEach
  def setUp(): Unit = {
    kernelModule = new KernelModule()
    val mockAnchor = new MockAnchor()
    mockEventHub = new MockEventHub()
    val mockContext = new MockServerContext()

    kernelModule.init(mockAnchor, mockContext)
    assertNotNull(mockAnchor.hookedEventSource, "KernelModule.init failed to hook EventSource")
    mockAnchor.hookedEventSource.init(mockEventHub)

    authHolder = mockEventHub.eventsFired.find(_.isInstanceOf[CollectAuthenticationMethodEvent]).get.asInstanceOf[CollectAuthenticationMethodEvent].collector
    parseHolder = mockEventHub.eventsFired.find(_.isInstanceOf[CollectParseRequestMethodEvent]).get.asInstanceOf[CollectParseRequestMethodEvent].collector
    getHolder = mockEventHub.eventsFired.find(_.isInstanceOf[CollectGetStreamMethodEvent]).get.asInstanceOf[CollectGetStreamMethodEvent].collector
    actionHolder = mockEventHub.eventsFired.find(_.isInstanceOf[CollectActionMethodEvent]).get.asInstanceOf[CollectActionMethodEvent].collector
    putHolder = mockEventHub.eventsFired.find(_.isInstanceOf[CollectPutStreamMethodEvent]).get.asInstanceOf[CollectPutStreamMethodEvent].collector
  }

  @Test
  def testInit_FiresAllEvents(): Unit = {
    assertEquals(6, mockEventHub.eventsFired.length, "init() should fire 6 events")
    assertTrue(mockEventHub.eventsFired.exists(_.isInstanceOf[CollectAuthenticationMethodEvent]))
    assertTrue(mockEventHub.eventsFired.exists(_.isInstanceOf[CollectParseRequestMethodEvent]))
    assertTrue(mockEventHub.eventsFired.exists(_.isInstanceOf[CollectActionMethodEvent]))
    assertTrue(mockEventHub.eventsFired.exists(_.isInstanceOf[CollectPutStreamMethodEvent]))
    assertTrue(mockEventHub.eventsFired.exists(_.isInstanceOf[CollectGetStreamMethodEvent]))
  }

  @Test
  def testDoAction_WithHandler(): Unit = {
    val mockHandler = new MockActionHandler()
    val mockRequest = new MockDftpActionRequest("test-action")
    val mockResponse = new MockDftpActionResponse()

    actionHolder.add(mockHandler)
    kernelModule.doAction(mockRequest, mockResponse)

    assertTrue(mockHandler.doActionCalled, "Injected ActionHandler should be called")
    assertEquals(mockRequest, mockHandler.requestCalledWith, "Request mismatch")
    assertFalse(mockResponse.errorSent, "Should not send error if handler exists")
  }

  @Test
  def testDoAction_NoHandler(): Unit = {
    val mockRequest = new MockDftpActionRequest("test-action")
    val mockResponse = new MockDftpActionResponse()

    kernelModule.doAction(mockRequest, mockResponse)

    assertTrue(mockResponse.errorSent, "Should send error if handler missing")
    assertEquals(404, mockResponse.errorCode, "Should return 404")
    assertTrue(mockResponse.message.contains("test-action"), "Message should contain action name")
  }

  @Test
  def testGetStream_WithHandler(): Unit = {
    val mockHandler = new MockGetStreamHandler()
    val mockRequest = new MockDftpGetStreamRequest()
    val mockResponse = new MockDftpGetStreamResponse()

    getHolder.addMethod(mockHandler)
    kernelModule.getStream(mockRequest, mockResponse)

    assertTrue(mockHandler.doGetStreamCalled, "Injected GetStreamHandler should be called")
    assertFalse(mockResponse.errorSent, "Should not send error if handler exists")
  }

  @Test
  def testGetStream_NoHandler(): Unit = {
    val mockRequest = new MockDftpGetStreamRequest()
    val mockResponse = new MockDftpGetStreamResponse()

    kernelModule.getStream(mockRequest, mockResponse)

    assertTrue(mockResponse.errorSent, "Should send error if handler missing")
    assertEquals(404, mockResponse.errorCode, "Should return 404")
  }

  @Test
  def testPutStream_WithHandler(): Unit = {
    val mockHandler = new MockPutStreamHandler()
    val mockRequest = new MockDftpPutStreamRequest()
    val mockResponse = new MockDftpPutStreamResponse()

    putHolder.add(mockHandler)
    kernelModule.putStream(mockRequest, mockResponse)

    assertTrue(mockHandler.doPutStreamCalled, "Injected PutStreamHandler should be called")
    assertFalse(mockResponse.errorSent, "Should not send error if handler exists")
  }

  @Test
  def testPutStream_NoHandler(): Unit = {
    val mockRequest = new MockDftpPutStreamRequest()
    val mockResponse = new MockDftpPutStreamResponse()

    kernelModule.putStream(mockRequest, mockResponse)

    assertTrue(mockResponse.errorSent, "Should send error if handler missing")
    assertEquals(500, mockResponse.errorCode, "Should return 500")
  }

  @Test
  def testParseGetStreamRequest_WithHandler(): Unit = {
    val mockParser = new MockGetStreamRequestParser()
    val mockToken = Array[Byte](1, 2)
    val mockPrincipal = new MockUserPrincipal("test")

    parseHolder.add(mockParser)
    val result = kernelModule.parseGetStreamRequest(mockToken, mockPrincipal)

    assertTrue(mockParser.parseCalled, "Injected Parser should be called")
    assertEquals(mockParser.requestToReturn, result, "Returned request mismatch")
  }

  @Test
  def testParseGetStreamRequest_NoHandler(): Unit = {
    val mockToken = Array[Byte](1, 2)
    val mockPrincipal = new MockUserPrincipal("test")

    val ex = assertThrows(classOf[Exception], () => {
      kernelModule.parseGetStreamRequest(mockToken, mockPrincipal)
      ()
    }, "Should throw exception if parser missing")

    assertTrue(ex.getMessage.contains("GetStreamRequestParser"), "Message should indicate missing parser")
  }

  @Test
  def testAuthenticate_WithHandler(): Unit = {
    val mockAuth = new MockAuthenticationService()

    authHolder.add(mockAuth)
    val user = kernelModule.authenticate(MockCredentials)

    assertTrue(mockAuth.authenticateCalled, "Injected AuthenticationService should be called")
    assertEquals(mockAuth.userToReturn, user, "Returned User mismatch")
  }

  @Test
  def testAuthenticate_NoHandler(): Unit = {
    val user = kernelModule.authenticate(MockCredentials)

    assertTrue(user.isInstanceOf[UserPrincipalWithCredentials], "Default should return UserPrincipalWithCredentials")
    assertEquals(MockCredentials, user.asInstanceOf[UserPrincipalWithCredentials].credentials, "Credentials mismatch")
  }
}