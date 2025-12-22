/**
 * @Author Yomi
 * @Description:
 * @Data 2025/11/6 18:27
 * @Modified By:
 */
package link.rdcn.server.module

import link.rdcn.server._
import link.rdcn.user._
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{BeforeEach, Test}

class UserPasswordAuthModuleTest {

  // --- Local Mocks ---

  case object MockUser extends UserPrincipal {
    def getName: String = "MockUser"
  }

  object MockCredentials extends UsernamePassword("mockUser", "mockPassword")

  class MockAuthenticationService(name: String) extends UserPasswordAuthService {
    var acceptsCreds: Boolean = false
    var userToReturn: UserPrincipal = MockUser
    var authenticateCalled: Boolean = false
    var credsChecked: Credentials = _

    override def accepts(credentials: Credentials): Boolean = acceptsCreds

    override def authenticate(credentials: Credentials): UserPrincipal = {
      authenticateCalled = true
      credsChecked = credentials
      userToReturn
    }

    override def toString: String = s"MockAuthenticationService($name)"
  }

  class MockAnchor extends Anchor {
    var hookedHandler: EventHandler = _
    override def hook(service: EventSource): Unit = {}
    override def hook(service: EventHandler): Unit = { hookedHandler = service }
  }

  class MockServerContext extends ServerContext {
    override def getHost(): String = "mock-host"
    override def getPort(): Int = 0
    override def getProtocolScheme(): String = "dftp"
    override def getDftpHome(): Option[String] = None
  }

  class OtherMockEvent extends CrossModuleEvent

  // --- Tests ---

  private var mockOldService: MockAuthenticationService = _
  private var mockInnerService: MockAuthenticationService = _
  private var moduleToTest: UserPasswordAuthModule = _
  private var mockAnchor: MockAnchor = _
  private var hookedEventHandler: EventHandler = _

  @BeforeEach
  def setUp(): Unit = {
    mockOldService = new MockAuthenticationService("OldService")
    mockInnerService = new MockAuthenticationService("InnerService")

    moduleToTest = new UserPasswordAuthModule(mockInnerService)
    mockAnchor = new MockAnchor()
    val mockContext = new MockServerContext()

    moduleToTest.init(mockAnchor, mockContext)
    hookedEventHandler = mockAnchor.hookedHandler
    assertNotNull(hookedEventHandler, "init() failed to register EventHandler")
  }

  @Test
  def testEventHandlerAcceptsLogic(): Unit = {
    val validEvent = new CollectAuthenticationMethodEvent(new Workers[AuthenticationMethod])
    val invalidEvent = new OtherMockEvent()

    assertTrue(hookedEventHandler.accepts(validEvent), "EventHandler should accept RequireAuthenticatorEvent")
    assertFalse(hookedEventHandler.accepts(invalidEvent), "EventHandler should not accept other event types")
  }

  @Test
  def testChainingLogic_InnerServiceAccepts(): Unit = {
    mockInnerService.acceptsCreds = true
    mockOldService.acceptsCreds = false

    val holder = new Workers[AuthenticationMethod]()
    holder.add(mockOldService)
    val event = new CollectAuthenticationMethodEvent(holder)

    hookedEventHandler.doHandleEvent(event)
    val chainedService = holder.work(runMethod = s => s, onFail = null)
    assertNotNull(chainedService, "Holder should not be empty")

    assertTrue(chainedService.accepts(MockCredentials), "Chained accepts() should return true (InnerService accepts)")

    val user = chainedService.authenticate(MockCredentials)
    assertEquals(mockInnerService.userToReturn, user, "Chained authenticate() should return InnerService's User")

    assertTrue(mockInnerService.authenticateCalled, "InnerService.authenticate should be called")
    assertEquals(MockCredentials, mockInnerService.credsChecked, "InnerService checked wrong credentials")
    assertFalse(mockOldService.authenticateCalled, "OldService.authenticate should not be called")
  }

  @Test
  def testChainingLogic_OldServiceAccepts(): Unit = {
    mockInnerService.acceptsCreds = false
    mockOldService.acceptsCreds = true
    val oldServiceUser = new UserPrincipal { def getName = "OldUser" }
    mockOldService.userToReturn = oldServiceUser

    val holder = new Workers[AuthenticationMethod]()
    holder.add(mockOldService)
    val event = new CollectAuthenticationMethodEvent(holder)

    hookedEventHandler.doHandleEvent(event)
    val chainedService = holder.work(runMethod = s => s, onFail = null)

    assertTrue(chainedService.accepts(MockCredentials), "Chained accepts() should return true (OldService accepts)")

    val user = chainedService.authenticate(MockCredentials)
    assertEquals(mockOldService.userToReturn, user, "Chained authenticate() should return OldService's User")

    assertFalse(mockInnerService.authenticateCalled, "InnerService.authenticate should not be called")
    assertTrue(mockOldService.authenticateCalled, "OldService.authenticate should be called")
  }

  @Test
  def testChainingLogic_NoServiceAccepts(): Unit = {
    mockInnerService.acceptsCreds = false
    mockOldService.acceptsCreds = false

    val holder = new Workers[AuthenticationMethod]()
    holder.add(mockOldService)
    val event = new CollectAuthenticationMethodEvent(holder)

    hookedEventHandler.doHandleEvent(event)
    val chainedService = holder.work(runMethod = s => s, onFail = null)

    assertFalse(chainedService.accepts(MockCredentials), "Chained accepts() should return false")
  }

  @Test
  def testChainingLogic_HolderInitiallyEmpty(): Unit = {
    mockInnerService.acceptsCreds = true

    val holder = new Workers[AuthenticationMethod]()
    val event = new CollectAuthenticationMethodEvent(holder)

    hookedEventHandler.doHandleEvent(event)
    val chainedService = holder.work(runMethod = s => s, onFail = null)

    assertTrue(chainedService.accepts(MockCredentials), "Chained accepts() should return true")

    val user = chainedService.authenticate(MockCredentials)
    assertEquals(mockInnerService.userToReturn, user, "Chained authenticate() should return InnerService's User")
    assertTrue(mockInnerService.authenticateCalled, "InnerService.authenticate should be called")
  }
}