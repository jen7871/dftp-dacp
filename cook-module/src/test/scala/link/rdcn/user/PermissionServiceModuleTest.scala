/**
 * @Author Yomi
 * @Description:
 * @Data 2025/11/6 15:06
 * @Modified By:
 */
package link.rdcn.user

import link.rdcn.dacp.user.{DataOperationType, PermissionService, PermissionServiceModule, RequirePermissionServiceEvent}
import link.rdcn.server._
import link.rdcn.server.module.Workers
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertNotNull, assertTrue}
import org.junit.jupiter.api.{BeforeEach, Test}

import scala.collection.mutable.ArrayBuffer

class PermissionServiceModuleTest {

  // --- Local Mocks ---

  case object MockUser extends UserPrincipal {
    def getName: String = "MockUser"
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

  class MockPermissionService(name: String) extends PermissionService {
    var acceptsUser: Boolean = false
    var permissionResult: Boolean = false
    var checkPermissionCalled: Boolean = false
    var userChecked: UserPrincipal = _
    var dataFrameChecked: String = _
    var opsChecked: List[DataOperationType] = _

    override def accepts(user: UserPrincipal): Boolean = acceptsUser

    override def checkPermission(user: UserPrincipal, dataFrameName: String, opList: List[DataOperationType]): Boolean = {
      checkPermissionCalled = true
      userChecked = user
      dataFrameChecked = dataFrameName
      opsChecked = opList
      permissionResult
    }

    override def toString: String = s"MockPermissionService($name)"
  }

  // --- Test Setup ---

  private var mockOldService: MockPermissionService = _
  private var mockInnerService: MockPermissionService = _
  private var moduleToTest: PermissionServiceModule = _
  private var mockAnchor: MockAnchor = _
  private var hookedEventHandler: EventHandler = _

  @BeforeEach
  def setUp(): Unit = {
    mockOldService = new MockPermissionService("OldService")
    mockInnerService = new MockPermissionService("InnerService")

    moduleToTest = new PermissionServiceModule(mockInnerService)
    mockAnchor = new MockAnchor()
    val mockContext = new MockServerContext()

    moduleToTest.init(mockAnchor, mockContext)

    hookedEventHandler = mockAnchor.hookedHandler
    assertNotNull(hookedEventHandler, "init() failed to register EventHandler to Anchor")
  }

  @Test
  def testEventHandlerAcceptsLogic(): Unit = {
    val validEvent = RequirePermissionServiceEvent(new Workers[PermissionService])
    val invalidEvent = new OtherMockEvent()

    assertTrue(hookedEventHandler.accepts(validEvent), "EventHandler should accept RequirePermissionServiceEvent")
    assertFalse(hookedEventHandler.accepts(invalidEvent), "EventHandler should not accept other event types")
  }

  @Test
  def testChainingLogic_InnerServiceAccepts(): Unit = {
    // 1. Setup
    mockInnerService.acceptsUser = true
    mockInnerService.permissionResult = true

    mockOldService.acceptsUser = false
    mockOldService.permissionResult = false

    // 2. Mock Event
    val holder = new Workers[PermissionService]()
    holder.add(mockOldService)
    val event = RequirePermissionServiceEvent(holder)

    // 3. Execute
    hookedEventHandler.doHandleEvent(event)

    // 4. Extract
    val chainedService = holder.work(runMethod = s => s, onFail = null)
    assertNotNull(chainedService, "Holder should not be empty")

    // 5. Verify accepts()
    assertTrue(chainedService.accepts(MockUser), "Chained accepts() should return true (InnerService accepts)")

    // 6. Verify checkPermission()
    val ops = List(DataOperationType.Map)
    assertTrue(chainedService.checkPermission(MockUser, "data", ops), "Chained checkPermission() should return true (InnerService)")

    // Verify calls
    assertTrue(mockInnerService.checkPermissionCalled, "InnerService.checkPermission should be called")
    assertEquals(MockUser, mockInnerService.userChecked, "InnerService checked wrong user")
    assertEquals("data", mockInnerService.dataFrameChecked, "InnerService checked wrong dataFrameName")
    assertEquals(ops, mockInnerService.opsChecked, "InnerService checked wrong opList")

    assertFalse(mockOldService.checkPermissionCalled, "OldService.checkPermission should not be called")
  }

  @Test
  def testChainingLogic_OldServiceAccepts(): Unit = {
    // 1. Setup
    mockInnerService.acceptsUser = false

    mockOldService.acceptsUser = true
    mockOldService.permissionResult = true

    // 2. Mock Event
    val holder = new Workers[PermissionService]()
    holder.add(mockOldService)
    val event = RequirePermissionServiceEvent(holder)

    // 3. Execute
    hookedEventHandler.doHandleEvent(event)

    // 4. Extract
    val chainedService = holder.work(runMethod = s => s, onFail = null)
    assertNotNull(chainedService, "Holder should not be empty")

    // 5. Verify accepts()
    assertTrue(chainedService.accepts(MockUser), "Chained accepts() should return true (OldService accepts)")

    // 6. Verify checkPermission()
    val ops = List(DataOperationType.Filter)
    assertTrue(chainedService.checkPermission(MockUser, "data2", ops), "Chained checkPermission() should return true (OldService)")

    // Verify calls
    assertFalse(mockInnerService.checkPermissionCalled, "InnerService.checkPermission should not be called")
    assertTrue(mockOldService.checkPermissionCalled, "OldService.checkPermission should be called")
  }

  @Test
  def testChainingLogic_NoServiceAccepts(): Unit = {
    mockInnerService.acceptsUser = false
    mockOldService.acceptsUser = false

    val holder = new Workers[PermissionService]()
    holder.add(mockOldService)
    val event = RequirePermissionServiceEvent(holder)

    hookedEventHandler.doHandleEvent(event)

    val chainedService = holder.work(runMethod = s => s, onFail = null)

    assertFalse(chainedService.accepts(MockUser), "Chained accepts() should return false")
    assertFalse(chainedService.checkPermission(MockUser, "data", List()), "Chained checkPermission() should return false")
  }

  @Test
  def testChainingLogic_HolderInitiallyEmpty(): Unit = {
    mockInnerService.acceptsUser = true
    mockInnerService.permissionResult = true

    val holder = new Workers[PermissionService]()
    val event = RequirePermissionServiceEvent(holder)

    hookedEventHandler.doHandleEvent(event)

    val chainedService = holder.work(runMethod = s => s, onFail = null)

    assertTrue(chainedService.accepts(MockUser), "Chained accepts() should return true")

    val ops = List(DataOperationType.Select)
    assertTrue(chainedService.checkPermission(MockUser, "data", ops), "Chained checkPermission() should return true")

    assertTrue(mockInnerService.checkPermissionCalled, "InnerService.checkPermission should be called")
  }
}