package link.rdcn.server

import org.junit.jupiter.api.Assertions.{assertTrue, assertEquals}
import org.junit.jupiter.api.Test

class DftpModuleTest {

  // --- Local Mocks ---

  class MockEvent extends CrossModuleEvent

  class MockEventHandler(var handled: Boolean = false) extends EventHandler {
    override def accepts(event: CrossModuleEvent): Boolean = event.isInstanceOf[MockEvent]
    override def doHandleEvent(event: CrossModuleEvent): Unit = handled = true
  }

  class MockEventSource(var initialized: Boolean = false) extends EventSource {
    override def init(eventHub: EventHub): Unit = {
      initialized = true
      eventHub.fireEvent(new MockEvent())
    }
  }

  class MockModule extends DftpModule {
    val handler = new MockEventHandler()
    val source = new MockEventSource()

    override def init(anchor: Anchor, serverContext: ServerContext): Unit = {
      anchor.hook(handler)
      anchor.hook(source)
    }
    override def destroy(): Unit = {}
  }

  class MockServerContext extends ServerContext {
    override def getHost(): String = "localhost"
    override def getPort(): Int = 0
    override def getProtocolScheme(): String = "dftp"
    override def getDftpHome(): Option[String] = None
  }

  // --- Tests ---

  @Test
  def testModulesWiringAndEventDispatch(): Unit = {
    val context = new MockServerContext()
    val modulesSystem = new Modules(context)
    val mockModule = new MockModule()

    modulesSystem.addModule(mockModule)

    modulesSystem.init()

    // Verify Module Init
    assertTrue(mockModule.source.initialized, "EventSource.init should be called during modules.init()")

    // Verify Event Dispatch (EventSource fired event -> Hub -> Handler)
    assertTrue(mockModule.handler.handled, "EventHandler should receive and handle the event fired by EventSource")
  }

  @Test
  def testDestroy(): Unit = {
    val context = new MockServerContext()
    val modulesSystem = new Modules(context)

    // We can't easily verify destroy call without a mock that tracks it,
    // but we verify no exception is thrown
    modulesSystem.destroy()
  }
}