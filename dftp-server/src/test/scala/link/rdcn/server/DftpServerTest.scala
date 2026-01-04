package link.rdcn.server

import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{AfterEach, Test}
import java.net.ServerSocket

class DftpServerTest {

  var server: DftpServer = _

  // Helper to find a random free port to avoid conflicts
  def findFreePort(): Int = {
    val socket = new ServerSocket(0)
    try socket.getLocalPort finally socket.close()
  }

  @AfterEach
  def tearDown(): Unit = {
    if (server != null) {
      server.close()
    }
  }

  @Test
  def testServerLifecycle(): Unit = {
    val port = findFreePort()
    // Configure server on localhost
    val config = DftpServerConfig("0.0.0.0", port)

    server = new DftpServer(config)

    // 1. Test Start (Non-blocking)
    try {
      server.start()
      // Allow some time for async thread to spin up
      Thread.sleep(500)
    } catch {
      case e: Exception => fail(s"Server failed to start: ${e.getMessage}")
    }

    // 2. Test Close
    try {
      server.close()
    } catch {
      case e: Exception => fail(s"Server failed to close: ${e.getMessage}")
    }
  }

  @Test
  def testFactoryMethodStart(): Unit = {
    val port = findFreePort()
    val config = DftpServerConfig("0.0.0.0", port)

    // Test static factory method
    server = DftpServer.start(config, Array.empty)
    assertNotNull(server, "Factory method should return server instance")

    Thread.sleep(200)
    server.close()
  }
}