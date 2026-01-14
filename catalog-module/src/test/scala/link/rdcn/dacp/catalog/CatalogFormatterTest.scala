/**
 * @Author Yomi
 * @Description:
 * @Data 2025/11/5 11:00
 * @Modified By:
 */
package link.rdcn.dacp.catalog

import link.rdcn.dacp.catalog.ConfigKeys.{FAIRD_HOST_PORT, FAIRD_HOST_POSITION}
import link.rdcn.message.DftpTicket.DftpTicket
import link.rdcn.server.ServerContext
import link.rdcn.struct.{Blob, DataFrame}
import org.junit.jupiter.api.Assertions.{assertEquals, assertNotNull, assertTrue}
import org.junit.jupiter.api.Test

class CatalogFormatterTest {

  /**
   * Test getHostInfo method.
   * Verify it returns a JSONObject containing host position and port.
   */
  @Test
  def testGetHostInfo(): Unit = {
    // Define MockServerContext locally using anonymous class
    val mockContext = new ServerContext {
      override def getHost(): String = "mock-host"
      override def getPort(): Int = 9999
      override def getProtocolScheme(): String = "dftp"
      override def getDftpHome(): Option[String] = None
      override def registry(dataframe: DataFrame): DftpTicket = "mock-ticket-df"
      override def registry(blob: Blob): DftpTicket = "mock-ticket-blob"
    }

    val jsonObject = CatalogFormatter.getHostInfo(mockContext)
    assertNotNull(jsonObject, "Returned JSONObject should not be null")

    assertEquals("mock-host", jsonObject.getString(FAIRD_HOST_POSITION), "Host position does not match")
    assertEquals("9999", jsonObject.getString(FAIRD_HOST_PORT), "Host port does not match")
  }

  /**
   * Test getSystemInfo method.
   * This reads real system data, so we checks for key existence and basic format.
   */
  @Test
  def testGetSystemInfo(): Unit = {
    val jsonObject = CatalogFormatter.getSystemInfo()
    assertNotNull(jsonObject, "Returned JSONObject should not be null")

    assertTrue(jsonObject.has("cpu.cores"), "JSONObject should contain key 'cpu.cores'")
    assertTrue(jsonObject.has("jvm.memory.used.mb"), "JSONObject should contain key 'jvm.memory.used.mb'")
    assertTrue(jsonObject.has("net.mac.address"), "JSONObject should contain key 'net.mac.address'")

    // Verify value is a valid number (or contains %/MB)
    assertTrue(jsonObject.getInt("cpu.cores") > 0, "CPU cores should be greater than 0")
    assertTrue(jsonObject.getString("jvm.memory.max.mb").endsWith(" MB"), "JVM memory should end with ' MB'")
  }
}