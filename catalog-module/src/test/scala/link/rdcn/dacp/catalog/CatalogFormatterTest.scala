/**
 * @Author Yomi
 * @Description:
 * @Data 2025/11/5 11:00
 * @Modified By:
 */
package link.rdcn.dacp.catalog

import link.rdcn.dacp.catalog.ConfigKeys.{FAIRD_HOST_PORT, FAIRD_HOST_POSITION}
import link.rdcn.server.ServerContext
import link.rdcn.struct.{DataFrameDocument, DataFrameStatistics, StructType}
import org.json.JSONObject
import org.junit.jupiter.api.Assertions.{assertEquals, assertNotNull, assertTrue}
import org.junit.jupiter.api.Test


class CatalogFormatterTest {

  // Local mock objects
  private val mockStatistics = new DataFrameStatistics{
    override def rowCount: Long = 1234L
    override def byteSize: Long = 5678L
  }

  private val mockDoc = new DataFrameDocument {
    override def getSchemaURL(): Option[String] = ???
    override def getDataFrameTitle(): Option[String] = ???
    override def getColumnURL(colName: String): Option[String] = ???
    override def getColumnAlias(colName: String): Option[String] = ???
    override def getColumnTitle(colName: String): Option[String] = ???
  }

  /**
   * Test getDataFrameStatisticsString method
   */
  @Test
  def testDataFrameStatisticsString(): Unit = {
    val jsonString = CatalogFormatter.getDataFrameStatisticsString(mockStatistics)
    assertNotNull(jsonString, "Returned JSON string should not be null")

    // Parse JSON to verify content
    val jo = new JSONObject(jsonString)

    assertEquals(1234L, jo.getLong("rowCount"), "Value of 'rowCount' does not match")
    assertEquals(5678L, jo.getLong("byteSize"), "Value of 'byteSize' does not match")
  }

  /**
   * Test getHostInfoString method
   */
  @Test
  def testHostInfoString(): Unit = {
    // Define MockServerContext locally using anonymous class
    val mockContext = new ServerContext {
      override def getHost(): String = "mock-host"
      override def getPort(): Int = 9999
      override def getProtocolScheme(): String = "dftp" // Unused but required by trait
      override def getDftpHome(): Option[String] = None // Unused but required by trait
    }

    val jsonString = CatalogFormatter.getHostInfoString(mockContext)
    assertNotNull(jsonString, "Returned JSON string should not be null")

    // Parse JSON to verify content
    val jo = new JSONObject(jsonString)

    assertEquals("mock-host", jo.getString(FAIRD_HOST_POSITION), "Host position does not match")
    assertEquals("9999", jo.getString(FAIRD_HOST_PORT), "Host port does not match")
  }

  /**
   * Test getResourceStatusString method
   * This reads *real* system data, so we only test format and key existence
   */
  @Test
  def testResourceStatusString(): Unit = {
    val resourceMap = CatalogFormatter.getResourceStatusString()
    assertNotNull(resourceMap, "Returned Map should not be null")

    assertTrue(resourceMap.contains("cpu.cores"), "Map should contain key 'cpu.cores'")
    assertTrue(resourceMap.contains("cpu.usage.percent"), "Map should contain key 'cpu.usage.percent'")
    assertTrue(resourceMap.contains("jvm.memory.max.mb"), "Map should contain key 'jvm.memory.max.mb'")
    assertTrue(resourceMap.contains("system.memory.total.mb"), "Map should contain key 'system.memory.total.mb'")

    // Verify value format
    assertTrue(resourceMap("cpu.usage.percent").endsWith("%"), "CPU usage should end with '%'")
    assertTrue(resourceMap("jvm.memory.max.mb").endsWith(" MB"), "JVM memory should end with ' MB'")
  }

  /**
   * Test getHostResourceString method
   * This reads *real* system data, so we only test format and key existence
   */
  @Test
  def testHostResourceString(): Unit = {
    val jsonString = CatalogFormatter.getHostResourceString()
    assertNotNull(jsonString, "Returned JSON string should not be null")

    val jo = new JSONObject(jsonString)

    assertTrue(jo.has("cpu.cores"), "JSON should contain key 'cpu.cores'")
    assertTrue(jo.has("jvm.memory.used.mb"), "JSON should contain key 'jvm.memory.used.mb'")

    // Verify value is a valid number (or contains %/MB, but here we check general validity)
    // The previous test checked string format, here we just ensure content exists
    assertTrue(jo.getString("cpu.cores").toInt > 0, "CPU cores should be greater than 0")
  }

  /**
   * Test getDataFrameDocumentJsonString - When schema is None
   */
  @Test
  def testDataFrameDocumentJsonString_SchemaNone(): Unit = {
    // Content of DataFrameDocument does not matter because schema is None
    val jsonString = CatalogFormatter.getDataFrameDocumentJsonString(mockDoc, None)

    assertEquals("[]", jsonString, "Should return an empty JSON array when schema is None")
  }

  /**
   * Test getDataFrameDocumentJsonString - When schema is Some(StructType.empty)
   */
  @Test
  def testDataFrameDocumentJsonString_SchemaEmpty(): Unit = {
    val emptySchema = Some(StructType.empty)

    val jsonString = CatalogFormatter.getDataFrameDocumentJsonString(mockDoc, emptySchema)

    assertEquals("[]", jsonString, "Should return an empty JSON array when schema is empty")
  }
}