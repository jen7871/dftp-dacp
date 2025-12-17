/**
 * @Author Yomi
 * @Description:
 * @Data 2025/11/5 11:00
 * @Modified By:
 */
package link.rdcn.dacp.catalog

import link.rdcn.server.ServerContext
import link.rdcn.struct.ValueType.{LongType, RefType, StringType}
import link.rdcn.struct._
import org.apache.jena.rdf.model.Model
import org.json.JSONObject
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.Test

import java.io.ByteArrayInputStream

class CatalogServiceTest {

  private val BASE_URL = "dftp://test-host:1234"

  // Helper method to create a simple ServerContext mock locally
  private def createMockServerContext(): ServerContext = new ServerContext {
    override def getHost(): String = "mock-host"
    override def getPort(): Int = 9999
    override def getProtocolScheme(): String = "dftp"
    override def getDftpHome(): Option[String] = None
  }

  /**
   * Test doListDataSets method
   */
  @Test
  def testDoListDataSets(): Unit = {
    // 1. Localize test data
    val dataSetName = "dataset1"
    val rdfXml = "<rdf:RDF xmlns:rdf=\"http://www.w3.org/1999/02/22-rdf-syntax-ns#\"/>"

    // 2. Local anonymous mock implementing only necessary methods
    val mockService = new CatalogService {
      override def listDataSetNames(): List[String] = List(dataSetName)

      override def getDataSetMetaData(dataSetId: String, rdfModel: Model): Unit = {
        rdfModel.read(new ByteArrayInputStream(rdfXml.getBytes), null, "RDF/XML")
      }

      // Methods below are not used in this test path, providing minimal or empty implementation
      override def accepts(request: CatalogServiceRequest): Boolean = false
      override def getDataFrameMetaData(name: String, model: Model): Unit = {}
      override def listDataFrameNames(dataSetId: String): List[String] = Nil
      override def getDocument(name: String): DataFrameDocument = null
      override def getStatistics(name: String): DataFrameStatistics = null
      override def getSchema(name: String): Option[StructType] = None
      override def getDataFrameTitle(name: String): Option[String] = None
    }

    // 3. Execute
    val df = mockService.doListDataSets(BASE_URL)

    // 4. Assertions
    val expectedSchema = StructType.empty
      .add("name", StringType)
      .add("meta", StringType)
      .add("DataSetInfo", StringType)
      .add("dataFrames", RefType)

    assertEquals(expectedSchema, df.schema, "Schema returned by doListDataSets does not match")

    val rows = df.collect()
    assertEquals(1, rows.length, "Row count returned by doListDataSets should be 1")

    val row1 = rows.head
    assertEquals(dataSetName, row1._1, "Column 'name' in the first row does not match")

    // Verify DataSetInfo JSON
    val infoJson = new JSONObject(row1.getAs[String](2))
    assertEquals(dataSetName, infoJson.getString("name"), "Field 'name' in 'DataSetInfo' JSON does not match")

    // Verify Ref link
    assertEquals(s"$BASE_URL/listDataFrames/$dataSetName", row1._4.asInstanceOf[DFRef].url,
      "URL in 'dataFrames' (Ref) column does not match")
  }

  /**
   * Test doListHostInfo method
   */
  @Test
  def testDoListHostInfo(): Unit = {
    // For this test, CatalogService implementation doesn't matter as doListHostInfo is final and depends on ServerContext
    val mockService = new CatalogService {
      override def accepts(r: CatalogServiceRequest): Boolean = false
      override def listDataSetNames(): List[String] = Nil
      override def getDataSetMetaData(id: String, m: Model): Unit = {}
      override def getDataFrameMetaData(n: String, m: Model): Unit = {}
      override def listDataFrameNames(id: String): List[String] = Nil
      override def getDocument(n: String): DataFrameDocument = null
      override def getStatistics(n: String): DataFrameStatistics = null
      override def getSchema(n: String): Option[StructType] = None
      override def getDataFrameTitle(n: String): Option[String] = None
    }

    val mockContext = createMockServerContext()

    val df = mockService.doListHostInfo(mockContext)

    val expectedSchema = StructType.empty
      .add("hostInfo", StringType)
      .add("resourceInfo", StringType)

    assertEquals(expectedSchema, df.schema, "Schema returned by doListHostInfo does not match")

    val rows = df.collect()
    assertEquals(1, rows.length, "doListHostInfo should return exactly 1 row")

    // Verify hostInfo JSON
    val hostInfoJson = new JSONObject(rows.head.getAs[String](0))
    assertEquals("mock-host", hostInfoJson.getString("faird.host.position"),
      "Field 'faird.host.position' in hostInfo JSON does not match")
    assertEquals("9999", hostInfoJson.getString("faird.host.port"),
      "Field 'faird.host.port' in hostInfo JSON does not match")

    // Verify resourceInfo JSON
    val resourceInfoJson = new JSONObject(rows.head.getAs[String](1))
    assertTrue(resourceInfoJson.has("cpu.cores"), "resourceInfo JSON should contain 'cpu.cores'")
    assertTrue(resourceInfoJson.has("jvm.memory.max.mb"), "resourceInfo JSON should contain 'jvm.memory.max.mb'")
  }

  /**
   * Test doListDataFrames - When Schema is empty
   */
  @Test
  def testDoListDataFrames_WithEmptySchema(): Unit = {
    val dataSetName = "dataset1"
    val tableName = "table_a"
    val fullTableName = s"${dataSetName}_$tableName"
    val tableTitle = s"Title for $fullTableName"
    val tableRowCount = 123L
    val tableByteSize = 456L

    // Local anonymous mock with specific logic for this test case
    val mockService = new CatalogService {
      override def listDataFrameNames(dataSetId: String): List[String] = List(fullTableName)

      override def getSchema(dataFrameName: String): Option[StructType] = Some(StructType.empty)

      override def getStatistics(dataFrameName: String): DataFrameStatistics = new DataFrameStatistics {
        override def rowCount: Long = tableRowCount
        override def byteSize: Long = tableByteSize
      }

      override def getDataFrameTitle(dataFrameName: String): Option[String] = Some(tableTitle)

      override def getDocument(dataFrameName: String): DataFrameDocument = new DataFrameDocument() {
        // Methods unused in this flow can be left unimplemented or return None
        override def getDataFrameTitle(): Option[String] = None

        override def getSchemaURL(): Option[String] = ???

        override def getColumnURL(colName: String): Option[String] = ???

        override def getColumnAlias(colName: String): Option[String] = ???

        override def getColumnTitle(colName: String): Option[String] = ???
      }

      // Unused methods
      override def accepts(r: CatalogServiceRequest): Boolean = false
      override def listDataSetNames(): List[String] = Nil
      override def getDataSetMetaData(id: String, m: Model): Unit = {}
      override def getDataFrameMetaData(n: String, m: Model): Unit = {}
    }

    val df = mockService.doListDataFrames(s"/listDataFrames/$dataSetName", BASE_URL)

    val expectedSchema = StructType.empty
      .add("name", StringType)
      .add("size", LongType)
      .add("title", StringType)
      .add("document", StringType)
      .add("schema", StringType)
      .add("statistics", StringType)
      .add("dataFrame", RefType)

    assertEquals(expectedSchema, df.schema, "Schema returned by doListDataFrames does not match")

    val rows = df.collect()
    assertEquals(1, rows.length, "doListDataFrames should return 1 row")

    val row1 = rows.head
    assertEquals(fullTableName, row1._1, "Column 'name' in the first row does not match")
    assertEquals(tableRowCount, row1._2, "Column 'size' (rowCount) does not match")
    assertEquals(tableTitle, row1._3, "Column 'title' does not match")

    // Verify 'document' (JSON) column
    // Since Schema is empty, CatalogFormatter.getDataFrameDocumentJsonString returns "[]"
    assertEquals("[]", row1._4, "Column 'document' should be '[]' for empty schema")

    // Verify 'schema' string column
    assertEquals(StructType.empty.toString, row1._5.toString, "Column 'schema' string does not match")

    // Verify 'statistics' (JSON) column
    val statsJson = new JSONObject(row1.getAs[String](5))
    assertEquals(tableRowCount, statsJson.getLong("rowCount"), "Field 'rowCount' in statistics JSON does not match")
    assertEquals(tableByteSize, statsJson.getLong("byteSize"), "Field 'byteSize' in statistics JSON does not match")

    // Verify 'dataFrame' (Ref) column
    assertEquals(s"$BASE_URL/$fullTableName", row1._7.asInstanceOf[DFRef].url,
      "URL in 'dataFrame' (Ref) column does not match")
  }
}
