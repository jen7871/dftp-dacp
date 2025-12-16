/**
 * @Author Yomi
 * @Description:
 * @Data 2025/11/5 10:36
 * @Modified By:
 */
package link.rdcn.dacp.catalog

import link.rdcn.client.DftpClient
import link.rdcn.dacp.catalog.DacpCatalogModuleTest._
import link.rdcn.server._
import link.rdcn.server.module._
import link.rdcn.struct.ValueType.{IntType, StringType}
import link.rdcn.struct._
import link.rdcn.user.{Credentials, UserPasswordAuthService, UserPrincipal, UserPrincipalWithCredentials}
import org.apache.arrow.flight.FlightRuntimeException
import org.apache.jena.rdf.model.{Model, ModelFactory}
import org.junit.jupiter.api.Assertions.{assertEquals, assertNotNull, assertThrows, assertTrue}
import org.junit.jupiter.api.{AfterAll, BeforeAll, Test}

import java.io.ByteArrayInputStream
import java.nio.charset.StandardCharsets

object DacpCatalogModuleTest {
  private var server: DftpServer = _
  private var client: DftpClient = _
  private val testPort = 3101
  private val baseUrl = s"dftp://0.0.0.0:$testPort"
  private val catalogService = new CatalogService {
    override def getDataSetMetaData(datasetName: String, model: Model): Unit =
      model.add(MockCatalogData.getMockModel)

    override def getDataFrameMetaData(dataFrameName: String, model: Model): Unit =
      model.add(MockCatalogData.getMockModel)

    override def getDocument(dataFrameName: String): DataFrameDocument =
      if (dataFrameName == "my_table") MockCatalogData.mockDoc else throw new NoSuchElementException("Doc not found")

    override def getStatistics(dataFrameName: String): DataFrameStatistics =
      if (dataFrameName == "my_table") MockCatalogData.mockStats else throw new NoSuchElementException("Stats not found")

    override def getSchema(dataFrameName: String): Option[StructType] =
      if (dataFrameName == "my_table") Some(MockCatalogData.mockSchema) else None

    override def getDataFrameTitle(dataFrameName: String): Option[String] =
      if (dataFrameName == "my_table") Some(MockCatalogData.mockTitle) else None

    override def accepts(request: CatalogServiceRequest): Boolean = true

    /**
     * 列出所有数据集名称
     *
     * @return java.util.List[String]
     */
    override def listDataSetNames(): List[String] = List("my_set")

    /**
     * 列出指定数据集下的所有数据帧名称
     *
     * @param dataSetId 数据集 ID
     * @return java.util.List[String]
     */
    override def listDataFrameNames(dataSetId: String): List[String] = List("my_table")
  }
  private val userPasswordAuthService = new UserPasswordAuthService {
    override def authenticate(credentials: Credentials): UserPrincipal =
      UserPrincipalWithCredentials(credentials)

    override def accepts(credentials: Credentials): Boolean = true
  }

  @BeforeAll
  def startServer(): Unit = {
    val config = DftpServerConfig("0.0.0.0", testPort, dftpHome = Some("data"))
    val modules = Array(
      new BaseDftpModule,
      new DacpCatalogModule,
      new GetStreamModule(),
//      new CatalogServiceModule(catalogService),
      new UserPasswordAuthModule(userPasswordAuthService),
    )
    server = DftpServer.start(config, modules)

    Thread.sleep(2000)

    client = DftpClient.connect(baseUrl)
    assertNotNull(client, "client connected failed")
  }

  @AfterAll
  def stopServer(): Unit = {
    if (server != null) server.close()
  }
}

/**
 * DacpCatalogModule test
 */
class DacpCatalogModuleTest {

  @Test
  def testGetSchemaAction(): Unit = {
    val actionName = "/getSchema/my_table"
    val resultBytes = client.doAction(actionName)
    val resultString = new String(resultBytes, StandardCharsets.UTF_8)

    val expectedString = MockCatalogData.mockSchema.toString

    assertEquals(expectedString, resultString, s"Action $actionName return  Schema string cant't match")
  }

  @Test
  def testGetDataFrameTitleAction(): Unit = {
    val actionName = "/getDataFrameTitle/my_table"
    val resultBytes = client.doAction(actionName)
    val resultString = new String(resultBytes, StandardCharsets.UTF_8)

    val expectedString = MockCatalogData.mockTitle

    assertEquals(expectedString, resultString, s"Action $actionName return  Title string cant't match")
  }

  @Test
  def testGetStatisticsAction(): Unit = {
    val actionName = "/getStatistics/my_table"
    val resultBytes = client.doAction(actionName)
    val resultString = new String(resultBytes, StandardCharsets.UTF_8)

    val expectedString =  CatalogFormatter.getDataFrameStatisticsString(MockCatalogData.mockStats)

    assertEquals(expectedString, resultString, s"Action $actionName return  Statistics JSON cant't match")
  }

  @Test
  def testGetDocumentAction(): Unit = {
    val actionName = "/getDocument/my_table"
    val resultBytes = client.doAction(actionName)
    val resultString = new String(resultBytes, StandardCharsets.UTF_8)

    val expectedString = CatalogFormatter.getDataFrameDocumentJsonString(
      MockCatalogData.mockDoc,
      Some(MockCatalogData.mockSchema)
    )

    assertEquals(expectedString, resultString, s"Action $actionName return  Document JSON cant't match")
  }

  @Test
  def testGetDataFrameMetaDataAction(): Unit = {
    val actionName = "/getDataFrameMetaData/my_table"
    val resultBytes = client.doAction(actionName)

    val expectedModel = MockCatalogData.getMockModel
    val resultModel = ModelFactory.createDefaultModel()
    resultModel.read(new ByteArrayInputStream(resultBytes), null, "RDF/XML")

    assertTrue(expectedModel.isIsomorphicWith(resultModel), s"Action $actionName return  RDF/XML content cant't match")
  }

  @Test
  def testGetDataSetMetaDataAction(): Unit = {
    val actionName = "/getDataSetMetaData/my_set"
    val resultBytes = client.doAction(actionName)

    val expectedModel = MockCatalogData.getMockModel
    val resultModel = ModelFactory.createDefaultModel()
    resultModel.read(new ByteArrayInputStream(resultBytes), null, "RDF/XML")

    assertTrue(expectedModel.isIsomorphicWith(resultModel), s"Action $actionName return  RDF/XML content cant't match")
  }

  @Test
  def testUnknownAction(): Unit = {
    val actionName = "/unknown/action"
    val ex = assertThrows(classOf[FlightRuntimeException], () => {
      client.doAction(actionName)
      ()
    }, "should throw FlightRuntimeException")

    assertTrue(ex.getMessage.contains(s"unknown action: $actionName"), "should be unknown action")
  }

  @Test
  def testListDataSetsStream(): Unit = {
    val path = s"$baseUrl/listDataSets"
    val df = client.get(path)
    val expectedDF = catalogService.doListDataSets("")
    val actualRows = df.collect()
    val expectedRows = expectedDF.collect()

    assertEquals(expectedDF.schema.toString, df.schema.toString, s"GetStream $path return  Schema cant't match")
    assertEquals(expectedRows.length, actualRows.length, s"GetStream $path return lines count cant't match")
    assertEquals(expectedRows.head._1, actualRows.head._1, s"GetStream $path return content cant't match")
  }

  @Test
  def testListDataFramesStream(): Unit = {
    val path = s"$baseUrl/listDataFrames/my_set"
    val df = client.get(path)
    val expectedDF = catalogService.doListDataFrames("my_set",baseUrl)
    val actualRows = df.collect()
    val expectedRows = expectedDF.collect()

    assertEquals(expectedDF.schema, df.schema, s"GetStream $path return  Schema cant't match")
    assertEquals(expectedRows.length, actualRows.length, s"GetStream $path return lines count cant't match")
    assertEquals(expectedRows.head._1, actualRows.head._1, s"GetStream $path return content cant't match")
  }

  @Test
  def testListHostsStream(): Unit = {
    val path = s"$baseUrl/listHosts"
    val df = client.get(path)
    val expectedDF = catalogService.doListHostInfo(new MockServerContext)
    val actualRows = df.collect()
    val expectedRows = expectedDF.collect()

    assertEquals(expectedDF.schema, df.schema, s"GetStream $path return Schema can't match")
    assertEquals(expectedRows.length, actualRows.length, s"GetStream $path lines count return can't match")
    assertEquals(expectedRows.head._1, actualRows.head._1, s"GetStream $path content return can't match")
  }

  @Test
  def testStreamHandlerChaining(): Unit = {
    val path = "/oldStream"
    val df = client.get(path)
    val expectedDF = MockCatalogData.mockDF
    val actualRows = df.collect()
    val expectedRows = expectedDF.collect()

    assertEquals(expectedDF.schema, df.schema, "GetStream chain return schema can't match")
    assertEquals(expectedRows.toString(), actualRows.toString(), "GetStream chain return can't match")
  }

  @Test
  def testUnknownStream(): Unit = {
    val path = "/unknown/stream"

    val ex = assertThrows(classOf[FlightRuntimeException], () => {
      client.get(s"${baseUrl}${path}").collect()
      ()
    }, "请求未知 Stream 应抛出 FlightRuntimeException")

    assertTrue(ex.getMessage.contains(s"not found"), "should be unknown stream")
  }
}

class GetStreamModule extends DftpModule {
  private val mockSchema: StructType = StructType.empty.add("id", IntType).add("name", StringType)
  private val getStreamHolder = new FilteredGetStreamMethods
  private var serverContext: ServerContext = _
  private val eventHandler = new EventHandler {

    override def accepts(event: CrossModuleEvent): Boolean = true

    override def doHandleEvent(event: CrossModuleEvent): Unit = {
      event match {
        case r: CollectGetStreamMethodEvent => r.collect(
          new GetStreamMethod {
            override def accepts(request: DftpGetStreamRequest): Boolean = request.asInstanceOf[DftpGetPathStreamRequest].getRequestURL().contains("oldStream")

            override def doGetStream(request: DftpGetStreamRequest, response: DftpGetStreamResponse): Unit = {
              request.asInstanceOf[DftpGetPathStreamRequest].getRequestURL() match {
                case url if url.contains("oldStream") =>
                  response.sendDataFrame(mockDF)
              }
            }
          })

        case _ =>
      }
    }

    def mockDF: DataFrame = DefaultDataFrame(
      mockSchema, Seq(Row(1, "data")).iterator
    )
  }

  override def init(anchor: Anchor, serverContext: ServerContext): Unit = {
    this.serverContext = serverContext
    anchor.hook(eventHandler)
    anchor.hook(new EventSource {
      override def init(eventHub: EventHub): Unit =
        eventHub.fireEvent(new CollectGetStreamMethodEvent(getStreamHolder))
    })
  }

  override def destroy(): Unit = {
  }
}

