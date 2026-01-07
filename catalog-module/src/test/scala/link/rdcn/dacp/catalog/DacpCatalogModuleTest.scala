package link.rdcn.dacp.catalog

import link.rdcn.server._
import link.rdcn.server.module.{ActionMethod, CollectActionMethodEvent, Workers}
import org.json.JSONObject
import org.junit.jupiter.api.Assertions.{assertEquals, assertNotNull, assertTrue}
import org.junit.jupiter.api.Test

class DacpCatalogModuleTest {

  /**
   * Test the initialization and action dispatching of DacpCatalogModule.
   */
  @Test
  def testModuleActionDispatch(): Unit = {
    // 1. Setup Environment
    val module = new DacpCatalogModule()

    // Mock Anchor to capture hooks
    val mockAnchor = new Anchor {
      var eventHandler: EventHandler = _
      var eventSource: EventSource = _
      override def hook(service: EventHandler): Unit = this.eventHandler = service
      override def hook(service: EventSource): Unit = this.eventSource = service
    }

    val mockContext = new ServerContext {
      override def getHost(): String = "test-host"
      override def getPort(): Int = 8080
      override def getProtocolScheme(): String = "dftp"
      override def getDftpHome(): Option[String] = None
    }

    // 2. Init Module
    module.init(mockAnchor, mockContext)
    assertNotNull(mockAnchor.eventHandler, "EventHandler should be registered")
    assertNotNull(mockAnchor.eventSource, "EventSource should be registered")

    // 3. Capture the Workers[CatalogService] by firing the event source
    var workersHolder: Workers[CatalogService] = null
    val mockEventHub = new EventHub {
      override def fireEvent(event: CrossModuleEvent): Unit = {
        event match {
          case e: CollectCatalogServiceEvent => workersHolder = e.holder
          case _ =>
        }
      }
    }
    mockAnchor.eventSource.init(mockEventHub)
    assertNotNull(workersHolder, "Should have captured Workers holder from CollectCatalogServiceEvent")

    // 4. Register a Mock Worker into the holder
    // We only need to implement methods used by the action we are testing (GET_SERVER_INFO uses formatter directly, so minimal worker needed)
    val mockWorker = new CatalogService {
      override def accepts(request: CatalogServiceRequest): Boolean = true
      // Other methods can be empty/null as we are testing GET_SERVER_INFO which doesn't call worker methods in this specific module logic
      override def listDataSetNames(): List[String] = Nil
      override def getDataSetMetaData(id: String, m: org.apache.jena.rdf.model.Model): Unit = {}
      override def getDataFrameMetaData(n: String, m: org.apache.jena.rdf.model.Model): Unit = {}
      override def listDataFrameNames(id: String): List[String] = Nil
      override def getDocument(n: String): link.rdcn.struct.DataFrameDocument = null
      override def getStatistics(n: String): link.rdcn.struct.DataFrameStatistics = null
      override def getSchema(n: String): Option[link.rdcn.struct.StructType] = None
      override def getDataFrameTitle(n: String): Option[String] = None
    }
    workersHolder.add(mockWorker)

    // 5. Simulate CollectActionMethodEvent to get the ActionMethod
    val collectActionEvent = new CollectActionMethodEvent() {
      var capturedMethod: ActionMethod = _
      override def collect(method: ActionMethod): Unit = capturedMethod = method
    }
    mockAnchor.eventHandler.doHandleEvent(collectActionEvent)
    assertNotNull(collectActionEvent.capturedMethod, "ActionMethod should be collected")

    // 6. Test Request Dispatching (Case: GET_SERVER_INFO)
    val actionMethod = collectActionEvent.capturedMethod

    // Create a mock request for GET_SERVER_INFO
    val mockRequest = new DftpActionRequest {
      override def getActionName(): String = CatalogActionMethodType.GET_SERVER_INFO
      override def getParameterAsMap(): Map[String, Any] = Map.empty
      override def getRequestParameters(): link.rdcn.struct.Row = link.rdcn.struct.Row.empty
    }

    // Capture response
    var responseJson: String = null
    val mockResponse = new DftpActionResponse {
      override def sendData(data: Array[Byte]): Unit = {}
      override def sendError(code: Int, msg: String): Unit = {}
      override def sendJsonString(json: String): Unit = responseJson = json
      override def sendJsonObject(json: JSONObject): Unit = responseJson = json.toString
      override def sendDataFrame(dataFrame: link.rdcn.struct.DataFrame): Unit = {}
    }

    // Execute Action
    assertTrue(actionMethod.accepts(mockRequest), "ActionMethod should accept GET_SERVER_INFO")
    actionMethod.doAction(mockRequest, mockResponse)

    // Verify Result
    assertNotNull(responseJson, "Response should have been sent")
    val responseObj = new JSONObject(responseJson)
    assertTrue(responseObj.has("cpu.cores"), "Response for GET_SERVER_INFO should contain system info")
  }
}