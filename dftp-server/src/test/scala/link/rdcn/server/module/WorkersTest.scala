package link.rdcn.server.module

import link.rdcn.operation.TransformOp
import link.rdcn.server._
import link.rdcn.struct.{Blob, DataFrame, DefaultDataFrame, StructType}
import link.rdcn.user.UserPrincipal
import org.json.JSONObject
import org.junit.jupiter.api.Assertions.{assertEquals, assertNotNull}
import org.junit.jupiter.api.Test

class WorkersTest {

  // --- Local Mocks ---

  class MockGetStreamRequest(path: String) extends DftpGetStreamRequest {
    override def getRequestPath(): String = path
    override def getRequestURL(): String = s"dftp://mock$path"
    override def getUserPrincipal(): UserPrincipal = null
  }

  class MockGetStreamResponse extends DftpGetStreamResponse {
    var dataFrame: DataFrame = _
    var errorCode: Int = 200

    override def sendDataFrame(df: DataFrame): Unit = this.dataFrame = df
    override def sendBlob(blob: Blob): Unit = {}
    override def sendError(code: Int, message: String): Unit = this.errorCode = code
  }

  // --- Tests ---

  @Test
  def testWorkersWorkLogic(): Unit = {
    val workers = new Workers[Int]()

    val task = new TaskRunner[Int, String] {
      override def acceptedBy(worker: Int): Boolean = worker > 10
      override def executeWith(worker: Int): String = s"Worker $worker"
      override def handleFailure(): String = "Failed"
    }

    assertEquals("Failed", workers.work(task), "Should fail with no workers")

    workers.add(5) // Too small
    assertEquals("Failed", workers.work(task), "Should fail if worker rejects")

    workers.add(15) // Good
    assertEquals("Worker 15", workers.work(task), "Should execute with accepted worker")
  }

  @Test
  def testFilteredGetStreamMethods(): Unit = {
    val methods = new FilteredGetStreamMethods()

    // Add a handler
    methods.addMethod(new GetStreamMethod {
      override def accepts(request: DftpGetStreamRequest): Boolean = request.getRequestPath().equals("/target")
      override def doGetStream(request: DftpGetStreamRequest, response: DftpGetStreamResponse): Unit = {
        response.sendDataFrame(DefaultDataFrame(StructType.empty, Iterator.empty))
      }
    })

    // Test Match
    val reqMatch = new MockGetStreamRequest("/target")
    val resMatch = new MockGetStreamResponse()
    methods.handle(reqMatch, resMatch)
    assertNotNull(resMatch.dataFrame, "Handler should be executed")

    // Test No Match
    val reqMiss = new MockGetStreamRequest("/other")
    val resMiss = new MockGetStreamResponse()
    methods.handle(reqMiss, resMiss)
    assertEquals(404, resMiss.errorCode, "Should return 404 when no handler accepts")
  }
}