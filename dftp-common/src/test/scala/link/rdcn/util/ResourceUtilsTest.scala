package link.rdcn.util

import org.junit.jupiter.api.Assertions.{assertEquals, assertThrows, assertTrue}
import org.junit.jupiter.api.{BeforeEach, Test}

import java.io.IOException
import scala.collection.mutable.ListBuffer

class ResourceUtilsTest {

  // List to track the order of close() calls
  private val closeOrderTracker = ListBuffer[String]()

  /**
   * A traceable AutoCloseable mock implementation for testing.
   *
   * @param id Identifier for the resource
   * @param throwOnClose If true, throws IOException when close() is called
   */
  class MockResource(val id: String, val throwOnClose: Boolean = false) extends AutoCloseable {
    var isClosed = false
    var actionCount = 0

    def performAction(): Unit = {
      if (isClosed) throw new IllegalStateException(s"Resource $id already closed")
      actionCount += 1
    }

    override def close(): Unit = {
      if (!isClosed) {
        isClosed = true
        closeOrderTracker.append(id)
        if (throwOnClose) {
          throw new IOException(s"Failed to close $id")
        }
      }
    }
  }

  @BeforeEach
  def setUp(): Unit = {
    // Clear tracker before each test
    closeOrderTracker.clear()
  }

  // --- Tests for using ---

  @Test
  def testUsing_Success(): Unit = {
    val res = new MockResource("res1")
    var blockExecuted = false

    val result = ResourceUtils.using(res) { r =>
      r.performAction()
      blockExecuted = true
      "success" // Return a value
    }

    assertTrue(blockExecuted, "Block should be executed")
    assertEquals(1, res.actionCount, "Method on resource should be called")
    assertEquals("success", result, "Should return result from the block")
    assertTrue(res.isClosed, "Resource should be closed after successful execution")
    assertEquals(List("res1"), closeOrderTracker.toList, "close() method should be called")
  }

  @Test
  def testUsing_ExceptionInBlock(): Unit = {
    val res = new MockResource("res-fail")
    val expectedExceptionMsg = "Error in block"

    val ex = assertThrows(classOf[RuntimeException], () => {
      ResourceUtils.using(res) { r =>
        r.performAction()
        throw new RuntimeException(expectedExceptionMsg)
      }
      ()
    }, "Exception in block should be thrown")

    assertEquals(expectedExceptionMsg, ex.getMessage, "Should throw original exception")
    assertEquals(1, res.actionCount, "Resource should be used before exception")
    assertTrue(res.isClosed, "Resource should be closed even if block fails")
    assertEquals(List("res-fail"), closeOrderTracker.toList, "close() method should be called")
  }

  @Test
  def testUsing_NullResource(): Unit = {
    // Note: f(resource) will be executed as f(null). If f tries to access r, it throws NPE.
    // The main purpose of 'using' is to ensure 'finally' block safety.

    // Test 1: Verify NPE if block accesses null
    assertThrows(classOf[NullPointerException], () => {
      ResourceUtils.using(null.asInstanceOf[MockResource]) { r =>
        r.performAction() // Throws NPE here
      }
      ()
    }, "Accessing r when it is null should throw NPE")

    // Test 2: If block handles null, 'finally' should safely ignore null resource
    ResourceUtils.using(null.asInstanceOf[MockResource]) { r =>
      // Do nothing with r
    }

    assertTrue(closeOrderTracker.isEmpty, "No resource created, so close should not be called")
  }

  // --- Tests for usingAll ---

  @Test
  def testUsingAll_SuccessAndReverseOrder(): Unit = {
    val res1 = new MockResource("res1")
    val res2 = new MockResource("res2")
    val res3 = new MockResource("res3")
    var blockExecuted = false

    ResourceUtils.usingAll(res1, res2, res3) {
      blockExecuted = true
    }

    assertTrue(blockExecuted, "Block should be executed")

    // Verify all resources are closed
    assertTrue(res1.isClosed, "res1 should be closed")
    assertTrue(res2.isClosed, "res2 should be closed")
    assertTrue(res3.isClosed, "res3 should be closed")

    // Key: Verify resources are closed in reverse order of declaration
    val expectedOrder = List("res3", "res2", "res1")
    assertEquals(expectedOrder, closeOrderTracker.toList, "Resources should be closed in reverse order")
  }

  @Test
  def testUsingAll_ExceptionInBlock(): Unit = {
    val resA = new MockResource("resA")
    val resB = new MockResource("resB")
    val expectedExceptionMsg = "All block failed"

    val ex = assertThrows(classOf[RuntimeException], () => {
      ResourceUtils.usingAll(resA, resB) {
        throw new RuntimeException(expectedExceptionMsg)
      }
      ()
    }, "Exception in block should be thrown")

    assertEquals(expectedExceptionMsg, ex.getMessage, "Should throw original exception")

    // All resources should still be closed
    assertTrue(resA.isClosed, "resA should be closed")
    assertTrue(resB.isClosed, "resB should be closed")

    val expectedOrder = List("resB", "resA")
    assertEquals(expectedOrder, closeOrderTracker.toList, "Resources should be closed in reverse order")
  }

  @Test
  def testUsingAll_ExceptionInClose(): Unit = {
    val res1 = new MockResource("res1")
    val res2_fails = new MockResource("res2_fails", throwOnClose = true)
    val res3 = new MockResource("res3")

    // 'usingAll' should suppress exception from res2_fails and continue closing res1
    ResourceUtils.usingAll(res1, res2_fails, res3) {
      // Block succeeds
    }

    // Verify all resources *attempted* to close
    assertTrue(res1.isClosed, "res1 should be closed")
    assertTrue(res2_fails.isClosed, "res2_fails should be marked as closed")
    assertTrue(res3.isClosed, "res3 should be closed")

    // Verify order (res3, res2_fails, res1)
    val expectedOrder = List("res3", "res2_fails", "res1")
    assertEquals(expectedOrder, closeOrderTracker.toList, "Should attempt to close all resources in reverse order even if one fails")
  }

  @Test
  def testUsingAll_ExceptionInBlockAndClose(): Unit = {
    val res1 = new MockResource("res1")
    val res2_fails = new MockResource("res2_fails", throwOnClose = true)
    val expectedExceptionMsg = "Block failed first"

    // When both block and close throw exceptions, block exception should be propagated
    val ex = assertThrows(classOf[RuntimeException], () => {
      ResourceUtils.usingAll(res1, res2_fails) {
        throw new RuntimeException(expectedExceptionMsg)
      }
      ()
    }, "Exception in block should be thrown")

    assertEquals(expectedExceptionMsg, ex.getMessage, "Should throw original block exception")

    // Verify close still attempted
    assertTrue(res1.isClosed, "res1 should be closed")
    assertTrue(res2_fails.isClosed, "res2_fails should be closed")

    val expectedOrder = List("res2_fails", "res1")
    assertEquals(expectedOrder, closeOrderTracker.toList, "Close still executes in reverse order")
  }

  @Test
  def testUsingAll_WithNulls(): Unit = {
    val res1 = new MockResource("res1")
    val res3 = new MockResource("res3")

    // Should not throw NullPointerException
    ResourceUtils.usingAll(res1, null, res3) {
      // Block succeeds
    }

    assertTrue(res1.isClosed, "res1 should be closed")
    assertTrue(res3.isClosed, "res3 should be closed")

    // Verify order (res3, res1)
    val expectedOrder = List("res3", "res1")
    assertEquals(expectedOrder, closeOrderTracker.toList, "Null resources should be safely skipped")
  }
}