/**
 * @Author Yomi
 * @Description:
 * @Data 2025/9/26 10:52
 * @Modified By:
 */
package link.rdcn.struct

import org.junit.jupiter.api.Assertions.{assertEquals, assertThrows, assertTrue}
import org.junit.jupiter.api.Test
import java.util.concurrent.atomic.AtomicInteger

class CloseTracker {
  val count = new AtomicInteger(0)
  val callback: () => Unit = () => count.incrementAndGet()
}

class ClosableIteratorTest {
  private val data = List(1, 2, 3)

  @Test
  def testExplicitClose_Success(): Unit = {
    val tracker = new CloseTracker()
    val iterator = ClosableIterator(data.iterator, tracker.callback, false)

    iterator.close()

    assertEquals(1, tracker.count.get(), "Explicit close should trigger onClose callback once")
    assertTrue(!iterator.hasNext, "Iterator internal state should be marked as closed")
  }

  @Test
  def testExplicitClose_OnlyTriggersOnce(): Unit = {
    val tracker = new CloseTracker()
    val iterator = ClosableIterator(data.iterator, tracker.callback, false)

    // First close
    iterator.close()
    // Second close (should be prevented)
    iterator.close()

    assertEquals(1, tracker.count.get(), "Explicit close should only trigger onClose callback once")
  }

  @Test
  def testExplicitClose_HandlesExceptionInOnClose(): Unit = {
    // Cover exception in onClose
    val exceptionCallback: () => Unit = () => throw new RuntimeException("Cleanup failed")
    val iterator = ClosableIterator(data.iterator, exceptionCallback, false)

    // Expect wrapper exception
    val exception = assertThrows(classOf[Exception], () => {
      iterator.close()
    })

    assertTrue(!iterator.hasNext, "Iterator should be marked closed even if onClose throws exception")
    assertEquals("[ClosableIterator] Error during close: Cleanup failed", exception.getMessage, "Exception message should contain the original error")
  }

  @Test
  def testAutoClose_OnHasNextCompletion(): Unit = {
    val tracker = new CloseTracker()
    val iterator = ClosableIterator(data.iterator, tracker.callback, false)

    // Consume all elements
    iterator.next()
    iterator.next()
    iterator.next()

    // Trigger hasNext on empty iterator
    val hasMore = iterator.hasNext

    assertTrue(!hasMore, "hasNext should return false after consuming all elements")
    assertEquals(1, tracker.count.get(), "onClose should be triggered when hasNext detects depletion")
  }

  @Test
  def testAutoClose_OnNextCompletion(): Unit = {
    val tracker = new CloseTracker()
    val iterator = ClosableIterator(List(1).iterator, tracker.callback, false)

    // Consume single element
    iterator.next()

    assertEquals(1, tracker.count.get(), "onClose should be triggered when next() consumes the last element")
    assertTrue(!iterator.hasNext, "Iterator should be closed after last element consumed")

    // Attempt next() again
    val exception = assertThrows(classOf[NoSuchElementException], () => {
      iterator.next()
    })
    assertEquals("next on empty iterator", exception.getMessage, "Should throw NoSuchElementException after close")
  }

  @Test
  def testNoAutoClose_IfExplicitlyClosedDuringIteration(): Unit = {
    val tracker = new CloseTracker()
    val iter = List(1, 2).iterator
    val iterator = ClosableIterator(iter, tracker.callback, false)

    iterator.next()
    iterator.close() // Explicit close

    // Call next() again, should fail but onClose should not trigger again
    assertThrows(classOf[NoSuchElementException], () => {
      iterator.next()
    })

    assertEquals(1, tracker.count.get(), "Explicit close should prevent auto-close from triggering twice")
  }

  @Test
  def testFileListMode_NoAutoClose(): Unit = {
    val tracker = new CloseTracker()
    val iterator = ClosableIterator(data.iterator, tracker.callback, true) // isFileList = true

    // Consume all elements
    while (iterator.hasNext) {
      iterator.next()
    }

    assertEquals(0, tracker.count.get(), "onClose should NOT be triggered automatically in FileList mode")
    // Explicit close
    iterator.close()
    assertEquals(1, tracker.count.get(), "Explicit close should still work in FileList mode")
  }

  @Test
  def testNextOnEmptyIterator_FileListMode(): Unit = {
    val tracker = new CloseTracker()
    val emptyIterator = ClosableIterator(List.empty[Int].iterator, tracker.callback, true)

    // In FileList mode, calling next() on empty iterator should act like standard iterator (throw exception)
    // and NOT trigger close automatically

    assertTrue(!emptyIterator.hasNext, "hasNext should be false for empty iterator")

    assertThrows(classOf[NoSuchElementException], () => {
      emptyIterator.next()
    })

    // Verify onClose not triggered
    assertEquals(0, tracker.count.get(), "onClose should not be triggered during next() in FileList mode")
  }
}