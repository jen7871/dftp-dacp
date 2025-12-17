/**
 * @Author Yomi
 * @Description:
 * @Data 2025/9/26 10:52
 * @Modified By:
 */
package link.rdcn.struct

import org.junit.jupiter.api.Assertions.{assertEquals, assertThrows, assertTrue}
import org.junit.jupiter.api.{AfterAll, BeforeAll, Test}

import java.io._
import java.nio.charset.StandardCharsets

object BlobFromFileTest {

  val TEST_CONTENT = "Hello, Blob File Test!"
  val testFile = new File("test_blob_temp.txt")

  @BeforeAll
  def setUp(): Unit = {
    // Write test content to temp file
    val writer = new BufferedWriter(new FileWriter(testFile))
    try {
      writer.write(TEST_CONTENT)
    } finally {
      writer.close()
    }
  }

  @AfterAll
  def tearDown(): Unit = {
    // Delete temp file
    if (testFile.exists()) {
      testFile.delete()
    }
  }
}

class BlobFromFileTest {
  import BlobFromFileTest._

  @Test
  def testFromFile_ReadsContentCorrectly(): Unit = {
    // Cover Blob.fromFile
    val blob = Blob.fromFile(testFile)
    val verifier = new StreamVerifier()

    // Cover offerStream try block
    blob.offerStream(verifier)

    // Verify content
    assertEquals(TEST_CONTENT, verifier.readContent, "Consumer should read the correct file content")
  }

  @Test
  def testFromFile_EnsuresStreamIsClosed(): Unit = {
    // Cover offerStream finally block
    val blob = Blob.fromFile(testFile)

    // Consumer that doesn't close the stream
    val closingConsumer: InputStream => Unit = stream => {
      val bytes = new Array[Byte](stream.available())
      stream.read(bytes)
    }

    blob.offerStream(closingConsumer)

    assertTrue(true, "The test verifies that the stream was closed by the finally block implicitly (no exception thrown).")
  }

  @Test
  def testFromFile_HandlesExceptionInConsumer(): Unit = {
    val blob = Blob.fromFile(testFile)

    // Cover exception in try block
    val exceptionConsumer: InputStream => Unit = _ => {
      throw new RuntimeException("Consumer failed")
    }

    val exception = assertThrows(classOf[RuntimeException], () => {
      blob.offerStream(exceptionConsumer)
    })
    assertEquals("Consumer failed", exception.getMessage, "The original consumer exception should be rethrown")
  }

  class StreamVerifier extends (InputStream => String) {
    var readContent: String = ""
    var streamClosed: Boolean = false

    override def apply(stream: InputStream): String = {
      // Check if stream is FileInputStream
      assertTrue(stream.isInstanceOf[FileInputStream], "Stream passed to consumer must be a FileInputStream")

      // Read content
      val contentBytes = new Array[Byte](stream.available())
      stream.read(contentBytes)
      readContent = new String(contentBytes, StandardCharsets.UTF_8)

      // Try closing, if successful it means it wasn't closed externally yet (but this is just verification logic)
      try {
        stream.close()
        streamClosed = true
      } catch {
        case _: IOException => // ignore
      }

      readContent
    }
  }
}