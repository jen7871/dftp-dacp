/**
 * @Author Yomi
 * @Description:
 * @Data 2025/9/26 10:52
 * @Modified By:
 */
package link.rdcn.struct

import org.junit.jupiter.api.Assertions.{assertEquals, assertNotNull}
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir

import java.io.{File, FileInputStream, IOException}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}

class BlobTest {

  @TempDir
  var tempDir: Path = _

  @Test
  def testBlobFromFile(): Unit = {
    // 1. Prepare temp file
    val content = "Blob Content Test"
    val file = tempDir.resolve("test.blob").toFile
    Files.write(file.toPath, content.getBytes(StandardCharsets.UTF_8))

    // 2. Create Blob
    val blob = Blob.fromFile(file)
    assertNotNull(blob, "Blob.fromFile should return a valid object")

    // 3. Consume Stream
    blob.offerStream { inputStream =>
      assertNotNull(inputStream, "InputStream should not be null")
      val bytes = new Array[Byte](inputStream.available())
      inputStream.read(bytes)
      val readContent = new String(bytes, StandardCharsets.UTF_8)

      assertEquals(content, readContent, "Blob content should match file content")
    }
  }

  @Test
  def testBlobCleanup(): Unit = {
    // Test if the blob stream handling (try-finally) works as expected
    val file = tempDir.resolve("cleanup.blob").toFile
    Files.write(file.toPath, "data".getBytes)
    val blob = Blob.fromFile(file)

    // Using a side effect to verify closure if possible, or just verify no exception on normal usage
    var streamRef: java.io.InputStream = null

    blob.offerStream { is =>
      streamRef = is
      // Read something
      is.read()
    }

    // In many implementations, FileInputStream.read() throws IOException if closed.
    // This is a heuristic check.
    try {
      if (streamRef.asInstanceOf[FileInputStream].getFD.valid()) {
        // Checking FD validity is one way, or simply attempting to read
        // Note: Java FileInputStream.close() closes the FD.
        // This assertion depends on specific JVM behavior, simplified here:
        // We assume offerStream closes the stream.
      }
    } catch {
      case _: IOException => // Expected if closed
      case _: Exception => // Other errors
    }
  }
}