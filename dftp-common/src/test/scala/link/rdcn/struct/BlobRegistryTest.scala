/**
 * @Author Yomi
 * @Description:
 * @Data 2025/9/26 10:50
 * @Modified By:
 */
package link.rdcn.struct

import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue, fail}
import org.junit.jupiter.api.{AfterAll, BeforeEach, Test}

import java.io.{ByteArrayInputStream, InputStream}
import java.nio.charset.StandardCharsets
import java.util.UUID

object BlobRegistryTest {

  val TEST_DATA = "Test data for stream".getBytes(StandardCharsets.UTF_8)
  val mockBlob = new Blob {
    override def offerStream[T](consume: InputStream => T): T = {
      val stream = new ByteArrayInputStream(TEST_DATA)
      consume(stream)
    }
  }

  @AfterAll
  def tearDown(): Unit = {
    // Ensure cleanup after tests
    BlobRegistry.cleanUp()
  }
}

class BlobRegistryTest {
  import BlobRegistryTest._

  private var testBlobId: String = _

  @BeforeEach
  def setUp(): Unit = {
    // Register a blob before each test
    testBlobId = BlobRegistry.register(mockBlob)
  }

  @Test
  def testRegisterAndGetBlobSuccess(): Unit = {
    // Cover register and getBlob success path
    val retrievedBlob = BlobRegistry.getBlob(testBlobId)

    assertTrue(retrievedBlob.isDefined, "Should successfully retrieve the registered Blob")
    assertTrue(retrievedBlob.get eq mockBlob, "Retrieved Blob should be the same instance (reference equality)")
  }

  @Test
  def testGetBlobNotFound(): Unit = {
    // Cover getBlob failure path
    val nonExistentId = UUID.randomUUID().toString
    val retrievedBlob = BlobRegistry.getBlob(nonExistentId)

    assertTrue(retrievedBlob.isEmpty, "Should return None for non-existent ID")
  }

  @Test
  def testGetStreamSuccess(): Unit = {
    // Define a consumer function
    val consumer: InputStream => String = stream => {
      val bytes = new Array[Byte](stream.available())
      stream.read(bytes)
      new String(bytes, StandardCharsets.UTF_8)
    }
    val result = BlobRegistry.getStream(testBlobId)(consumer)

    assertTrue(result.isDefined, "Should return Some(result) when Blob is found")
    assertEquals(new String(TEST_DATA, StandardCharsets.UTF_8), result.get, "Result from consumer must match original data")
  }

  @Test
  def testGetStreamNotFound(): Unit = {
    // Cover getStream failure path
    val nonExistentId = UUID.randomUUID().toString

    val consumer: InputStream => String = _ => {
      fail("Consumer should not be called when Blob is not found")
      "Should not be reached"
    }

    // Expect None
    val result = BlobRegistry.getStream(nonExistentId)(consumer)

    assertTrue(result.isEmpty, "Should return None when Blob is not found")
  }

  @Test
  def testCleanUp(): Unit = {
    // Verify cleanUp()
    BlobRegistry.cleanUp()
    val retrievedBlob = BlobRegistry.getBlob(testBlobId)

    assertTrue(retrievedBlob.isEmpty, "Registry should be empty after cleanUp()")
  }
}