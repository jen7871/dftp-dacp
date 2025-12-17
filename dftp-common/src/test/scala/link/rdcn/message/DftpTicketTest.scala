/**
 * @Author Yomi
 * @Description:
 * @Data 2025/9/26 10:51
 * @Modified By:
 */
package link.rdcn.message

import org.junit.jupiter.api.Assertions.{assertArrayEquals, assertEquals}
import org.junit.jupiter.api.Test

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

class DftpTicketJunitTest {

  private val BLOB_TICKET_ID: Byte = 1
  private val GET_TICKET_ID: Byte = 2

  @Test
  def testBlobTicketEncode(): Unit = {
    val blobId = "blob-uuid-12345"
    val blobTicket = BlobTicket(blobId)

    // Cover BlobTicket instantiation and encodeTicket()
    val encoded = blobTicket.encodeTicket()

    val contentBytes = blobId.getBytes(StandardCharsets.UTF_8)
    val expectedLength = 1 + 4 + contentBytes.length

    assertEquals(expectedLength, encoded.length, "Encoded array length must match (typeId + length + content)")

    // Verify encoded content
    val buffer = ByteBuffer.wrap(encoded)

    assertEquals(BLOB_TICKET_ID, buffer.get(), "First byte must be the BlobTicket typeId (1)")
    assertEquals(contentBytes.length, buffer.getInt(), "Next 4 bytes must be the content length")

    val decodedContent = new Array[Byte](contentBytes.length)
    buffer.get(decodedContent)

    assertArrayEquals(contentBytes, decodedContent, "Decoded content bytes must match original blobId")
  }

  @Test
  def testBlobTicketWithEmptyContent(): Unit = {
    val blobId = ""
    val blobTicket = BlobTicket(blobId)

    val encoded = blobTicket.encodeTicket()

    // Verify length and content
    val buffer = ByteBuffer.wrap(encoded)
    assertEquals(BLOB_TICKET_ID, buffer.get(), "Type ID must be 1")
    assertEquals(0, buffer.getInt(), "Content length must be 0 for empty content")

    assertEquals(buffer.remaining(), 0, "Buffer should be exhausted")
  }

  @Test
  def testGetTicketEncode(): Unit = {
    val url = "dftp://server/query?op=filter"
    val getTicket = GetTicket(url)

    // Cover GetTicket instantiation and encodeTicket()
    val encoded = getTicket.encodeTicket()

    val contentBytes = url.getBytes(StandardCharsets.UTF_8)

    // Verify encoded content
    val buffer = ByteBuffer.wrap(encoded)

    assertEquals(GET_TICKET_ID, buffer.get(), "First byte must be the GetTicket typeId (2)")
    assertEquals(contentBytes.length, buffer.getInt(), "Next 4 bytes must be the content length")

    val decodedContent = new Array[Byte](contentBytes.length)
    buffer.get(decodedContent)

    assertArrayEquals(contentBytes, decodedContent, "Decoded content bytes must match original URL")
  }
}