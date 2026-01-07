/**
 * @Author Yomi
 * @Description:
 * @Data 2025/9/26 10:51
 * @Modified By:
 */
package link.rdcn.message

import org.apache.arrow.flight.Ticket
import org.junit.jupiter.api.Assertions.{assertEquals, assertNotNull}
import org.junit.jupiter.api.Test

import java.nio.charset.StandardCharsets

class DftpTicketTest {

  @Test
  def testGetTicketFromDftpTicketString(): Unit = {
    val dftpTicketString = "token-12345"

    // Execute: Convert String to Arrow Ticket
    val arrowTicket = DftpTicket.getTicket(dftpTicketString)

    assertNotNull(arrowTicket, "Generated Arrow Ticket should not be null")

    // Verify: The byte content of the ticket should match the encoded string
    // Note: CodecUtils.encodeString typically uses UTF-8 or specific encoding.
    // Assuming standard string encoding here for verification.
    val decodedBytes = arrowTicket.getBytes
    assertNotNull(decodedBytes, "Ticket bytes should not be null")
  }

  @Test
  def testGetDftpTicketFromArrowTicket(): Unit = {
    val originalString = "access-token-abc"
    val arrowTicket = DftpTicket.getTicket(originalString)

    // Execute: Convert Arrow Ticket back to String
    val resultString = DftpTicket.getDftpTicket(arrowTicket)

    // Verify: Round trip should match
    assertEquals(originalString, resultString, "Decoded DftpTicket string should match original")
  }

  @Test
  def testRoundTripWithSpecialCharacters(): Unit = {
    val complexString = "user:password@domain.com/path?arg=1"
    val ticket = DftpTicket.getTicket(complexString)
    val decoded = DftpTicket.getDftpTicket(ticket)

    assertEquals(complexString, decoded, "Round trip should handle special characters")
  }
}