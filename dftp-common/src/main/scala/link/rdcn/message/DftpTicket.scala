package link.rdcn.message

import link.rdcn.util.CodecUtils
import org.apache.arrow.flight.Ticket

import java.nio.charset.StandardCharsets

/**
 * @Author renhao
 * @Description:
 * @Data 2025/9/19 16:54
 * @Modified By:
 */

object ActionMethodType {
  final val GET = "GET"
  final val PUT = "PUT"
}

object DftpTicket {
  type DftpTicket = String

  def getDftpTicket(ticket: Ticket): DftpTicket =
    CodecUtils.decodeString(ticket.getBytes)

  def getTicket(dftpTicket: DftpTicket): Ticket =
    new Ticket(CodecUtils.encodeString(dftpTicket))
}


