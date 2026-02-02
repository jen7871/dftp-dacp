package link.rdcn.struct

import link.rdcn.message.DftpTicket.DftpTicket

/**
 * @Author renhao
 * @Description:
 * @Data 2026/1/30 14:05
 * @Modified By:
 */
trait BlobHandler {
  def getBlobSize: Long

  def getBlobType: String

  def getDataFrameTicket: DftpTicket
}
