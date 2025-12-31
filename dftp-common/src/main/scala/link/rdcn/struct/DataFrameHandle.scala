package link.rdcn.struct

import link.rdcn.message.DftpTicket
import link.rdcn.message.DftpTicket.DftpTicket

/**
 * @Author renhao
 * @Description:
 * @Data 2025/12/25 17:08
 * @Modified By:
 */
trait DataFrameHandle {

  def getDataFrameMeta: DataFrameMetaData

  def getDataFrameTicket: DftpTicket

}
