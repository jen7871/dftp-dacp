package link.rdcn.server

import link.rdcn.struct.{Blob, DataFrame, DataFrameMetaData}

/**
 * @Author renhao
 * @Description:
 * @Data 2025/12/27 13:11
 * @Modified By:
 */

trait DataFrameResource {

  def getDataFrameMetaData: DataFrameMetaData

  def getDataFrame: DataFrame

}
