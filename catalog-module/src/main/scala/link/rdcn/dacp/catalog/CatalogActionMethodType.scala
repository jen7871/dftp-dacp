package link.rdcn.dacp.catalog

/**
 * @Author renhao
 * @Description:
 * @Data 2025/12/29 16:29
 * @Modified By:
 */
object CatalogActionType {
  final val GetDataSetMetaData  = "GET_DATASET_METADATA"
  final val GetDataFrameMetaData = "GET_DATAFRAME_METADATA"
  final val GetDocument         = "GET_DOCUMENT"
  final val GetDataFrameInfo    = "GET_DATAFRAME_INFO"
  final val GetSchema           = "GET_SCHEMA"
  final val GetHostInfo         = "GET_HOST_INFO"
  final val GetServerInfo       = "GET_SERVER_INFO"

  val all: Set[String] = Set(
    GetDataSetMetaData,
    GetDataFrameMetaData,
    GetDocument,
    GetDataFrameInfo,
    GetSchema,
    GetHostInfo,
    GetServerInfo
  )
}
