package link.rdcn.dacp.catalog

/**
 * @Author renhao
 * @Description:
 * @Data 2025/12/29 16:29
 * @Modified By:
 */
object CatalogActionMethodType {

  final val GET_DATASET_METADATA   = "GET_DATASET_METADATA"
  final val GET_DATAFRAME_METADATA = "GET_DATAFRAME_METADATA"
  final val GET_DOCUMENT           = "GET_DOCUMENT"
  final val GET_DATAFRAME_INFO     = "GET_DATAFRAME_INFO"
  final val GET_SCHEMA             = "GET_SCHEMA"
  final val GET_HOST_INFO          = "GET_HOST_INFO"
  final val GET_SERVER_INFO        = "GET_SERVER_INFO"

  private final val ALL: Set[String] = Set(
    GET_DATASET_METADATA,
    GET_DATAFRAME_METADATA,
    GET_DOCUMENT,
    GET_DATAFRAME_INFO,
    GET_SCHEMA,
    GET_HOST_INFO,
    GET_SERVER_INFO
  )

  def exists(action: String): Boolean =
    ALL.contains(action)
}

