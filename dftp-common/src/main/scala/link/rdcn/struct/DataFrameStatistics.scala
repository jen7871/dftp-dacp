package link.rdcn.struct

import org.json.JSONObject

/**
 * @Author Yomi
 * @Description:
 * @Data 2025/7/22 13:49
 * @Modified By:
 */
trait DataFrameStatistics extends Serializable {
  def rowCount: Long

  def byteSize: Long

  final def toJson(): JSONObject = {
    new JSONObject().put("rowCount", rowCount)
      .put("byteSize", byteSize)
  }
}

object DataFrameStatistics {

  def empty(): DataFrameStatistics =
    new DataFrameStatistics {
      override def rowCount: Long = -1L

      override def byteSize: Long = -1L
    }

  def fromJson(jsonObject: JSONObject): DataFrameStatistics = {
    new DataFrameStatistics {
      override def rowCount: Long = jsonObject.getLong("rowCount")

      override def byteSize: Long = jsonObject.getLong("byteSize")
    }
  }

}
