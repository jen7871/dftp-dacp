package link.rdcn.struct

import org.json.JSONObject

import scala.collection.JavaConverters.asScalaSetConverter

/**
 * @Author renhao
 * @Description:
 * @Data 2025/12/24 15:43
 * @Modified By:
 */
trait DataFrameMetaData {

  def getDataFrameSchema: StructType

  def getDataFrameDocument: DataFrameDocument = DataFrameDocument.empty()

  def getDataFrameStatistic: DataFrameStatistics = DataFrameStatistics.empty()

  final def toJson(): JSONObject = {
    val schema = getDataFrameSchema
    val documentJson = getDataFrameDocument.toJson(schema)
    val statisticsJson = getDataFrameStatistic.toJson()
    documentJson.put("schema", schema.toJson().getJSONArray("schema"))
    val statisticsJsonKeys = statisticsJson.keySet().asScala
    statisticsJsonKeys.foreach(key => documentJson.put(key, statisticsJson.get(key)))
    documentJson
  }
}

object DataFrameMetaData {

  def fromJson(jsonObject: JSONObject): DataFrameMetaData = {

    new DataFrameMetaData {
      override def getDataFrameSchema: StructType = StructType.fromJson(jsonObject)

      override def getDataFrameDocument: DataFrameDocument = DataFrameDocument.fromJson(jsonObject)

      override def getDataFrameStatistic: DataFrameStatistics = DataFrameStatistics.fromJson(jsonObject)
    }
  }

}
