package link.rdcn.struct

import org.json.JSONObject

/**
 * @Author renhao
 * @Description:
 * @Data 2025/7/10 16:21
 * @Modified By:
 */
trait DataFrameDocument extends Serializable {
  def getSchemaURL(): Option[String]

  def getDataFrameTitle(): Option[String]

  def getColumnURL(colName: String): Option[String]

  def getColumnAlias(colName: String): Option[String]

  def getColumnTitle(colName: String): Option[String]

  final def toJson(schema: StructType): JSONObject = {
    val resultJsonObject = new JSONObject()
    val columnsJsonObject = new JSONObject()
    schema.columns.map(_.name).map(col => {
      val jo = new JSONObject()
      jo.put("columnUrl", getColumnURL(col).orNull)
      jo.put("columnAlias", getColumnAlias(col).orNull)
      jo.put("columnTitle", getColumnTitle(col).orNull)
      columnsJsonObject.put(col, jo)
    })
    resultJsonObject.put("schemaUrl", getSchemaURL().orNull)
      .put("dataframeTitle", getDataFrameTitle().orNull)
      .put("columnMetaData", columnsJsonObject)
    resultJsonObject
  }
}

object DataFrameDocument{

  def empty(): DataFrameDocument = {
    new DataFrameDocument {
      override def getSchemaURL(): Option[String] = None

      override def getDataFrameTitle(): Option[String] = None

      override def getColumnURL(colName: String): Option[String] = None

      override def getColumnAlias(colName: String): Option[String] = None

      override def getColumnTitle(colName: String): Option[String] = None
    }
  }

  def fromJson(jsonObject: JSONObject): DataFrameDocument = {
    val colJsonObject = jsonObject.getJSONObject("columnMetaData")

    new DataFrameDocument {
      override def getSchemaURL(): Option[String] = Some(jsonObject.getString("columnUrl"))

      override def getDataFrameTitle(): Option[String] = Some(jsonObject.getString("dataframeTitle"))

      override def getColumnURL(colName: String): Option[String] =
        Some(colJsonObject.getJSONObject(colName).getString("columnUrl"))

      override def getColumnAlias(colName: String): Option[String] =
        Some(colJsonObject.getJSONObject(colName).getString("columnAlias"))

      override def getColumnTitle(colName: String): Option[String] =
        Some(colJsonObject.getJSONObject(colName).getString("columnTitle"))
    }

  }

}
