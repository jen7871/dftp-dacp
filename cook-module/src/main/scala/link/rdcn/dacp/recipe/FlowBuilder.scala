/**
 * @Author Yomi
 * @Description:
 * @Data 2025/11/25 16:54
 * @Modified By:
 */
package link.rdcn.dacp.recipe

import org.json.JSONObject

import scala.collection.JavaConverters._
import scala.collection.mutable.{Map => MMap}
import scala.collection.immutable.HashMap

object FlowBuilder {
  /**
   * 将 org.json.JSONObject 的 properties 转换为 Map[String, String]
   */
  private def jsonObjectToMap(jsonObject: JSONObject): Map[String, String] = {
    jsonObject.keys().asScala.collect {
      case key: String =>
        key -> jsonObject.get(key).toString
    }.toMap
  }

  /**
   * 将 Stop 对象（JSONObject 形式）转换为具体的 FlowNode 对象
   */
  def stopToFlowNode(stopJson: JSONObject): FlowNode = {

    val nodeType = stopJson.getString("type")

    val properties = stopJson.optJSONObject("properties", new JSONObject())

    val stringProps = jsonObjectToMap(properties)


    nodeType match {
      case "SourceNode" =>
        SourceNode(dataFrameName = stringProps.getOrElse("path",""))

      case "RepositoryNode" =>
        RepositoryNode(
          stringProps.get("name").get,
          stringProps.get("version"),
          stringProps.get("id").get
        )

      case "RemoteDataFrameFlowNode" =>
        RemoteDataFrameFlowNode(
          stringProps.get("baseUrl").get,
          FlowBuilder.buildFlow(new JSONObject().put("flow",new JSONObject(stringProps.get("flow").get)).toString),
          stringProps.get("certificate").get// 包含 version 等其他属性
        )

      case other => throw new IllegalArgumentException(s"Unknown FlowNode type: $other")
    }
  }

  /**
   * 将 Source JSON 字符串转换为目标 Flow 中间类型
   */
  def buildFlow(sourceJsonString: String): Flow = {
    val root = new JSONObject(sourceJsonString)
    val flow = root.getJSONObject("flow")
    val stopsArray = flow.getJSONArray("stops")
    val pathsArray = flow.getJSONArray("paths")

    // 构建 Nodes Map (String -> FlowNode)
    val nodesMap: MMap[String, FlowNode] = MMap.empty

    for (i <- 0 until stopsArray.length()) {
      val stopJson = stopsArray.getJSONObject(i)
      val id = stopJson.getString("id")
      nodesMap(id) = stopToFlowNode(stopJson)
    }

    // 构建 Edges Map (String -> Seq[String])
    val edgesMap: MMap[String, Seq[String]] = MMap.empty

    for (i <- 0 until pathsArray.length()) {
      val pathJson = pathsArray.getJSONObject(i)
      val from = pathJson.getString("from")
      val to = pathJson.getString("to")

      val currentTargets = edgesMap.getOrElse(from, Seq.empty[String])
      edgesMap(from) = currentTargets :+ to
    }

    Flow(
      nodes = HashMap.empty[String, FlowNode] ++ nodesMap,
      edges = HashMap.empty[String, Seq[String]] ++ edgesMap
    )
  }

  /**
   * 完整的转换流程
   */
  def convert(sourceJsonString: String): Either[String, Flow] = {
    try {
      Right(buildFlow(sourceJsonString))
    } catch {
      case e: Exception =>
        Left(s"JSON error: ${e.getClass.getSimpleName} - ${e.getMessage}")
    }
  }
}
