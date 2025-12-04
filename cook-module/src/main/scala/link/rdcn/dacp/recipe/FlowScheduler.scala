package link.rdcn.dacp.recipe

import org.json.{JSONArray, JSONObject}
import scala.collection.JavaConverters._
import scala.collection.mutable.{ArrayBuffer, Map => MMap, Set => MSet}

object FlowScheduler {

  // 定义简单的内部类方便处理，避免过度依赖 FlowBuilder 的具体实现细节
  case class NodeInfo(name: String, nodeType: String, props: Map[String, Any])
  case class Edge(from: String, to: String)

  /**
   * 核心入口：优化并重组 Flow JSON
   */
  def optimize(sourceJsonString: String): String = {
    // 1. 解析原始 JSON
    val root = new JSONObject(sourceJsonString)
    val flowJson = root.getJSONObject("flow")
    val stopsArray = flowJson.getJSONArray("stops")
    val pathsArray = flowJson.getJSONArray("paths")

    // 2. 构建图结构
    val nodes = MMap[String, NodeInfo]()
    val edges = ArrayBuffer[Edge]()
    // 上游映射: Child -> Parents
    val parentsMap = MMap[String, ArrayBuffer[String]]()

    for (i <- 0 until stopsArray.length()) {
      val stop = stopsArray.getJSONObject(i)
      val name = stop.getString("name")
      val nType = stop.getString("type")
      val propsJson = stop.optJSONObject("properties")
      val props = if (propsJson != null) jsonObjectToMap(propsJson) else Map.empty[String, Any]
      nodes(name) = NodeInfo(name, nType, props)
      parentsMap(name) = ArrayBuffer.empty
    }

    for (i <- 0 until pathsArray.length()) {
      val path = pathsArray.getJSONObject(i)
      val from = path.getString("from")
      val to = path.getString("to")
      edges += Edge(from, to)
      if (parentsMap.contains(to)) {
        parentsMap(to) += from
      }
    }

    // 3. 确定每个节点的归属 URL (Location)
    // 这里使用拓扑顺序遍历，确保处理某个节点时，其父节点位置已知
    val nodeLocations = determineNodeLocations(nodes.toMap, parentsMap.toMap)

    // 4. 选举主节点 (拥有最多算子的节点)
    val locationCounts = nodeLocations.values.groupBy(identity).mapValues(_.size)
    // 如果没有位置信息，默认空字符串
    val masterUrl = if (locationCounts.nonEmpty) locationCounts.maxBy(_._2)._1 else ""

    // 5. 构建输出 JSON
    val outputStops = new JSONArray()
    val outputPaths = new JSONArray()
    val processedLocations = MSet[String]()

    // 5.1 处理节点 (Stops)
    // 按位置分组
    val nodesByLocation = nodeLocations.groupBy(_._2)

    nodesByLocation.foreach { case (locUrl, nodeNames) =>
      if (locUrl == masterUrl) {
        // 主节点：保留原样
        nodeNames.keys.foreach { nodeName =>
          val originalNodeJson = stopsArray.asScala
            .map(_.asInstanceOf[JSONObject])
            .find(_.getString("name") == nodeName).get
          outputStops.put(originalNodeJson)
        }
      } else {
        // 从节点：聚合为一个 RemoteDataFrameFlowNode
        // 构建子工作流 (Sub-Flow)
        val subFlowJson = buildSubFlow(nodeNames.keys.toSet, nodes.toMap, edges)

        val remoteNode = new JSONObject()
        remoteNode.put("name", locUrl)
        remoteNode.put("type", "RemoteDataFrameFlowNode")

        val props = new JSONObject()
        props.put("baseUrl", locUrl)
        props.put("flow", subFlowJson) // 嵌入子 JSON
        props.put("certificate", "")

        remoteNode.put("properties", props)
        outputStops.put(remoteNode)
      }
    }

    // 5.2 处理边 (Paths)
    // 映射逻辑：节点名称 -> 如果在Master则保持名称，如果在Remote则变为RemoteUrl
    def getNodeRepresentative(nodeName: String): String = {
      val loc = nodeLocations.getOrElse(nodeName, "")
      if (loc == masterUrl) nodeName else loc
    }

    val uniqueNewEdges = MSet[Edge]()

    edges.foreach { edge =>
      val newFrom = getNodeRepresentative(edge.from)
      val newTo = getNodeRepresentative(edge.to)

      // 只有当边的两端不再是同一个对象时（即跨节点或Master内部连接），才添加到主图
      // 这里的逻辑：
      // 1. Master -> Master (保留)
      // 2. Master -> Remote (保留，变为 Node -> Url)
      // 3. Remote -> Master (保留，变为 Url -> Node)
      // 4. RemoteA -> RemoteB (保留，变为 UrlA -> UrlB)
      // 5. RemoteA -> RemoteA (内部边，剔除，因为它在 SubFlow 内部)

      if (newFrom != newTo) {
        uniqueNewEdges += Edge(newFrom, newTo)
      }
    }

    uniqueNewEdges.foreach { e =>
      val pathObj = new JSONObject()
      pathObj.put("from", e.from)
      pathObj.put("to", e.to)
      outputPaths.put(pathObj)
    }

    // 6. 组装最终结果
    val finalFlow = new JSONObject()
    finalFlow.put("stops", outputStops)
    finalFlow.put("paths", outputPaths)

    val finalRoot = new JSONObject()
    finalRoot.put("flow", finalFlow)

    finalRoot.toString(2) // 格式化输出
  }

  /**
   * 逻辑：推断每个节点的归属 URL
   */
  private def determineNodeLocations(nodes: Map[String, NodeInfo],
                                     parentsMap: Map[String, ArrayBuffer[String]]): Map[String, String] = {
    val locations = MMap[String, String]()

    // 简单的拓扑排序处理顺序 (DFS based)
    val visited = MSet[String]()
    val processingOrder = ArrayBuffer[String]()

    def dfs(node: String): Unit = {
      if (!visited.contains(node)) {
        visited += node
        parentsMap.getOrElse(node, ArrayBuffer()).foreach(dfs)
        processingOrder += node
      }
    }
    nodes.keys.foreach(dfs) // 此时 processingOrder 是拓扑序（先父后子）

    processingOrder.foreach { name =>
      val node = nodes(name)
      if (node.nodeType == "SourceNode") {
        // 规则：SourceNode 从 path 属性提取 URL
        val path = node.props.getOrElse("path", "").toString
        locations(name) = extractUrlPrefix(path)
      } else {
        // 规则：中间节点根据父节点推断
        val parents = parentsMap.getOrElse(name, ArrayBuffer())
        if (parents.isEmpty) {
          locations(name) = "unknown" // 孤立节点
        } else {
          val parentLocs = parents.flatMap(locations.get).toSet
          if (parentLocs.size == 1) {
            // 所有父节点在同一位置 -> 跟随
            locations(name) = parentLocs.head
          } else {
            // 父节点位置不同 -> 随机选择一个 (这里取第一个作为确定性随机)
            // 根据题目描述："从上游节点随机选择一个节点"
            locations(name) = parentLocs.head
          }
        }
      }
    }
    locations.toMap
  }

  /**
   * 辅助：从 dacp://ip:port/... 中提取 dacp://ip:port
   */
  private def extractUrlPrefix(path: String): String = {
    // 匹配 dacp:// + IP + (可选端口)
    val pattern = "(dacp://[0-9.]+(:[0-9]+)?)".r
    pattern.findFirstIn(path).getOrElse("unknown")
  }

  /**
   * 辅助：构建子工作流 JSON
   */
  private def buildSubFlow(subNodeNames: Set[String],
                           allNodes: Map[String, NodeInfo],
                           allEdges: ArrayBuffer[Edge]): JSONObject = {
    val stops = new JSONArray()
    val paths = new JSONArray()

    // 添加节点
    subNodeNames.foreach { name =>
      val node = allNodes(name)
      val stopObj = new JSONObject()
      stopObj.put("name", node.name)
      stopObj.put("type", node.nodeType)
      val props = new JSONObject()
      node.props.foreach { case (k, v) => props.put(k, v) }
      stopObj.put("properties", props)
      stops.put(stopObj)
    }

    // 添加内部边 (只有当 from 和 to 都在该子图内才添加)
    allEdges.foreach { edge =>
      if (subNodeNames.contains(edge.from) && subNodeNames.contains(edge.to)) {
        val pathObj = new JSONObject()
        pathObj.put("from", edge.from)
        pathObj.put("to", edge.to)
        paths.put(pathObj)
      }
    }

    val flow = new JSONObject()
    flow.put("stops", stops)
    flow.put("paths", paths)
    // 按照题目要求，flow 下面可能需要再包一层 flow?
    // 题目描述："RemoteDataFrameFlowNode中flow代表子工作流的flow，格式和原json一样"
    // 原 JSON 结构是 root -> flow -> {stops, paths}
    // 所以这里返回的应该是 { "stops": ..., "paths": ... } 这一层结构即可，
    // 因为外部把它放在 "flow" 字段下。

    // 如果需要严格完全一致的嵌套 (root -> flow)，请根据实际解析器调整。
    // 这里根据 "output.json" 样例，Remote 节点的 properties.flow 是一个对象，
    // 通常包含 stops 和 paths。
    flow
  }

  private def jsonObjectToMap(json: JSONObject): Map[String, Any] = {
    json.keys().asScala.map(k => k -> json.get(k)).toMap
  }
}
