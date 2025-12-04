package link.rdcn.recipe

import link.rdcn.dacp.recipe.FlowScheduler
import org.json.JSONObject
import org.junit.jupiter.api.Assertions.{assertEquals, assertThrows, assertTrue}
import org.junit.jupiter.api.Test

class FlowSchedulerTest {

  @Test
  def testOptimizeFlowDistribution(): Unit = {
    val inputJson =
      """
        |{
        |  "flow": {
        |    "paths": [
        |      { "from": "sourceCsv", "to": "gully" },
        |      { "from": "sourceTif", "to": "gully" },
        |      { "from": "sourceGEO", "to": "fileRepositoryHydro" },
        |      { "from": "gully", "to": "fileRepositoryHydro" },
        |      { "from": "fileRepositoryHydro", "to": "fileRepositorySelect" },
        |      { "from": "sourceTfwDir", "to": "fileRepositoryGeoTransMain" },
        |      { "from": "sourceLabelsDir", "to": "fileRepositoryGeoTransMain" },
        |      { "from": "fileRepositoryGeoTransMain", "to": "fileRepositorySelect" }
        |    ],
        |    "stops": [
        |      { "name": "sourceCsv", "type": "SourceNode", "properties": { "path": "dacp://10.0.82.143:3103/file.csv" } },
        |      { "name": "sourceTif", "type": "SourceNode", "properties": { "path": "dacp://10.0.82.147:3103/file.tif" } },
        |      { "name": "sourceGEO", "type": "SourceNode", "properties": { "path": "dacp://10.0.82.147/geo.csv" } },
        |      { "name": "sourceLabelsDir", "type": "SourceNode", "properties": { "path": "dacp://10.0.82.210:3103/labels" } },
        |      { "name": "sourceTfwDir", "type": "SourceNode", "properties": { "path": "dacp://10.0.82.210:3103/tfw" } },
        |      { "name": "gully", "type": "RepositoryNode", "properties": { "name": "gully_slop" } },
        |      { "name": "fileRepositoryHydro", "type": "RepositoryNode", "properties": { "name": "hydro" } },
        |      { "name": "fileRepositoryGeoTransMain", "type": "RepositoryNode", "properties": { "name": "geotrans" } },
        |      { "name": "fileRepositorySelect", "type": "RepositoryNode", "properties": { "name": "select" } }
        |    ]
        |  }
        |}
        |""".stripMargin

    // 执行优化逻辑
    val resultJsonStr = FlowScheduler.optimize(inputJson)
    val resultJson = new JSONObject(resultJsonStr)
    println(resultJson)
    val flow = resultJson.getJSONObject("flow")
    val stops = flow.getJSONArray("stops")
    val paths = flow.getJSONArray("paths")

    // 统计节点类型
    var remoteNodesCount = 0
    var repositoryNodesCount = 0

    for (i <- 0 until stops.length()) {
      val stop = stops.getJSONObject(i)
      if (stop.getString("type") == "RemoteDataFrameFlowNode") {
        remoteNodesCount += 1
        // 验证 Remote 节点必须包含 flow 属性
        val props = stop.getJSONObject("properties")
        assertTrue(props.has("flow"), "Remote节点必须包含flow属性")
      } else {
        repositoryNodesCount += 1
      }
    }

    // 断言验证
    // 逻辑预期：应该有多个 Remote 节点被折叠，主节点算子被保留
    // 注意：assertTrue 也是变量(条件)在前，信息在后
    assertTrue(remoteNodesCount >= 2, "应该至少有两个远程节点被折叠")
    assertTrue(repositoryNodesCount > 0, "主节点算子应该被保留")
  }

  @Test
  def testEmptyFlow(): Unit = {
    val ex = assertThrows(classOf[Exception], () => {
      FlowScheduler.optimize("{}")
      ()
    }, "空JSON应该导致异常")
  }
}