package link.rdcn.recipe

import link.rdcn.dacp.recipe.FlowScheduler
import org.json.JSONObject
import org.junit.jupiter.api.Assertions.{assertEquals, assertThrows, assertTrue}
import org.junit.jupiter.api.Test

class FlowSchedulerTest {

  @Test
  def testScheduleFlowDistribution(): Unit = {
    val inputJson =
      """
        |{
        |  "flow": {
        |    "paths": [
        |      {
        |        "from": "sourceCsv",
        |        "to": "gully"
        |      },
        |      {
        |        "from": "sourceTif",
        |        "to": "gully"
        |      },
        |            {
        |        "from": "sourceGEO",
        |        "to": "node-fileRepositoryHydro"
        |      },
        |      {
        |        "from": "gully",
        |        "to": "node-fileRepositoryHydro"
        |      },
        |      {
        |        "from": "node-fileRepositoryHydro",
        |        "to": "fileRepositorySelect"
        |      },
        |      {
        |        "from": "sourceTfwDir",
        |        "to": "fileRepositoryGeoTransMain"
        |      },
        |      {
        |        "from": "sourceLabelsDir",
        |        "to": "fileRepositoryGeoTransMain"
        |      },
        |      {
        |        "from": "fileRepositoryGeoTransMain",
        |        "to": "fileRepositorySelect"
        |      }
        |    ],
        |    "stops": [
        |      {
        |        "id": "sourceCsv",
        |        "type": "SourceNode",
        |        "properties": {
        |          "path": "dacp://10.0.82.143:3103/2019年中国榆林市沟道信息.csv"
        |        }
        |      },
        |      {
        |        "id": "sourceTif",
        |        "type": "SourceNode",
        |        "properties": {
        |          "path": "dacp://10.0.82.147:3103/2019年中国榆林市30m数字高程数据集.tif"
        |        }
        |      },
        |      {
        |        "id": "sourceGEO",
        |        "type": "SourceNode",
        |        "properties": {
        |          "path": "dacp://10.0.82.147:3103/geo_entropy.csv"
        |        }
        |      },
        |      {
        |        "id": "sourceLabelsDir",
        |        "type": "SourceNode",
        |        "properties": {
        |          "path": "dacp://10.0.82.210:3103/op2/labels"
        |        }
        |      },
        |      {
        |        "id": "sourceTfwDir",
        |        "type": "SourceNode",
        |        "properties": {
        |          "path": "dacp://10.0.82.210:3103/op2/tfw"
        |        }
        |      },
        |      {
        |        "id": "gully",
        |        "type": "RepositoryNode",
        |        "properties": {
        |          "name": "gully_slop",
        |          "version" : "0.5.0-20251115-1"
        |        }
        |      },
        |      {
        |        "id": "node-fileRepositoryHydro",
        |        "type": "RepositoryNode",
        |        "properties": {
        |          "name": "hydro_susceptibility",
        |          "version" : "0.5.0-20251115-1"
        |        }
        |      },
        |      {
        |        "id": "fileRepositoryGeoTransMain",
        |        "type": "RepositoryNode",
        |        "properties": {
        |          "name": "geotrans",
        |          "version" : "0.5.0-20251115-2"
        |        }
        |      },
        |      {
        |        "id": "fileRepositorySelect",
        |        "type": "RepositoryNode",
        |        "properties": {
        |          "name": "overlap_dam_select",
        |          "version" : "0.5.0-20251115-1"
        |        }
        |      }
        |    ]
        |  }
        |}
        |""".stripMargin

    // 执行优化逻辑
    val resultJsonStr = FlowScheduler.schedule(inputJson)
    val resultJson = new JSONObject(resultJsonStr)
    println(resultJson)
    val flow = resultJson.getJSONObject("flow")
    val stops = flow.getJSONArray("stops")
    val paths = flow.getJSONArray("paths")
    import java.io.{File, PrintWriter}
    val outputPath: String = "output.json"
    val writer = new PrintWriter(new File(outputPath))
    try {
      // 写入内容
      writer.write(resultJsonStr)
    } finally {
      // 务必关闭流以确保内容被刷新到磁盘
      writer.close()
    }

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

    assertTrue(remoteNodesCount >= 2, "应该至少有两个远程节点被折叠")
    assertTrue(repositoryNodesCount > 0, "主节点算子应该被保留")
  }

  @Test
  def testEmptyFlow(): Unit = {
    val ex = assertThrows(classOf[Exception], () => {
      FlowScheduler.schedule("{}")
      ()
    }, "空JSON应该导致异常")
  }
}