package link.rdcn.recipe

import link.rdcn.dacp.recipe.FlowScheduler
import org.json.JSONObject
import org.junit.jupiter.api.Assertions.{assertThrows, assertTrue}
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir

import java.io.{File, PrintWriter}

class FlowSchedulerTest {

  @TempDir
  var tempDir: File = _

  @Test
  def testScheduleFlowDistribution(): Unit = {
    val inputJson =
      """
        |{
        |  "flow": {
        |    "paths": [
        |      { "from": "sourceCsv", "to": "gully" },
        |      { "from": "sourceTif", "to": "gully" },
        |      { "from": "sourceGEO", "to": "node-fileRepositoryHydro" },
        |      { "from": "gully", "to": "node-fileRepositoryHydro" },
        |      { "from": "node-fileRepositoryHydro", "to": "fileRepositorySelect" },
        |      { "from": "sourceTfwDir", "to": "fileRepositoryGeoTransMain" },
        |      { "from": "sourceLabelsDir", "to": "fileRepositoryGeoTransMain" },
        |      { "from": "fileRepositoryGeoTransMain", "to": "fileRepositorySelect" }
        |    ],
        |    "stops": [
        |      { "id": "sourceCsv", "type": "SourceNode", "properties": { "path": "dacp://10.0.82.148:3103/data.csv" } },
        |      { "id": "sourceTif", "type": "SourceNode", "properties": { "path": "dacp://10.0.82.147:3103/data.tif" } },
        |      { "id": "sourceGEO", "type": "SourceNode", "properties": { "path": "dacp://10.0.82.147:3103/geo.csv" } },
        |      { "id": "sourceLabelsDir", "type": "SourceNode", "properties": { "path": "dacp://10.0.82.210:3103/labels" } },
        |      { "id": "sourceTfwDir", "type": "SourceNode", "properties": { "path": "dacp://10.0.82.210:3103/tfw" } },
        |      { "id": "gully", "type": "RepositoryNode", "properties": { "name": "gully_slop", "version" : "0.5.0" } },
        |      { "id": "node-fileRepositoryHydro", "type": "RepositoryNode", "properties": { "name": "hydro", "version" : "0.5.0" } },
        |      { "id": "fileRepositoryGeoTransMain", "type": "RepositoryNode", "properties": { "name": "geotrans", "version" : "0.5.0" } },
        |      { "id": "fileRepositorySelect", "type": "RepositoryNode", "properties": { "name": "select", "version" : "0.5.0" } }
        |    ]
        |  }
        |}
        |""".stripMargin

    // Execute logic
    val resultJsonStr = FlowScheduler.schedule(inputJson)
    val resultJson = new JSONObject(resultJsonStr)

    val flow = resultJson.getJSONObject("flow")
    val stops = flow.getJSONArray("stops")

    // Write to file (using temp dir for safety)
    val outputFile = new File(tempDir, "output.json")
    val writer = new PrintWriter(outputFile)
    try {
      writer.write(resultJsonStr)
    } finally {
      writer.close()
    }

    // Count node types
    var remoteNodesCount = 0
    var repositoryNodesCount = 0

    for (i <- 0 until stops.length()) {
      val stop = stops.getJSONObject(i)
      if (stop.getString("type") == "RemoteDataFrameFlowNode") {
        remoteNodesCount += 1
        val props = stop.getJSONObject("properties")
        assertTrue(props.has("flow"), "Remote node must contain 'flow' property")
      } else {
        repositoryNodesCount += 1
      }
    }

    assertTrue(remoteNodesCount >= 2, "Should have at least two remote nodes collapsed")
    assertTrue(repositoryNodesCount > 0, "Main repository nodes should be preserved")
  }

  @Test
  def testEmptyFlow(): Unit = {
    assertThrows(classOf[Exception], () => {
      FlowScheduler.schedule("{}")
      ()
    }, "Empty JSON should throw exception")
  }
}