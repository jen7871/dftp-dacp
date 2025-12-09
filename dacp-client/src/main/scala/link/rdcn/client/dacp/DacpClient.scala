package link.rdcn.client

import link.rdcn.dacp.optree._
import link.rdcn.dacp.recipe._
import link.rdcn.message.DftpTicket
import link.rdcn.operation._
import link.rdcn.struct._
import link.rdcn.user.{AnonymousCredentials, Credentials, UsernamePassword}
import org.apache.arrow.flight.Ticket
import org.apache.jena.rdf.model.{Model, ModelFactory}
import org.json.{JSONArray, JSONObject}

import java.io.{File, StringReader}
import scala.collection.JavaConverters.{asJavaCollectionConverter, asScalaIteratorConverter}
import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, Map => MMap}

/**
 * @Author renhao
 * @Description:
 * @Data 2025/10/29 20:12
 * @Modified By:
 */
class DacpClient(host: String, port: Int, useTLS: Boolean = false) extends DftpClient(host, port, useTLS) {

  private val dacpUrlPrefix: String = s"dacp://$host:$port"

  def listDataSetNames(): Seq[String] = {
    val result = ArrayBuffer[String]()
    get(dacpUrlPrefix + "/listDataSets").mapIterator(rows => rows.foreach(row => {
      result+=(row.getAs[String](0))
    }))
    result
  }

  def listDataFrameNames(dsName: String): Seq[String] = {
    val result = ArrayBuffer[String]()
    get(dacpUrlPrefix+ s"/listDataFrames/$dsName")
      .foreach(row => result.append(row.getAs[String](0)))
    result
  }

  def getDataSetMetaData(dsName: String): Model = {
    val rdfString = new String(doAction(s"getDataSetMetaData", Map("dataSetName"-> dsName)), "UTF-8").trim
    getModelByString(rdfString)
  }

  def getDataFrameMetaData(dfName: String): Model = {
    val rdfString = new String(doAction(s"getDataFrameMetaData", Map("dataFrameName" -> dfName)), "UTF-8").trim
    getModelByString(rdfString)
  }

  private def getModelByString(rdfString: String): Model = {
    val model = ModelFactory.createDefaultModel()
    rdfString match {
      case s if s.nonEmpty =>
        val reader = new StringReader(s)
        model.read(reader, null, "RDF/XML")
      case _ =>
    }
    model
  }

  def getSchema(dataFrameName: String): StructType = {
    val structTypeStr = new String(doAction(s"getSchema", Map("dataFrameName" -> dataFrameName)), "UTF-8")
    StructType.fromString(structTypeStr)
  }

  def getDataFrameTitle(dataFrameName: String): String = {
    val jsonString = new String(doAction(s"getDataFrameInfo", Map("dataFrameName" -> dataFrameName)), "UTF-8")
    val jo = new JSONObject(jsonString)
    jo.getString("title")
  }

  def getDocument(dataFrameName: String): DataFrameDocument = {

    new String(doAction(s"getDocument", Map("dataFrameName" -> dataFrameName)), "UTF-8").trim match {
      case s if s.nonEmpty =>
        val jo = new JSONArray(s).getJSONObject(0)
        new DataFrameDocument {
          override def getSchemaURL(): Option[String] = Some("SchemaUrl")

          override def getDataFrameTitle(): Option[String] = Some(jo.getString("DataFrameTitle"))

          override def getColumnURL(colName: String): Option[String] = Some(jo.getString("ColumnUrl"))

          override def getColumnAlias(colName: String): Option[String] = Some(jo.getString("ColumnAlias"))

          override def getColumnTitle(colName: String): Option[String] = Some(jo.getString("ColumnTitle"))
        }
      case _ => DataFrameDocument.empty()
    }

  }

  def getStatistics(dataFrameName: String): DataFrameStatistics = {
    val jsonString: String = {
      new String(doAction(s"getDataFrameInfo", Map("dataFrameName" -> dataFrameName)), "UTF-8").trim match {
        case s if s.isEmpty => ""
        case s => s
      }
    }
    val jo = new JSONObject(jsonString)
    new DataFrameStatistics {
      override def rowCount: Long = jo.getLong("rowCount")

      override def byteSize: Long = jo.getLong("byteSize")
    }
  }

  def getHostInfo: Map[String, String] = {
    val jo = new JSONObject(new String(doAction("getHostInfo"), "UTF-8").trim)
    jo.keys().asScala.map { key =>
      key -> jo.getString(key)
    }.toMap
  }

  def getServerResourceInfo: Map[String, String] = {
    val jo = new JSONObject(new String(doAction("getServerInfo"), "UTF-8").trim)
    jo.keys().asScala.map { key =>
      key -> jo.getString(key)
    }.toMap
  }

  def executeTransformTree(transformOp: TransformOp): DataFrame = {
    val schemaAndRow = getCookRows(transformOp.toJsonString)
    DefaultDataFrame(schemaAndRow._1, schemaAndRow._2)
  }

  def cook(recipe: Flow): ExecutionResult = {
    val executePaths: Seq[FlowPath] = recipe.getExecutionPaths()
    val ops = transformFlowToOperation(executePaths.head)
    val dfs: Seq[DataFrame] = executePaths.map(path => RemoteDataFrameProxy(transformFlowToOperation(path), getCookRows))
    new ExecutionResult() {
      override def single(): DataFrame = dfs.head

      override def get(name: String): DataFrame = dfs(name.toInt - 1)

      override def map(): Map[String, DataFrame] = dfs.zipWithIndex.map {
        case (dataFrame, id) => (id.toString, dataFrame)
      }.toMap
    }
  }

  def cook(recipeString: String): ExecutionResult = {
    val transformOp = transformFlowJsonToOperation(recipeString)
    val dfs: Seq[DataFrame] = Seq(RemoteDataFrameProxy(transformOp, getCookRows))
    new ExecutionResult() {
      override def single(): DataFrame = dfs.head

      override def get(name: String): DataFrame = dfs(name.toInt - 1)

      override def map(): Map[String, DataFrame] = dfs.zipWithIndex.map {
        case (dataFrame, id) => (id.toString, dataFrame)
      }.toMap
    }
  }

  private def transformFlowToOperation(path: FlowPath): TransformOp = {
    path.node match {
      case f: Transformer11 =>
        val genericFunctionCall = DataFrameCall11(new SerializableFunction[DataFrame, DataFrame] {
          override def apply(v1: DataFrame): DataFrame = f.transform(v1)
        })
        val transformerNode: TransformerNode = TransformerNode(TransformFunctionWrapper.getJavaSerialized(genericFunctionCall),
          transformFlowToOperation(path.children.head))
        transformerNode
      case f: Transformer21 =>
        val genericFunctionCall = DataFrameCall21(new SerializableFunction[(DataFrame, DataFrame), DataFrame] {
          override def apply(v1: (DataFrame, DataFrame)): DataFrame = f.transform(v1._1, v1._2)
        })
        val leftInput = transformFlowToOperation(path.children.head)
        val rightInput = transformFlowToOperation(path.children.last)
        val transformerNode: TransformerNode = TransformerNode(TransformFunctionWrapper.getJavaSerialized(genericFunctionCall), leftInput, rightInput)
        transformerNode
      case node: RepositoryNode =>
        val jo = new JSONObject()
        jo.put("type", LangTypeV2.REPOSITORY_OPERATOR.name)
        jo.put("functionName", node.functionName)
        jo.put("functionVersion", node.functionVersion.orNull)
        val transformerNode: TransformerNode = TransformerNode(
          TransformFunctionWrapper.fromJsonObject(jo).asInstanceOf[RepositoryOperator],
          path.children.map(transformFlowToOperation(_)): _*)
        transformerNode
      case FifoFileBundleFlowNode(command, inputFilePath, outputFilePath, dockerContainer) =>
        val jo = new JSONObject()
        jo.put("type", LangTypeV2.FILE_REPOSITORY_BUNDLE.name)
        jo.put("command", new JSONArray(command.asJavaCollection))
        jo.put("inputFilePath", new JSONArray(inputFilePath.map(file => new JSONObject().put("filePath", file._1)
        .put("fileType", file._2)).asJavaCollection))
        jo.put("outputFilePath", new JSONArray(outputFilePath.map(file => new JSONObject().put("filePath", file._1)
          .put("fileType", file._2)).asJavaCollection))
        jo.put("dockerContainer", dockerContainer.toJson())
        val transformerNode: TransformerNode = TransformerNode(
          TransformFunctionWrapper.fromJsonObject(jo).asInstanceOf[FileRepositoryBundle],
          path.children.map(transformFlowToOperation(_)): _* )
        transformerNode
      case FifoFileFlowNode() => FiFoFileNode(path.children.map(transformFlowToOperation(_)): _*)
      case RemoteDataFrameFlowNode(baseUrl, flow, certificate) =>
        RemoteSourceProxyOp(baseUrl, transformFlowToOperation(flow.getExecutionPaths().head), certificate)
      case s: SourceNode => SourceOp(s.dataFrameName)
      case other => throw new IllegalArgumentException(s"This FlowNode ${other} is not supported please extend Transformer11 trait")
    }
  }

  private def transformFlowJsonToOperation(flowString: String): TransformOp = {
    val rootJson = new JSONObject(flowString)
    val flowJson = if (rootJson.has("flow")) rootJson.getJSONObject("flow") else rootJson

    val stopsArray = flowJson.getJSONArray("stops")
    val pathsArray = flowJson.getJSONArray("paths")

    val nodesMap = MMap[String, JSONObject]()
    for (i <- 0 until stopsArray.length()) {
      val stop = stopsArray.getJSONObject(i)
      nodesMap(stop.getString("id")) = stop
    }

    val incomingEdges = MMap[String, mutable.Buffer[(String, Int)]]()
    val allSourceIds = mutable.Set[String]()

    for (i <- 0 until pathsArray.length()) {
      val path = pathsArray.getJSONObject(i)
      val from = path.getString("from")
      val to = path.getString("to")

      val inportStr = path.optString("inport", "0")
      val inport = try {
        inportStr.toInt
      } catch {
        case _: NumberFormatException => 0
      }

      incomingEdges.getOrElseUpdate(to, mutable.Buffer()) += ((from, inport))
      allSourceIds.add(from)
    }

    val allIds = nodesMap.keys.toSet
    val sinkIds = allIds -- allSourceIds

    if (sinkIds.isEmpty) throw new IllegalArgumentException("Invalid Flow: Cyclic graph or empty (No sink node found).")

    val rootId = sinkIds.head

    def recursiveBuild(currentId: String): TransformOp = {
      val nodeJson = nodesMap(currentId)
      val nodeType = nodeJson.getString("type")
      val properties = nodeJson.optJSONObject("properties", new JSONObject())

      val edges = incomingEdges.getOrElse(currentId, mutable.Buffer.empty)

      val sortedInputIds = edges.sortBy(_._2).map(_._1)

      val inputOps = sortedInputIds.map(recursiveBuild).toSeq

      nodeType match {
        case "SourceNode" =>
          val path = if (properties.has("dataFrameName")) properties.getString("dataFrameName")
          else properties.optString("path", "")
          SourceOp(path)

        case "RepositoryNode" =>
          val jo = new JSONObject()
          jo.put("type", LangTypeV2.REPOSITORY_OPERATOR.name)
          jo.put("functionName", properties.get("name").asInstanceOf[String])
          jo.put("functionVersion", properties.get("version").asInstanceOf[String])

          TransformerNode(
            TransformFunctionWrapper.fromJsonObject(jo).asInstanceOf[RepositoryOperator],
            inputOps: _*
          )

        case "RemoteDataFrameFlowNode" =>
          RemoteSourceProxyOp(
            properties.get("baseUrl").asInstanceOf[String],
            transformFlowJsonToOperation(new JSONObject().put("flow", new JSONObject(properties.get("flow").asInstanceOf[String])).toString),
            properties.get("certificate").asInstanceOf[String]
          )

        case other => throw new IllegalArgumentException(s"Unknown FlowNode type: $other at id: $currentId")
      }
    }

    recursiveBuild(rootId)
  }

  def getCookRows(transformOpStr: String): (StructType, ClosableIterator[Row]) = {
    val schemaAndIter = getStream(new Ticket(CookTicket(transformOpStr).encodeTicket()))
    val stream = schemaAndIter._2.map(seq => Row.fromSeq(seq))
    (schemaAndIter._1, ClosableIterator(stream)())
  }
  private case class CookTicket(ticketContent: String) extends DftpTicket {
    override val typeId: Byte = 3
  }
}

object DacpClient {
  val protocolSchema = "dacp"
  private val urlValidator = UrlValidator(protocolSchema)

  def connect(url: String, credentials: Credentials = null, useUnifiedLogin: Boolean = false): DacpClient = {
    urlValidator.validate(url) match {
      case Right(parsed) =>
        val client = new DacpClient(parsed._1, parsed._2.getOrElse(3101))
        if(credentials != null) {
          if(useUnifiedLogin){
            credentials match {
              case AnonymousCredentials => client.login(credentials)
              case c: UsernamePassword => client.login(OdcAuthClient.requestAccessToken(c))
              case _ => throw new IllegalArgumentException(s"the $credentials is not supported")
            }
          }else client.login(credentials)
        }
        client
      case Left(err) =>
        throw new IllegalArgumentException(s"Invalid DACP URL: $err")
    }
  }

  def connectTLS(url: String, file: File, credentials: Credentials = null, useUnifiedLogin: Boolean = false): DacpClient = {
    System.setProperty("javax.net.ssl.trustStore", file.getAbsolutePath)
    urlValidator.validate(url) match {
      case Right(parsed) =>
        val client = new DacpClient(parsed._1, parsed._2.getOrElse(3101), true)
        if(credentials != null) {
          if(useUnifiedLogin){
            credentials match {
              case AnonymousCredentials => client.login(credentials)
              case c: UsernamePassword => client.login(OdcAuthClient.requestAccessToken(c))
              case _ => throw new IllegalArgumentException(s"the $credentials is not supported")
            }
          }else client.login(credentials)
        }
        client
      case Left(err) =>
        throw new IllegalArgumentException(s"Invalid DACP URL: $err")
    }
  }
}
