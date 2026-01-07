package link.rdcn.dacp.cook

import link.rdcn.Logging
import link.rdcn.client.{DftpClient, UrlValidator}
import link.rdcn.dacp.cook.JobStatus.{COMPLETE, RUNNING}
import link.rdcn.dacp.optree._
import link.rdcn.dacp.recipe.ExecutionResult
import link.rdcn.operation.TransformOp
import link.rdcn.server._
import link.rdcn.server.module._
import link.rdcn.struct.{Blob, DataFrame, DataFrameMetaData, DefaultDataFrame, Row, StructType}
import link.rdcn.user.{Credentials, UserPrincipal}
import link.rdcn.util.DataUtils
import org.json.JSONObject

import java.time.format.DateTimeFormatter
import java.time.{ZoneId, ZonedDateTime}
import java.util.concurrent.atomic.AtomicLong
import scala.collection.concurrent.TrieMap

/**
 * @Author renhao
 * @Description:
 * @Data 2025/10/31 10:59
 * @Modified By:
 */
object CookActionMethodType {
  final val SUBMIT_FLOW = "SUBMIT_FLOW"
  final val SUBMIT_RECIPE = "SUBMIT_RECIPE"
  final val GET_JOB_STATUS = "GET_JOB_STATUS"
  final val GET_JOB_EXECUTE_RESULT = "GET_JOB_EXECUTE_RESULT"
  final val GET_JOB_EXECUTE_PROCESS = "GET_JOB_EXECUTE_PROCESS"

  private final val ALL: Set[String] = Set(
    SUBMIT_FLOW,
    SUBMIT_RECIPE,
    GET_JOB_STATUS,
    GET_JOB_EXECUTE_RESULT,
    GET_JOB_EXECUTE_PROCESS
  )

  def exists(action: String): Boolean =
    ALL.contains(action)
}

trait DacpCookStreamRequest extends DftpGetStreamRequest {
  def getTransformTree: TransformOp
}

class DacpCookModule extends DftpModule with Logging {

  private implicit var serverContext: ServerContext = _
  private val getMethods = new FilteredGetStreamMethods

  private val formatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmss")
  private val recipeCounter = new AtomicLong(0L)
  private val jobResultCache = TrieMap.empty[String, ExecutionResult]
  private val jobTransformOpsCache = TrieMap.empty[String, Seq[TransformOp]]

  override def init(anchor: Anchor, serverContext: ServerContext): Unit = {
    this.serverContext = serverContext

    def flowExecutionContext(userPrincipal: UserPrincipal, response: DftpResponse): FlowExecutionContext = new FlowExecutionContext {

      override def fairdHome: String = serverContext.getDftpHome().getOrElse("./")

      override def pythonHome: String = sys.env
        .getOrElse("PYTHON_HOME", throw new Exception("PYTHON_HOME environment variable is not set"))

      override def isAsyncEnabled(wrapper: TransformFunctionWrapper): Boolean = wrapper match {
        case r: RepositoryOperator => r.transformFunctionWrapper match {
          case _: FifoFileRepositoryBundle => true
          case _  => false
        }
        case _ => false
      }

      override def loadSourceDataFrame(dataFrameNameUrl: String): Option[DataFrame] = {
        var result: Option[DataFrame] = None
        val dftpGetStreamRequest = new DftpGetStreamRequest {
          override def getRequestPath(): String = UrlValidator.extractPath(dataFrameNameUrl)

          override def getRequestURL(): String = serverContext.baseUrl + getRequestPath()

          override def getUserPrincipal(): UserPrincipal = userPrincipal
        }

        val dftpGetStreamResponse = new DftpGetStreamResponse {
          override def sendDataFrame(dataFrame: DataFrame): Unit = result =  Some(dataFrame)

          override def sendBlob(blob: Blob): Unit = blob.offerStream[DataFrame](inputStream => {
            val stream: Iterator[Row] = DataUtils.chunkedIterator(inputStream)
              .map(bytes => Row.fromSeq(Seq(bytes)))
            val schema = StructType.blobStreamStructType
            DefaultDataFrame(schema, stream)
          })

          override def sendError(errorCode: Int, message: String): Unit = response.sendError(errorCode, message)
        }
        getMethods.handle(dftpGetStreamRequest, dftpGetStreamResponse)
        result
      }

      //TODO Repository config
      override def getRepositoryClient(): Option[OperatorRepository] = Some(new RepositoryClient("http://10.0.89.39", 8090))

      override def loadRemoteDataFrame(baseUrl: String, transformOp: TransformOp, credentials: Credentials): Option[DataFrame] = {
        val urlInfo = UrlValidator.extractBase(baseUrl)
          .getOrElse(throw new IllegalArgumentException(s"Invalid URL format $baseUrl"))
        val client = new Client(urlInfo._2, urlInfo._3)
        client.login(credentials)
        Some(client.getRemoteDataFrame(transformOp))
      }

      private class Client(host: String, port: Int, useTLS: Boolean = false) extends DftpClient(host, port, useTLS) {
        def getRemoteDataFrame(transformOp: TransformOp): DataFrame = {
          val dataFrameHandle = openDataFrame(transformOp)
          getTabular(dataFrameHandle)
        }
      }
    }

    anchor.hook(new EventHandler {
      override def accepts(event: CrossModuleEvent): Boolean = {
        event match {
          case r: CollectGetStreamMethodEvent => true
          case r: CollectActionMethodEvent => true
          case _ => false
        }
      }

      override def doHandleEvent(event: CrossModuleEvent): Unit = {
        event match {
          case r: CollectActionMethodEvent =>
            r.collect(new ActionMethod {

              override def accepts(request: DftpActionRequest): Boolean = {
                CookActionMethodType.exists(request.getActionName())
              }

              override def doAction(request: DftpActionRequest, response: DftpActionResponse): Unit = {
                val paramsJsonObject = request.getRequestParameters()
                request.getActionName() match {
                  case CookActionMethodType.SUBMIT_FLOW =>
                    val jobId = getRecipeId()
                    val transformOps: Seq[TransformOp] = TransformTree.fromFlowdJsonString(paramsJsonObject.toString)
                    val ctx = flowExecutionContext(request.getUserPrincipal(), response)
                    val dfs = transformOps.map(_.execute(ctx))
                    val executeResult = new ExecutionResult {
                      override def single(): DataFrame = dfs.head

                      override def get(name: String): DataFrame = dfs(name.toInt - 1)

                      override def map(): Map[String, DataFrame] = dfs.zipWithIndex.map {
                        case (dataFrame, id) => (id.toString, dataFrame)
                      }.toMap
                    }
                    jobResultCache.put(jobId, executeResult)
                    jobTransformOpsCache.put(jobId, transformOps)
                    response.sendJsonObject(new JSONObject().put("jobId", jobId))
                  case CookActionMethodType.SUBMIT_RECIPE =>
                    val transformOp = TransformTree.fromJsonObject(paramsJsonObject)
                    val dataframe = transformOp.execute(flowExecutionContext(request.getUserPrincipal(), response))
                    val dataFrameResponse = new DataFrameResponse {
                      override def getDataFrameMetaData: DataFrameMetaData = new DataFrameMetaData {
                        override def getDataFrameSchema: StructType = dataframe.schema
                      }

                      override def getDataFrame: DataFrame = dataframe
                    }
                    response.attachStream(dataFrameResponse)
                  case CookActionMethodType.GET_JOB_STATUS =>
                    val jobId = paramsJsonObject.optString("jobId", null)
                    if (jobId == null) {
                      response.sendError(400, "empty request require jobId")
                    }
                    val executionResult = jobResultCache.get(jobId)
                    if(executionResult.nonEmpty){
                      val df = executionResult.get.map().values.toList.find(df => df.mapIterator(_.hasNext))
                      if(df.nonEmpty) response.sendJsonObject(new JSONObject().put("status", RUNNING.name))
                      else response.sendJsonObject(new JSONObject().put("status", COMPLETE.name))
                    }else response.sendError(404, s"job $jobId not exist")
                  case CookActionMethodType.GET_JOB_EXECUTE_RESULT =>
                    val jobId = paramsJsonObject.optString("jobId", null)
                    if (jobId == null) {
                      response.sendError(400, "empty request require jobId")
                    }
                    val executionResult = jobResultCache.get(jobId)
                    if(executionResult.isEmpty) response.sendError(404, s"job $jobId not exist")
                    else {
                      val jo = new JSONObject()
                      executionResult.get.map().foreach(kv => {
                        val dataFrameJson = new JSONObject()
                        dataFrameJson.put("dataframeMetaData", new DataFrameMetaData {
                            override def getDataFrameSchema: StructType = kv._2.schema
                          }.toJson())
                        dataFrameJson.put("ticket", serverContext.registry(kv._2))
                        jo.put(kv._1, dataFrameJson)
                      })
                      response.sendJsonObject(jo)
                    }
                  case CookActionMethodType.GET_JOB_EXECUTE_PROCESS =>
                    val jobId = paramsJsonObject.optString("jobId", null)
                    if (jobId == null) {
                      response.sendError(400, "empty request require jobId")
                    }
                    val transformOps = jobTransformOpsCache.get(jobId)
                    if(transformOps.isEmpty) response.sendError(404, s"job $jobId not exist")
                    else{
                      val processSeq: Seq[Double] = transformOps.get.map(_.executionProgress).filter(_.nonEmpty).map(_.get)
                      if(processSeq.isEmpty) response.sendError(400, s"Unable to calculate ${transformOps.mkString(",")} progress")
                      else {
                        val process = processSeq.sum/processSeq.length
                        response.sendJsonObject(new JSONObject().put("process", process))
                      }
                    }
                  case _ => response.sendError(400, "")
                }
              }
            })
          case _ =>
        }
      }
    })

    anchor.hook(new EventSource {
      override def init(eventHub: EventHub): Unit = {
        eventHub.fireEvent(CollectGetStreamMethodEvent(getMethods))
      }
    })
  }

  private def getRecipeId(): String = {
    val timestamp = ZonedDateTime.now(ZoneId.of("UTC")).format(formatter)
    val seq = recipeCounter.incrementAndGet()
    s"job-${timestamp}-${seq}"
  }

  override def destroy(): Unit = {}
}
