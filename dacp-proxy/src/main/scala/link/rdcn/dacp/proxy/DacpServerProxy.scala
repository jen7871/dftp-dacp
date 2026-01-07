package link.rdcn.dacp.proxy

import link.rdcn.client.{DacpClient, UrlValidator}
import link.rdcn.dacp.cook.{CookActionMethodType, DacpCookModule, DacpCookStreamRequest}
import link.rdcn.operation.TransformOp
import link.rdcn.dacp.optree.TransformTree
import link.rdcn.dacp.recipe.ExecutionResult
import link.rdcn.message.ActionMethodType
import link.rdcn.server.module._
import link.rdcn.server._
import link.rdcn.struct.{BlobRegistry, DataFrame, DataFrameMetaData, DefaultDataFrame, Row, StructType}
import link.rdcn.user.{AuthenticationMethod, Credentials, UserPrincipal}
import link.rdcn.util.DataUtils
import org.json.JSONObject

import java.nio.charset.StandardCharsets
import java.util.concurrent.ConcurrentHashMap
import java.beans.BeanProperty

/**
 * @Author renhao
 * @Description:
 * @Data 2025/11/5 11:31
 * @Modified By:
 */
object ProxyActionMethodType {
  final val GET_TARGET_SERVER_URL = "GET_TARGET_SERVER_URL"
}

class ProxyModule extends DftpModule {

  @BeanProperty var targetServerUrl: String = _

  private val clientCache = new ConcurrentHashMap[Credentials, DacpClient]()

  private def getInternalClient(credentials: Credentials): DacpClient = {
    if(clientCache.contains(credentials)) clientCache.get(credentials)
    else {
      val client = DacpClient.connect(targetServerUrl, credentials)
      clientCache.put(credentials, client)
      client
    }
  }

  override def init(anchor: Anchor, serverContext: ServerContext): Unit = {
    anchor.hook(new EventHandler {
      override def accepts(event: CrossModuleEvent): Boolean =
        event match {
          case _: CollectAuthenticationMethodEvent => true
          case _: CollectActionMethodEvent => true
          case _: CollectPutStreamMethodEvent => true
          case _ => false
        }

      override def doHandleEvent(event: CrossModuleEvent): Unit = {
        event match {
          case r: CollectAuthenticationMethodEvent =>
            r.collect(new AuthenticationMethod {

              override def accepts(credentials: Credentials): Boolean = true

              override def authenticate(credentials: Credentials): UserPrincipal =
                ProxyUserPrincipal(credentials)
            })
          case r: CollectActionMethodEvent => r.collect(new ActionMethod {
            override def accepts(request: DftpActionRequest): Boolean = true

            override def doAction(request: DftpActionRequest, response: DftpActionResponse): Unit = {
              val internalClient = getInternalClient(request.getUserPrincipal()
                .asInstanceOf[ProxyUserPrincipal].credentials)
              val parameters: JSONObject = request.getRequestParameters()
              request.getActionName() match {
                case ProxyActionMethodType.GET_TARGET_SERVER_URL  =>
                  response.sendJsonObject(new JSONObject().put("targetServerUrl", targetServerUrl))
                case ActionMethodType.GET =>
                  val transformOp: TransformOp = TransformOp.fromJsonObject(parameters)
                  val dataFrame = internalClient.executeTransformTree(transformOp)
                  val dataFrameResponse = new DataFrameResponse {
                    override def getDataFrameMetaData: DataFrameMetaData = new DataFrameMetaData {
                      override def getDataFrameSchema: StructType = dataFrame.schema
                    }

                    override def getDataFrame: DataFrame = dataFrame
                  }
                  response.attachStream(dataFrameResponse)
                case CookActionMethodType.GET_JOB_EXECUTE_RESULT =>
                  val jobId = parameters.optString("jobId", null)
                  if (jobId == null) {
                    response.sendError(400, "empty request require jobId")
                  }
                  val executionResult: ExecutionResult = internalClient.getJobExecuteResult(jobId)
                  val jo = new JSONObject()
                  executionResult.map().foreach(kv => {
                    val dataFrameJson = new JSONObject()
                    dataFrameJson.put("dataframeMetaData", new DataFrameMetaData {
                      override def getDataFrameSchema: StructType = kv._2.schema
                    }.toJson())
                    dataFrameJson.put("ticket", serverContext.registry(kv._2))
                    jo.put(kv._1, dataFrameJson)
                  })
                  response.sendJsonObject(jo)
                case _ =>
                  try{
                    val actionResult = internalClient.doAction(request.getActionName(), parameters)
                    response.sendJsonString(actionResult.result)
                  }catch {
                    case e: Exception => response.sendError(500, e.getMessage)
                  }
              }
            }
          })
          case r: CollectPutStreamMethodEvent => r.collect(new PutStreamMethod {
            override def accepts(request: DftpPutStreamRequest): Boolean = true

            override def doPutStream(request: DftpPutStreamRequest, response: DftpPutStreamResponse): Unit = {
              val internalClient = getInternalClient(request.getUserPrincipal()
                .asInstanceOf[ProxyUserPrincipal].credentials)
              try{
                val iter =  request match {
                  case r: DftpPutBlobRequest =>
                   internalClient.put(r.getBlob(), r.getRequestParameters().toString)
                  case r: DftpPutDataFrameRequest =>
                    internalClient.put(r.getDataFrame(), r.getRequestParameters().toString)
                }
                while(iter.hasNext){
                  response.onNext(iter.next())
                }
                response.onCompleted()
              }catch {
                case e: Exception => response.sendError(500, e.getMessage)
              }
            }
          })
          case _ =>
        }
      }
    })
  }

  override def destroy(): Unit = {}
}

case class ProxyUserPrincipal(credentials: Credentials) extends UserPrincipal
