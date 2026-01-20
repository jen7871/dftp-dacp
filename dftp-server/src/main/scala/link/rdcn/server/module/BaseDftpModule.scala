package link.rdcn.server.module

import link.rdcn.Logging
import link.rdcn.client.UrlValidator
import link.rdcn.message.ActionMethodType
import link.rdcn.operation.{ExecutionContext, TransformOp}
import link.rdcn.server._
import link.rdcn.struct.{Blob, DataFrame, DataFrameMetaData, StructType}
import link.rdcn.user.UserPrincipal

class BaseDftpModule extends DftpModule with Logging{

  private val getMethods = new FilteredGetStreamMethods
  private implicit var serverContext: ServerContext = _
  private val dftpBaseEventHandler = new EventHandler {

    override def accepts(event: CrossModuleEvent): Boolean = {
      event match {
        case _: CollectActionMethodEvent => true
        case _ => false
      }
    }

    override def doHandleEvent(event: CrossModuleEvent): Unit = {
      event match {
        case require: CollectActionMethodEvent =>
          require.collect(new ActionMethod {
            override def accepts(request: DftpActionRequest): Boolean = {
              request.getActionName() match {
                case ActionMethodType.GET => true
                case _ => false
              }
            }

            override def doAction(request: DftpActionRequest, response: DftpActionResponse): Unit = {
              request.getActionName() match {
                case ActionMethodType.GET =>
                  val requestJsonObject = request.getRequestParameters()
                  val transformOp: TransformOp = TransformOp.fromJsonObject(requestJsonObject)

                  val dataFrame = transformOp.execute(new ExecutionContext {
                    override def loadSourceDataFrame(dataFrameNameUrl: String): Option[DataFrame] = {
                      var result: Option[DataFrame] = None
                      val dftpGetStreamRequest = new DftpGetStreamRequest {
                        override def getRequestPath(): String = UrlValidator.extractPath(dataFrameNameUrl)

                        override def getRequestURL(): String = serverContext.baseUrl + getRequestPath()

                        override def getUserPrincipal(): UserPrincipal = request.getUserPrincipal()
                      }

                      val dftpGetStreamResponse = new DftpGetStreamResponse {
                        override def sendDataFrame(dataFrame: DataFrame): Unit = result =  Some(dataFrame)

                        override def sendBlob(blob: Blob): Unit = response.attachStream(new BlobResponse {
                          override def getBlob: Blob = blob
                        })

                        override def sendError(errorCode: Int, message: String): Unit = response.sendError(errorCode, message)
                      }
                      getStream(dftpGetStreamRequest, dftpGetStreamResponse)
                      result
                    }
                  })
                  val dataFrameResponse: DataFrameResponse = new DataFrameResponse {
                    override def getDataFrameMetaData: DataFrameMetaData =
                      new DataFrameMetaData {
                        override def getDataFrameSchema: StructType = dataFrame.schema
                      }

                    override def getDataFrame: DataFrame = dataFrame
                  }
                  response.attachStream(dataFrameResponse)
              }
            }

          })
        case _ =>
      }
    }
  }

  private def getStream(request: DftpGetStreamRequest, response: DftpGetStreamResponse): Unit = {
    getMethods.handle(request, response)
  }

  override def init(anchor: Anchor, serverContext: ServerContext): Unit = {
    this.serverContext = serverContext

    anchor.hook(dftpBaseEventHandler)
    anchor.hook(new EventSource {
      override def init(eventHub: EventHub): Unit =
        eventHub.fireEvent(CollectGetStreamMethodEvent(getMethods))
    })
  }

  override def destroy(): Unit = {
  }
}

trait DataFrameProviderService {
  def accepts(dataFrameUrl: String): Boolean

  def getDataFrame(dataFrameUrl: String, userPrincipal: UserPrincipal)(implicit ctx: ServerContext): DataFrame
}

case class CollectDataFrameProviderEvent(holder: Workers[DataFrameProviderService]) extends CrossModuleEvent {
  def collect(dataFrameProviderService: DataFrameProviderService): Unit = holder.add(dataFrameProviderService)
}

