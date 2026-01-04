package link.rdcn.server.module

import link.rdcn.{Logging, server}
import link.rdcn.message.ActionMethodType
import link.rdcn.operation.{ExecutionContext, TransformOp}
import link.rdcn.server._
import link.rdcn.server.exception.{DataFrameNotFoundException, TicketExpiryException, TicketNotFoundException}
import link.rdcn.struct.{DataFrame, DataFrameMetaData, StructType}
import link.rdcn.user.UserPrincipal

class BaseDftpModule extends DftpModule with Logging{

  //TODO: should all data frame providers be registered?
  private val dataFrameHolder = new Workers[DataFrameProviderService]
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
                      Some(dataFrameHolder.work(new TaskRunner[DataFrameProviderService, DataFrame] {

                        override def acceptedBy(worker: DataFrameProviderService): Boolean = worker.accepts(dataFrameNameUrl)

                        override def executeWith(worker: DataFrameProviderService): DataFrame = worker.getDataFrame(dataFrameNameUrl, request.getUserPrincipal())

                        override def handleFailure(): DataFrame = throw new DataFrameNotFoundException(dataFrameNameUrl)
                      }))
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

  override def init(anchor: Anchor, serverContext: ServerContext): Unit = {
    this.serverContext = serverContext

    anchor.hook(dftpBaseEventHandler)
    anchor.hook(new EventSource {
      override def init(eventHub: EventHub): Unit =
        eventHub.fireEvent(CollectDataFrameProviderEvent(dataFrameHolder))
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
  def collect = holder.add(_)
}

