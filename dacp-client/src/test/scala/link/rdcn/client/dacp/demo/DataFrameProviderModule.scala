package link.rdcn.client.dacp.demo

import link.rdcn.server._
import link.rdcn.server.module.{CollectDataFrameProviderEvent, CollectGetStreamMethodEvent, DataFrameProviderService, GetStreamMethod}
import link.rdcn.struct.DataFrame
import link.rdcn.user.UserPrincipal

/**
 * @Author renhao
 * @Description:
 * @Data 2025/10/31 18:43
 * @Modified By:
 */
class DataFrameProviderModule(dataFrameProvider: DataFrameProviderService) extends DftpModule {

  override def init(anchor: Anchor, serverContext: ServerContext): Unit = {
    anchor.hook(new EventHandler {
      override def accepts(event: CrossModuleEvent): Boolean =
        event.isInstanceOf[CollectGetStreamMethodEvent]

      override def doHandleEvent(event: CrossModuleEvent): Unit = {
        event match {
          case r: CollectGetStreamMethodEvent =>
            r.collect(new GetStreamMethod {

              override def accepts(request: DftpGetStreamRequest): Boolean =
                dataFrameProvider.accepts(request.getRequestURL())

              override def doGetStream(request: DftpGetStreamRequest, response: DftpGetStreamResponse): Unit = {
                val df = dataFrameProvider.getDataFrame(request.getRequestURL(), request.getUserPrincipal())(serverContext)
                response.sendDataFrame(df)
              }
            })
        }
      }
    })
  }

  override def destroy(): Unit = {}
}