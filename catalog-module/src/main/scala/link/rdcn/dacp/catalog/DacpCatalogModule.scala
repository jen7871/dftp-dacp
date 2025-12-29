package link.rdcn.dacp.catalog

import CatalogFormatter.{getDataFrameDocumentJsonObject, getHostInfo, getSystemInfo}
import link.rdcn.Logging
import link.rdcn.client.UrlValidator
import link.rdcn.server._
import link.rdcn.server.exception.DataFrameNotFoundException
import link.rdcn.server.module.{ActionMethod, CollectActionMethodEvent, CollectDataFrameProviderEvent, CollectGetStreamMethodEvent, DataFrameProviderService, GetStreamFilter, GetStreamFilterChain, GetStreamMethod, TaskRunner, Workers}
import link.rdcn.struct.{DataFrame, StructType}
import link.rdcn.user.UserPrincipal
import org.apache.jena.rdf.model.{Model, ModelFactory}
import org.json.JSONObject

import java.io.StringWriter

/**
 * @Author renhao
 * @Description:
 * @Data 2025/10/29 21:46
 * @Modified By:
 */
case class CollectCatalogServiceEvent(holder: Workers[CatalogService]) extends CrossModuleEvent

class DacpCatalogModule extends DftpModule with Logging {

  private val catalogServiceHolder = new Workers[CatalogService]

  override def init(anchor: Anchor, serverContext: ServerContext): Unit = {
    anchor.hook(new EventHandler {
      override def accepts(event: CrossModuleEvent): Boolean = {
        event match {
          case _: CollectActionMethodEvent => true
          case _: CollectDataFrameProviderEvent => true
          case _ => false
        }
      }

      override def doHandleEvent(event: CrossModuleEvent): Unit = {
        event match {
          case r: CollectActionMethodEvent => r.collect(new ActionMethod {

            override def accepts(request: DftpActionRequest): Boolean =
              CatalogActionType.all.contains(request.getActionName())

            override def doAction(request: DftpActionRequest, response: DftpActionResponse): Unit = {
              val actionName = request.getActionName()
              val parameter = request.getRequestParameters()
              catalogServiceHolder.work[Unit](new TaskRunner[CatalogService, Unit] {
                override def acceptedBy(worker: CatalogService): Boolean =
                  worker.accepts(new CatalogServiceRequest {
                    override def getDataSetId: String = parameter.optString("dataSetName", null)

                    override def getDataFrameUrl: String = parameter.optString("dataFrameName", null)
                  })

                override def executeWith(worker: CatalogService): Unit = {
                  actionName match {
                    case CatalogActionType.GetDataSetMetaData =>
                      val model: Model = ModelFactory.createDefaultModel
                      worker.getDataSetMetaData(parameter.get("dataSetName").toString, model)
                      val writer = new StringWriter();
                      model.write(writer, "RDF/XML");
                      response.sendJsonObject(new JSONObject().put("content", writer.toString))
                    case CatalogActionType.GetDataFrameMetaData =>
                      val model: Model = ModelFactory.createDefaultModel
                      worker.getDataFrameMetaData(parameter.get("dataFrameName").toString, model)
                      val writer = new StringWriter();
                      model.write(writer, "RDF/XML");
                      response.sendJsonObject(new JSONObject().put("content", writer.toString))
                    case CatalogActionType.GetDocument =>
                      val dataFrameName = parameter.get("dataFrameName").toString
                      val document = worker.getDocument(dataFrameName)
                      val schema = worker.getSchema(dataFrameName)
                      response.sendJsonObject(getDataFrameDocumentJsonObject(document, schema))
                    case CatalogActionType.GetDataFrameInfo =>
                      val dataFrameName = parameter.get("dataFrameName").toString
                      val dataFrameTitle = worker.getDataFrameTitle(dataFrameName).getOrElse(dataFrameName)
                      val statistics = worker.getStatistics(dataFrameName)
                      val jo = new JSONObject()
                      jo.put("byteSize", statistics.byteSize)
                      jo.put("rowCount", statistics.rowCount)
                      jo.put("title", dataFrameTitle)
                      response.sendJsonObject(jo)
                    case CatalogActionType.GetSchema =>
                      val dataFrameName = parameter.get("dataFrameName").toString
                      response.sendJsonObject(worker.getSchema(dataFrameName)
                        .getOrElse(StructType.empty)
                        .toJson())
                    case CatalogActionType.GetHostInfo => response.sendJsonObject(getHostInfo(serverContext))
                    case CatalogActionType.GetServerInfo => response.sendJsonObject(getSystemInfo())
                  }
                }
                override def handleFailure(): Unit =
                  response.sendError(404, s"unknown action: ${request.getActionName()}")
              })
            }
          })

          case r: CollectDataFrameProviderEvent =>
            r.holder.add(
              new DataFrameProviderService {
                override def accepts(dataFrameUrl: String): Boolean =
                  UrlValidator.extractPath(dataFrameUrl) match {
                    case "/listDataSets" => true
                    case path if path.startsWith("/dataset") => true
                    case _ => false
                  }

                override def getDataFrame(dataFrameUrl: String, userPrincipal: UserPrincipal)
                                         (implicit ctx: ServerContext): DataFrame = {
                  catalogServiceHolder.work(new TaskRunner[CatalogService, DataFrame] {

                    override def acceptedBy(worker: CatalogService): Boolean = true

                    override def executeWith(worker: CatalogService): DataFrame = {
                      UrlValidator.extractPath(dataFrameUrl) match {
                        case "/listDataSets" => worker.doListDataSets(serverContext.baseUrl)
                        case path if path.startsWith("/dataset") =>
                           worker.doListDataFrames(path, serverContext.baseUrl)
                      }
                    }

                    override def handleFailure(): DataFrame = throw new DataFrameNotFoundException(dataFrameUrl)
                  })
                }
              }
            )

          case _ =>
        }
      }
    })

    anchor.hook(new EventSource {
      override def init(eventHub: EventHub): Unit =
        eventHub.fireEvent(CollectCatalogServiceEvent(catalogServiceHolder))
    })
  }

  override def destroy(): Unit = {
  }
}