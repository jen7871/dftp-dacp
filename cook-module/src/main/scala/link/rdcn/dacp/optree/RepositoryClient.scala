package link.rdcn.dacp.optree

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.stream.scaladsl.{FileIO, Source}
import akka.util.ByteString
import cn.cnic.operatordownload.client.OperatorClient
import link.rdcn.dacp.optree.fifo.FileType.FileType
import link.rdcn.dacp.optree.fifo.{DockerContainer, FileType}
import link.rdcn.dacp.recipe.FifoFileBundleFlowNode
import link.rdcn.dacp.utils.FileUtils
import link.rdcn.struct.DataFrame
import org.json.{JSONArray, JSONObject}

import java.io.{File, FileOutputStream, IOException, InputStream}
import java.nio.file.Paths
import java.util
import java.util.UUID
import java.util.concurrent.atomic.AtomicLong
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.language.postfixOps
import scala.util.{Failure, Success}

/**
 * @Author Yomi
 * @Description:
 * @Data 2025/7/30 17:03
 * @Modified By:
 */
trait OperatorRepository {
  def parseTransformFunctionWrapper(functionName: String, functionVersion: Option[String], ctx: FlowExecutionContext): TransformFunctionWrapper
}

class RepositoryClient(host: String = "http://10.0.89.39", port: Int = 8090) extends OperatorRepository {

  override def parseTransformFunctionWrapper(functionName: String, functionVersion: Option[String], ctx: FlowExecutionContext): TransformFunctionWrapper = {
    val client: OperatorClient = OperatorClient.connect(s"$host:$port", null)
    val operatorInfo = new JSONObject(client.getOperatorByNameAndVersion(functionName, functionVersion.orNull))
    if (operatorInfo.has("data") && operatorInfo.getJSONObject("data").getString("type") == "python-script") {
      val operatorImage = operatorInfo.getJSONObject("data").getString("nexusUrl")

      val inputCounter = new AtomicLong(0)
      val outputCounter = new AtomicLong(0)
      val ja = new JSONArray(operatorInfo.getJSONObject("data").getString("paramInfos"))
      val files = (0 until ja.length).map(index => ja.getJSONObject(index))
        //subfix,fileType,inParam,paramType
        .map(jo => (jo.getString("name"), jo.getString("fileType"), jo.getString("paramDescription"), jo.getString("paramType")))
        .map(file => {
          if (file._4 == "INPUT_FILE") {
            (file._1, file._2, file._3, file._4, s"input${inputCounter.incrementAndGet()}${file._1}")
          } else {
            (file._1, file._2, file._3, file._4, s"output${outputCounter.incrementAndGet()}${file._1}")
          }
        })
      val commands = operatorInfo.getJSONObject("data").getString("command").split(" ")

      val operationId = s"${functionName}_${UUID.randomUUID().toString}"
      val hostPath = FileUtils.getTempDirectory("", operationId)
      val containerPath = s"/$operationId"

      val commandsWithParams = commands ++ files.flatMap(file => Seq(file._3, Paths.get(containerPath, file._5).toString))
      var outputFileType = FileType.FIFO_BUFFER

      val inputFiles = files.filter(_._4 == "INPUT_FILE")
        .map(file => (Paths.get(hostPath, file._5).toString, FileType.fromString(file._2)))
      val outputFiles = files.filter(_._4 == "OUTPUT_FILE")
        .map { file =>
          outputFileType = FileType.fromString(file._2)
          (Paths.get(hostPath, file._5).toString, outputFileType)
        }
      val dockerContainer = DockerContainer(functionName, Some(hostPath), Some(containerPath), Some(operatorImage))
      if (outputFileType == FileType.FIFO_BUFFER)
        FifoFileRepositoryBundle(commandsWithParams, inputFiles, outputFiles, dockerContainer)
      else
        TempFileRepositoryBundle(commandsWithParams, inputFiles, outputFiles, dockerContainer)
    } else {
      val operatorDir = ctx.fairdHome
      val inputStream: InputStream = client.downloadOperatorAsStream(functionName, functionVersion.get)
      val operatorFunctionName = operatorInfo.getJSONObject("data").getString("language")
      val className = operatorInfo.getJSONObject("data").getString("help")
      operatorInfo.getJSONObject("data").getString("type") match {
        case LangTypeV2.JAVA_JAR.name =>
          val packageFile = Paths.get(operatorDir, "lib", s"$functionName-${functionVersion.get}.jar").toFile
          saveInputStreamToFile(inputStream, packageFile)
          JavaJar(packageFile.getAbsolutePath, operatorFunctionName, className)
        case LangTypeV2.CPP_BIN.name =>
          val packageFile = Paths.get(operatorDir, "lib", s"$functionName-${functionVersion.get}.cpp").toFile
          saveInputStreamToFile(inputStream, packageFile)
          CppBin(packageFile.getAbsolutePath)
        case LangTypeV2.PYTHON_BIN.name =>
          val packageFile = Paths.get(operatorDir, "lib", s"$functionName-${functionVersion.get}.whl").toFile
          PythonBin(operatorFunctionName, packageFile.getAbsolutePath)
        case _ => throw new IllegalArgumentException(s"Unsupported operator type: ${operatorInfo.get("type")}")

      }
    }
  }

  private def saveInputStreamToFile(inputStream: InputStream, file: File): Unit = {
    val outputStream = new FileOutputStream(file)
    val buffer = new Array[Byte](1024 * 8)
    var bytesRead = 0

    try {
      while ({ bytesRead = inputStream.read(buffer); bytesRead != -1 }) {
        outputStream.write(buffer, 0, bytesRead)
      }
      println(s"File saved at: ${file.getAbsoluteFile}")
    } catch {
      case e: IOException => println(s"Error while saving file: ${e.getMessage}")
    } finally {
      inputStream.close()
      outputStream.close()
    }
  }

}
