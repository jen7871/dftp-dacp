package link.rdcn.client.dacp

import link.rdcn.client.{DacpClient, RemoteDataFrameProxy, UrlValidator}
import link.rdcn.operation.SourceOp
import link.rdcn.struct.DataFrame
import link.rdcn.user.Credentials

/**
 * @Author Yomi
 * @Description:
 * @Data 2025/10/9 17:55
 * @Modified By:
 */
class DacpClientProxy private(host: String, port: Int, useTLS: Boolean = false) extends DacpClient(host, port, useTLS) {
  def getTargetServerUrl: String = {
    doAction("/getTargetServerUrl").result
  }

  override def validateUrl(url: String): String = {
    if (UrlValidator.isPath(url)) url
    else {
      UrlValidator.validate(url) match {
        case Right(v) => url
        case Left(message) => throw new IllegalArgumentException(message)
      }
    }
  }
}


object DacpClientProxy {
  val protocolSchema = "dacp"
  private val urlValidator = UrlValidator(protocolSchema)

  def connect(url: String, credentials: Credentials = Credentials.ANONYMOUS): DacpClientProxy = {
    urlValidator.validate(url) match {
      case Right(parsed) =>
        val client = new DacpClientProxy(parsed._1, parsed._2.getOrElse(3101))
        client.login(credentials)
        client
      case Left(err) =>
        throw new IllegalArgumentException(s"Invalid DACP URL: $err")
    }
  }

  def connectTLS(url: String, credentials: Credentials = Credentials.ANONYMOUS): DacpClientProxy = {
    urlValidator.validate(url) match {
      case Right(parsed) =>
        val client = new DacpClientProxy(parsed._1, parsed._2.getOrElse(3101), true)
        client.login(credentials)
        client
      case Left(err) =>
        throw new IllegalArgumentException(s"Invalid DACP URL: $err")
    }
  }
}
