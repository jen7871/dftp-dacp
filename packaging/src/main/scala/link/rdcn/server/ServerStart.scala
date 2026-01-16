package link.rdcn.server

import java.nio.file.Paths

/**
 * @Author renhao
 * @Description:
 * @Data 2025/10/28 16:12
 * @Modified By:
 */

object ServerStart {
  def main(args: Array[String]): Unit = {
//    if (args.length < 1) sys.error("need set Dftp Home")
//    val serverHome = args(0)
    val serverHome = "/Users/renhao/IdeaProjects/dftp-dacp/packaging/home"

    val confDir = Paths.get(serverHome, "conf").toFile

    if (confDir.exists() && confDir.isDirectory) {
      val xmlFiles = confDir.listFiles().filter(f => f.isFile && f.getName.endsWith(".xml"))
      if (xmlFiles.length == 2) {
        val configXmlFile = xmlFiles.find(_.getName != "log4j2.xml").getOrElse(
          throw new RuntimeException("Expected one XML file other than log4.xml")
        )
        DftpServer.start(configXmlFile)
      } else {
        throw new RuntimeException(s"Unexpected number of XML files found in the conf directory: ${xmlFiles.length}")
      }
    } else {
      throw new RuntimeException(s"Conf directory does not exist: $confDir")
    }
  }
}
