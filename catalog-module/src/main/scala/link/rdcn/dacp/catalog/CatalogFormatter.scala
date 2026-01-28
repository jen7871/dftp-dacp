package link.rdcn.dacp.catalog

import com.sun.management.OperatingSystemMXBean
import ConfigKeys.{FAIRD_HOST_PORT, FAIRD_HOST_POSITION}
import link.rdcn.server.{ServerContext, ServerUtils}
import link.rdcn.struct.ValueType.StringType
import link.rdcn.struct.{DataFrameDocument, DataFrameStatistics, DataStreamSource, Row, StructType}
import link.rdcn.util.DataUtils
import org.json.{JSONArray, JSONObject}

import java.io.File
import java.lang.management.ManagementFactory

/**
 * @Author renhao
 * @Description:
 * @Data 2025/10/30 14:12
 * @Modified By:
 */
object CatalogFormatter {

  def getSystemInfo(): JSONObject = {
    val osBean = ManagementFactory.getOperatingSystemMXBean
      .asInstanceOf[OperatingSystemMXBean]
    val runtime = Runtime.getRuntime

    val cpuLoadPercent = (osBean.getSystemCpuLoad * 100).formatted("%.2f")
    val availableProcessors = osBean.getAvailableProcessors

    val totalMemory = runtime.totalMemory() / 1024 / 1024 // MB
    val freeMemory = runtime.freeMemory() / 1024 / 1024 // MB
    val maxMemory = runtime.maxMemory() / 1024 / 1024 // MB
    val usedMemory = totalMemory - freeMemory

    val file = new File("/")
    val totalDiskSpace = file.getTotalSpace / 1024 / 1024 / 1024 // GB
    val freeDiskSpace = file.getFreeSpace / 1024 / 1024 / 1024 // GB

    val systemMemoryTotal = osBean.getTotalPhysicalMemorySize / 1024 / 1024 // MB
    val systemMemoryFree = osBean.getFreePhysicalMemorySize / 1024 / 1024 // MB
    val systemMemoryUsed = systemMemoryTotal - systemMemoryFree
    val json = new JSONObject()
    json.put("net.mac.address", ServerUtils.getFirstNonLoopBackMacAddress)
    json.put("cpu.cores", availableProcessors)
    json.put("cpu.usage.percent", s"$cpuLoadPercent%")
    json.put("jvm.memory.max.mb", s"$maxMemory MB")
    json.put("jvm.memory.total.mb", s"$totalMemory MB")
    json.put("jvm.memory.used.mb", s"$usedMemory MB")
    json.put("jvm.memory.free.mb", s"$freeMemory MB")
    json.put("system.memory.total.mb", s"$systemMemoryTotal MB")
    json.put("system.memory.used.mb", s"$systemMemoryUsed MB")
    json.put("system.memory.free.mb", s"$systemMemoryFree MB")

    json.put("disk.total.space.gb", s"$totalDiskSpace GB")
    json.put("disk.used.space.gb", s"${totalDiskSpace - freeDiskSpace} GB")
    json.put("disk.free.space.gb", s"$freeDiskSpace GB")

    json
  }

  def getHostInfo(serverContext: ServerContext): JSONObject = {
    val hostInfo = Map(
      s"$FAIRD_HOST_POSITION" -> s"${serverContext.getHost()}",
      s"$FAIRD_HOST_PORT" -> s"${serverContext.getPort()}"
    )
    val jo = new JSONObject()
    hostInfo.foreach(kv => jo.put(kv._1, kv._2))
    jo
  }
}
