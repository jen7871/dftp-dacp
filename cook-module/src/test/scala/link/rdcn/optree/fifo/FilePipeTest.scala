/**
 * @Author Yomi
 * @Description:
 * @Data 2025/11/6 15:12
 * @Modified By:
 */
package link.rdcn.optree.fifo

import link.rdcn.dacp.optree.fifo.FilePipe
import link.rdcn.struct.{ClosableIterator, DataFrame, DefaultDataFrame, Row, StructType}
import link.rdcn.struct.ValueType.StringType
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertNotNull, assertTrue}
import org.junit.jupiter.api.condition.{DisabledOnOs, OS}
import org.junit.jupiter.api.io.TempDir
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}

import java.io.File
import java.nio.file.Files
import scala.util.Random

class FilePipeTest {

  // Local Mock Implementation
  class MockFilePipe(file: File) extends FilePipe(file) {
    override def dataFrame(): DataFrame =
      DefaultDataFrame(StructType.empty.add("mock", StringType), Seq(Row("mockData")).iterator)
    override def write(messages: Iterator[String]): Unit = ???
    override def read(): ClosableIterator[String] = ???
  }

  @TempDir
  var tempDir: File = _
  var testFile: File = _
  var pipe: MockFilePipe = _

  @BeforeEach
  def setUp(): Unit = {
    testFile = new File(tempDir, s"test_pipe_${Random.nextLong()}.fifo")
    if (testFile.exists()) testFile.delete()
    pipe = new MockFilePipe(testFile)
  }

  @Test
  @DisabledOnOs(value = Array(OS.WINDOWS), disabledReason = "mkfifo not available on Windows")
  def testCreate_OnUnix(): Unit = {
    assertFalse(testFile.exists(), "File should not exist before create()")
    pipe.create()
    assertTrue(testFile.exists(), "File should exist after create()")
    assertFalse(Files.isRegularFile(testFile.toPath), "File should be a FIFO pipe")
  }

  @Test
  def testDataFrame(): Unit = {
    val df = pipe.dataFrame()
    assertNotNull(df, "dataFrame() should not return null")
    assertEquals(1, df.collect().length, "DataFrame should have 1 row")
  }

  @AfterEach
  def release(): Unit = {
    if (testFile != null) testFile.delete()
  }
}
