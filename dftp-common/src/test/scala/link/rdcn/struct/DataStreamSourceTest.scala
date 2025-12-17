/**
 * @Author Yomi
 * @Description:
 * @Data 2025/9/26 10:53
 * @Modified By:
 */
package link.rdcn.struct

import link.rdcn.struct.ValueType.{DoubleType, LongType, StringType}
import org.apache.poi.xssf.usermodel.XSSFWorkbook
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.io.TempDir
import org.junit.jupiter.api.Test

import java.io.{File, FileOutputStream}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import scala.io.Source

class DataStreamSourceTest {

  @TempDir
  var tempDir: Path = _

  // Helper to create a CSV file
  private def createCsvFile(name: String, content: String): File = {
    val file = tempDir.resolve(name).toFile
    Files.write(file.toPath, content.getBytes(StandardCharsets.UTF_8))
    file
  }

  // Helper to create a simple Excel file
  private def createExcelFile(name: String, rowCount: Int): File = {
    val file = tempDir.resolve(name).toFile
    val wb = new XSSFWorkbook()
    val sheet = wb.createSheet("Sheet1")

    // Header
    val header = sheet.createRow(0)
    header.createCell(0).setCellValue("id")
    header.createCell(1).setCellValue("value")

    for (i <- 1 to rowCount) {
      val row = sheet.createRow(i)
      row.createCell(0).setCellValue(i.toDouble)
      row.createCell(1).setCellValue(i * 1.5)
    }

    val fos = new FileOutputStream(file)
    wb.write(fos)
    fos.close()
    wb.close()
    file
  }

  @Test
  def testCsvWithHeader(): Unit = {
    // Prepare data
    val csvContent = "id,value\n1,10.5\n2,20.0"
    val mockFile = createCsvFile("data_header.csv", csvContent)

    // Execute: DataStreamSource.csv
    val source = DataStreamSource.csv(mockFile, delimiter = Some(","))

    // Verify properties
    // Note: countLinesFast counts physical lines. Header + 2 rows = 3 lines.
    assertEquals(3L, source.rowCount, "rowCount must match line count of file")

    val expectedSchema = StructType.empty.add("id", LongType).add("value", DoubleType)
    assertEquals(expectedSchema, source.schema, "Schema must be inferred correctly from header and data")

    val iter = source.iterator
    val rows = iter.toList

    // Validate first row data (Iterator skips header if inferred)
    val firstRow = rows.head
    assertEquals(1L, firstRow.get(0), "First column of first row must be 1")
    assertEquals(10.5, firstRow.get(1), "Second column of first row must be 10.5")
  }

  @Test
  def testCsvWithoutHeader(): Unit = {
    // Prepare data (No header)
    val csvContent = "val1,val2\nval3,val4"
    val mockFile = createCsvFile("data_no_header.csv", csvContent)

    // Execute: DataStreamSource.csv with header=false
    val source = DataStreamSource.csv(mockFile, delimiter = Some(","), header = false)

    // Verify properties
    assertEquals(2L, source.rowCount, "rowCount must match line count")

    // Without header, columns are named _1, _2 and typed as String (default inference if mixed or unknown)
    val expectedSchema = StructType.empty.add("_1", StringType).add("_2", StringType)
    assertEquals(expectedSchema, source.schema, "Schema must use default names and inferred types")

    val iter = source.iterator
    val rows = iter.toList

    val firstRow = rows.head
    assertEquals("val1", firstRow.getAs[String](0), "First column value mismatch")
    assertEquals("val2", firstRow.getAs[String](1), "Second column value mismatch")
  }

  @Test
  def testExcel(): Unit = {
    val totalRows = 5
    val mockFile = createExcelFile("data.xlsx", totalRows)
    val excelPath = mockFile.getPath

    // Execute: DataStreamSource.excel
    val source = DataStreamSource.excel(excelPath)

    // Verify properties
    assertEquals(-1L, source.rowCount, "rowCount for Excel source is typically -1 (unknown until read)")

    // Verify iterator
    val iter = source.iterator
    val rows = iter.toList
    assertEquals(totalRows, rows.size, "Iterator should contain exactly the number of data rows created")
    assertEquals(1.0, rows.head.getAs[Double](0), "First ID should be 1.0")
  }

  @Test
  def testFilePathNonRecursive(): Unit = {
    // Create a dummy file in the temp dir to list
    createCsvFile("file1.bin", "content")

    val mockDir = tempDir.toFile

    // Execute: DataStreamSource.filePath
    val source = DataStreamSource.filePath(mockDir, "")

    // Verify properties
    assertEquals(-1L, source.rowCount, "rowCount for FilePath source is -1")
    assertEquals(StructType.binaryStructType, source.schema, "Schema must be binaryStructType")

    // Verify iterator content
    val iter = source.iterator
    val row = iter.next()
    // binaryStructType usually has: fileName, path, size, mTime, aTime, cTime, isDir, content(Blob)
    assertEquals(8, row.values.size, "Row must contain 8 file attributes and Blob")
    assertEquals("file1.bin", row.getAs[String](0), "First column should be fileName")
  }
}