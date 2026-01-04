/**
 * @Author Yomi
 * @Description:
 * @Data 2025/9/26 10:53
 * @Modified By:
 */
package link.rdcn.struct

import org.junit.jupiter.api.Assertions.{assertEquals, assertThrows, assertTrue}
import org.junit.jupiter.api.{BeforeEach, Test}

import java.util.concurrent.atomic.AtomicInteger

class DefaultDataFrameTest {

  private val originalSchema = StructType(List(Column("id", ValueType.IntType), Column("name", ValueType.StringType)))
  private val mockRows = List(Row(1, "Alice"), Row(2, "Bob"), Row(3, "Charlie"))
  private var closeCounter: AtomicInteger = _
  private var mockStream: ClosableIterator[Row] = _
  private var df: DefaultDataFrame = _

  @BeforeEach
  def setUp(): Unit = {
    closeCounter = new AtomicInteger(0)
    // Create stream with trackable close behavior
    mockStream = ClosableIterator(mockRows.iterator)(closeCounter.incrementAndGet())
    df = DefaultDataFrame(originalSchema, mockStream)
  }

  @Test
  def testMapOperation(): Unit = {
    // Cover map method
    val mappedDf = df.map(row => Row(row.get(0).asInstanceOf[Int] * 2))

    // Verify new stream's close logic: original stream's close() should be propagated
    val transformedRows = mappedDf.collect()

    assertEquals(1, closeCounter.get(), "New stream's close must invoke original stream's close()")
    assertEquals(2, transformedRows.head.values.head, "Mapped stream should contain transformed data (1*2=2)")
  }

  @Test
  def testFilterOperation(): Unit = {
    // Cover filter method
    val filteredDf = df.filter(row => row.get(0).asInstanceOf[Int] > 1)

    // Verify data transformation
    val newStream = filteredDf.asInstanceOf[DefaultDataFrame].stream
    val filteredRows = newStream.toList

    assertEquals(2, filteredRows.size, "Filtered stream should contain 2 rows (2 and 3)")
    assertEquals(2, filteredRows.head.values.head, "Filtered stream should start with row 2")
  }

  @Test
  def testSelectOperation_Success(): Unit = {
    val columnsToSelect = Seq("name", "id")
    // Cover select method success path
    val selectedDf = df.select(columnsToSelect: _*)

    assertTrue(selectedDf.isInstanceOf[DefaultDataFrame], "Select should return a DefaultDataFrame")

    // Verify new Schema
    val newSchema = selectedDf.schema
    assertEquals(2, newSchema.columns.size, "New schema should contain 2 columns")
    assertEquals("name", newSchema.columns.head.name, "New schema should respect order: name, id")

    // Verify data transformation
    val collectedRows = selectedDf.collect()
    assertEquals("Alice", collectedRows.head.values.head, "Data should be transformed and reordered: name")
    assertEquals(1, collectedRows.head.values(1), "Data should be transformed and reordered: id")

    // Verify close propagation
    assertEquals(1, closeCounter.get(), "Collect on selectedDf should close the original stream")
  }

  @Test
  def testSelectOperation_InvalidColumn(): Unit = {
    // Cover select method exception path
    val exception = assertThrows(
      classOf[IllegalArgumentException],
      () => df.select("id", "non_existent_col")
    )

    // Note: Adjust the expected message if the source code message is in English
    // Assuming source throws "StructType.select: 列名 'non_existent_col' 不存在" or similar
    assertTrue(exception.getMessage.contains("non_existent_col"), "Exception should indicate the missing column name")
  }

  @Test
  def testLimitOperation(): Unit = {
    val limitN = 2
    // Cover limit method
    val limitedDf = df.limit(limitN)

    assertTrue(limitedDf.isInstanceOf[DefaultDataFrame], "Limit should return a DefaultDataFrame")
    assertEquals(originalSchema, limitedDf.schema, "Schema should be preserved by limit")

    // Verify data is limited
    val collectedRows = limitedDf.collect()
    assertEquals(limitN, collectedRows.size, "Limited DataFrame should contain only N rows")

    // Verify close propagation
    assertEquals(1, closeCounter.get(), "Collect on limitedDf should close the original stream")
  }

  @Test
  def testForeach_UsesResourceUtilsAndCloses(): Unit = {
    var count = 0
    // Cover foreach method
    df.foreach(_ => count += 1)

    assertEquals(mockRows.size, count, "Foreach must iterate over all rows")
    assertEquals(1, closeCounter.get(), "Foreach must close the stream via ResourceUtils")
  }

  @Test
  def testCollect_UsesResourceUtilsAndCloses(): Unit = {
    // Cover collect method
    val collected = df.collect()

    assertEquals(mockRows.size, collected.size, "Collect must return all rows")
    assertEquals(1, closeCounter.get(), "Collect must close the stream via ResourceUtils")
  }

  @Test
  def testMapIterator(): Unit = {
    // Cover mapIterator[T] method
    val result: Int = df.mapIterator(iter => iter.size)

    assertEquals(mockRows.size, result, "MapIterator should receive and count the stream size")
  }
}