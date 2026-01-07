/**
 * @Author Yomi
 * @Description:
 * @Data 2025/9/26 10:49
 * @Modified By:
 */
package link.rdcn.struct

import link.rdcn.struct.ValueType.{IntType, StringType}
import org.apache.arrow.memory.{BufferAllocator, RootAllocator}
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, Schema}
import org.apache.arrow.vector.{IntVector, VarCharVector, VectorSchemaRoot}
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}

import java.nio.charset.StandardCharsets
import scala.collection.JavaConverters._

class ArrowFlightStreamWriterTest {

  private var allocator: BufferAllocator = _
  private var root: VectorSchemaRoot = _

  @BeforeEach
  def setUp(): Unit = {
    allocator = new RootAllocator(Long.MaxValue)
  }

  @AfterEach
  def tearDown(): Unit = {
    if (root != null) root.close()
    if (allocator != null) allocator.close()
  }

  @Test
  def testProcess_WritesDataToVectorSchemaRoot(): Unit = {
    // 1. Prepare Schema and Root
    val arrowFields = List(
      Field.nullable("name", ArrowType.Utf8.INSTANCE),
      Field.nullable("age", new ArrowType.Int(32, true))
    ).asJava
    val arrowSchema = new Schema(arrowFields)
    root = VectorSchemaRoot.create(arrowSchema, allocator)

    // 2. Prepare Data
    val rows = Seq(
      Row("Alice", 30),
      Row("Bob", 25),
      Row("Charlie", 35)
    )
    val iterator = rows.iterator

    // 3. Initialize Writer
    val writer = ArrowFlightStreamWriter(iterator)

    // 4. Process (Batch size 2)
    val batches = writer.process(root, 2)

    // 5. Verify First Batch
    assertTrue(batches.hasNext, "Should have a first batch")
    val batch1 = batches.next()

    // Note: ArrowFlightStreamWriter.process usually populates the root and returns VectorLoader.load objects or similar references.
    // Assuming implementation populates 'root' and yields it.

    assertEquals(2, root.getRowCount, "First batch should have 2 rows")
    val nameVec = root.getVector("name").asInstanceOf[VarCharVector]
    val ageVec = root.getVector("age").asInstanceOf[IntVector]

    assertEquals("Alice", new String(nameVec.get(0), StandardCharsets.UTF_8), "Row 0 Name mismatch")
    assertEquals(30, ageVec.get(0), "Row 0 Age mismatch")
    assertEquals("Bob", new String(nameVec.get(1), StandardCharsets.UTF_8), "Row 1 Name mismatch")

    batch1.close() // Clean up batch

    // 6. Verify Second Batch
    assertTrue(batches.hasNext, "Should have a second batch")
    val batch2 = batches.next()

    // Root should now reflect second batch data
    assertEquals(1, root.getRowCount, "Second batch should have 1 row")
    assertEquals("Charlie", new String(nameVec.get(0), StandardCharsets.UTF_8), "Row 0 (Batch 2) Name mismatch")

    batch2.close()

    // 7. Verify End
    assertTrue(!batches.hasNext, "Should be exhausted")
  }
}