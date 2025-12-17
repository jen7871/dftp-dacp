/**
 * @Author Yomi
 * @Description:
 * @Data 2025/9/26 10:49
 * @Modified By:
 */
package link.rdcn.struct

import link.rdcn.util.CodecUtils
import org.apache.arrow.memory.{BufferAllocator, RootAllocator}
import org.apache.arrow.vector._
import org.apache.arrow.vector.types.Types
import org.apache.arrow.vector.types.pojo._
import org.junit.jupiter.api.Assertions.{assertArrayEquals, assertEquals, assertThrows, assertTrue}
import org.junit.jupiter.api.{AfterAll, Test}

import java.io.InputStream
import java.math.BigDecimal
import java.nio.charset.StandardCharsets
import scala.collection.JavaConverters._

// Singleton object to hold shared resources
object ArrowFlightStreamWriterTest {
  private val ALL_FIELD_NAMES = List("int", "long", "double", "float", "decimal", "string", "boolean", "binary", "ref", "blob")
  private val fields: java.lang.Iterable[Field] = List(
    Field.nullable(ALL_FIELD_NAMES(0), Types.MinorType.INT.getType),
    Field.nullable(ALL_FIELD_NAMES(1), Types.MinorType.BIGINT.getType),
    Field.nullable(ALL_FIELD_NAMES(2), Types.MinorType.FLOAT8.getType),
    Field.nullable(ALL_FIELD_NAMES(3), Types.MinorType.FLOAT4.getType),
    Field.nullable(ALL_FIELD_NAMES(4), Types.MinorType.VARCHAR.getType), // BigDecimal encoded as String/VarChar
    Field.nullable(ALL_FIELD_NAMES(5), Types.MinorType.VARCHAR.getType),
    Field.nullable(ALL_FIELD_NAMES(6), Types.MinorType.BIT.getType),
    Field.nullable(ALL_FIELD_NAMES(7), Types.MinorType.VARBINARY.getType),
    Field.nullable(ALL_FIELD_NAMES(8), Types.MinorType.VARCHAR.getType), // DFRef encoded as String/VarChar
    Field.nullable(ALL_FIELD_NAMES(9), Types.MinorType.VARBINARY.getType) // Blob encoded as Byte[]
  ).asJava

  val schema = new Schema(fields)
  val allocator: BufferAllocator = new RootAllocator()
  val root: VectorSchemaRoot = VectorSchemaRoot.create(schema, allocator)

  val testBlob = new Blob{
    override def offerStream[T](consume: InputStream => T): T = {
      val stream = new InputStream {
        override def read(): Int = ???
      }
      try consume(stream)
      finally stream.close()
    }
  }

  // Create a row covering all supported types and case branches
  val testRow = Row(
    10: Int, // Int
    20L: Long, // Long
    30.5: Double, // Double
    40.5f: Float, // Float
    new BigDecimal("50.5"), // BigDecimal (encoded as VarChar)
    "test_str", // String (encoded as VarChar)
    true: Boolean, // Boolean (encoded as BitVector)
    "raw_bytes".getBytes(StandardCharsets.UTF_8), // Array[Byte] (encoded as VarBinary)
    DFRef("http://ref"), // DFRef (encoded as VarChar)
    testBlob // Blob (encoded as VarBinary)
  )

  @AfterAll
  def tearDown(): Unit = {
    if (root != null) root.close()
    if (allocator != null) allocator.close()
  }
}

class ArrowFlightStreamWriterTest {

  import ArrowFlightStreamWriterTest._

  @Test
  def testProcessGroupsRowsCorrectly(): Unit = {
    val allRows = List(testRow, testRow, testRow)

    val writer = ArrowFlightStreamWriter(allRows.iterator)

    // Test batchSize = 2
    val batches = writer.process(root, 2).toList

    assertEquals(2, batches.size, "Process should produce 2 batches (2 + 1)")
    assertEquals(2, batches.head.getLength, "First batch row count should be 2")
    assertEquals(1, batches(1).getLength, "Second batch row count should be 1")

    // Clean up
    batches.foreach(_.close())
  }

  @Test
  def testCreateDummyBatch_AllTypesCovered(): Unit = {
    val writer = ArrowFlightStreamWriter(List(testRow).iterator)
    val batch = writer.process(root, 1).next() // Trigger createDummyBatch

    // Verify row count
    assertEquals(1, root.getRowCount, "Row count should be 1")

    // Verify IntVector
    val intVector = root.getVector("int").asInstanceOf[IntVector]
    assertEquals(10, intVector.get(0), "Int value must match")

    // Verify LongVector
    val longVector = root.getVector("long").asInstanceOf[BigIntVector]
    assertEquals(20L, longVector.get(0), "Long value must match")

    // Verify BooleanVector
    val bitVector = root.getVector("boolean").asInstanceOf[BitVector]
    assertEquals(1, bitVector.get(0), "Boolean value must be 1 (true)")

    // Verify Blob registration and encoding (VarBinaryVector)
    val blobVector = root.getVector("blob").asInstanceOf[VarBinaryVector]
    assertTrue(BlobRegistry.getBlob(CodecUtils.decodeString(blobVector.get(0))).isDefined, "Blob should be registered and encoded as Byte[]")

    // Verify DFRef
    val refVector = root.getVector("ref").asInstanceOf[VarCharVector]
    assertArrayEquals("http://ref".getBytes(StandardCharsets.UTF_8), refVector.get(0), "DFRef should be encoded as URL bytes")

    // Clean up
    batch.close()
    BlobRegistry.cleanUp()
  }

  @Test
  def testCreateDummyBatchNullValue(): Unit = {
    // Cover case null => vec.setNull(i)
    val nullRow = Row(null, null, null, null, null, null, null, null, null, null)
    val writer = ArrowFlightStreamWriter(List(nullRow).iterator)
    writer.process(root, 1).next().close() // Trigger createDummyBatch

    // Verify all fields are null
    root.allocateNew()
    val allVectors = root.getFieldVectors.asScala

    allVectors.foreach(vec => {
      assertTrue(vec.isNull(0), s"Vector ${vec.getName} should be null at index 0")
    })
  }

  @Test
  def testCreateDummyBatchUnsupportedType(): Unit = {
    class UnsupportedType
    val rowWithUnsupported = Row(Seq(10, 20L, new UnsupportedType()))

    // Create a root with Int, Long and VarChar
    val unsupportedRoot = VectorSchemaRoot.create(
      new Schema(List(
        Field.nullable("i", Types.MinorType.INT.getType),
        Field.nullable("l", Types.MinorType.BIGINT.getType),
        Field.nullable("u", Types.MinorType.VARCHAR.getType) // Expected to fail here
      ).asJava), allocator)

    try {
      val writer = ArrowFlightStreamWriter(List(rowWithUnsupported).iterator)
      // Attempt process, expect exception when VarCharVector tries to handle UnsupportedType

      val exception = assertThrows(classOf[UnsupportedOperationException], () => {
        writer.process(unsupportedRoot, 1).next().close()
      })
      assertEquals("Type not supported", exception.getMessage)

    } finally {
      unsupportedRoot.close()
    }
  }
}