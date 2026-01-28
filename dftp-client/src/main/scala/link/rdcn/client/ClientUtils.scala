package link.rdcn.client

import link.rdcn.struct.ValueType._
import link.rdcn.struct._
import link.rdcn.util.CodecUtils
import org.apache.arrow.flight.{PutResult, Result, SyncPutListener}
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.ipc.ArrowStreamReader
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}
import org.apache.arrow.vector.types.{FloatingPointPrecision, Types}
import org.apache.arrow.vector._

import java.io.ByteArrayInputStream
import java.util.Collections
import scala.collection.JavaConverters.{asJavaIterableConverter, asScalaBufferConverter}

/**
 * @Author renhao
 * @Description:
 * @Data 2025/9/17 15:28
 * @Modified By:
 */
object ClientUtils {
  def arrowSchemaToStructType(schema: org.apache.arrow.vector.types.pojo.Schema): StructType = {
    val columns = schema.getFields.asScala.map { field =>
      val colType = field.getType match {
        case t if t == Types.MinorType.INT.getType => ValueType.IntType
        case t if t == Types.MinorType.BIGINT.getType => ValueType.LongType
        case t if t == Types.MinorType.FLOAT4.getType => ValueType.FloatType
        case t if t == Types.MinorType.FLOAT8.getType => ValueType.DoubleType
        case t if t == Types.MinorType.VARCHAR.getType =>
          if (field.getMetadata.isEmpty) ValueType.StringType else ValueType.RefType
        case t if t == Types.MinorType.BIT.getType => ValueType.BooleanType
        case _ => throw new UnsupportedOperationException(s"Unsupported Arrow type: ${field.getType}")
      }
      Column(field.getName, colType)
    }
    StructType(columns.toList)
  }

  def getVectorSchemaRootFromBytes(bytes: Array[Byte], allocator: BufferAllocator): VectorSchemaRoot = {
    val inputStream = new ByteArrayInputStream(bytes)
    val reader = new ArrowStreamReader(inputStream, allocator)
    reader.loadNextBatch()
    reader.getVectorSchemaRoot
  }

  def convertStructTypeToArrowSchema(structType: StructType): Schema = {
    val fields: List[Field] = structType.columns.map { column =>
      val arrowFieldType = column.colType match {
        case IntType =>
          new FieldType(column.nullable, new ArrowType.Int(32, true), null)
        case LongType =>
          new FieldType(column.nullable, new ArrowType.Int(64, true), null)
        case FloatType =>
          new FieldType(column.nullable, new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE), null)
        case DoubleType =>
          new FieldType(column.nullable, new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE), null)
        case StringType =>
          new FieldType(column.nullable, ArrowType.Utf8.INSTANCE, null)
        case BooleanType =>
          new FieldType(column.nullable, ArrowType.Bool.INSTANCE, null)
        case BinaryType =>
          new FieldType(column.nullable, new ArrowType.Binary(), null)
        case RefType =>
          val metadata = new java.util.HashMap[String, String]()
          metadata.put("logicalType", "Url")
          new FieldType(column.nullable, ArrowType.Utf8.INSTANCE, null, metadata)
        case _ =>
          throw new UnsupportedOperationException(s"Unsupported type: ${column.colType}")
      }

      new Field(column.name, arrowFieldType, Collections.emptyList())
    }.toList

    new Schema(fields.asJava)
  }

  def parsePutListener(putListener: SyncPutListener): Iterator[String] = {
    new Iterator[String] {
      private var nextAck: Option[PutResult] = Option(putListener.read())

      override def hasNext: Boolean = nextAck.isDefined

      override def next(): String = {
        nextAck match {
          case Some(ack) =>
            val metadataBuf = ack.getApplicationMetadata
            val bytes = new Array[Byte](metadataBuf.readableBytes().toInt)
            metadataBuf.readBytes(bytes)
            CodecUtils.decodeString(bytes)
          case None =>
            throw new NoSuchElementException("No more ACKs")
        }
      }
    }
  }
}
