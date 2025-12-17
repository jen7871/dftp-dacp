/**
 * @Author Yomi
 * @Description:
 * @Data 2025/9/26 10:54
 * @Modified By:
 */
package link.rdcn.struct

import link.rdcn.struct.ValueType._
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertThrows, assertTrue}
import org.junit.jupiter.api.Test

class ValueTypeTest {

  // --- DFRef Tests ---

  @Test
  def testDFRef(): Unit = {
    val url = "dftp://example.com/data"
    val ref = DFRef(url)

    assertEquals(url, ref.value, "DFRef.value should return url")
    assertEquals(RefType, ref.valueType, "DFRef.valueType should return RefType")
  }

  // --- ValueType object (case objects) Tests ---

  @Test
  def testValueType_NamesAndToString(): Unit = {
    assertEquals("Int", IntType.name, "IntType.name mismatch")
    assertEquals("Long", LongType.name, "LongType.name mismatch")
    assertEquals("Float", FloatType.name, "FloatType.name mismatch")
    assertEquals("Double", DoubleType.name, "DoubleType.name mismatch")
    assertEquals("String", StringType.name, "StringType.name mismatch")
    assertEquals("Boolean", BooleanType.name, "BooleanType.name mismatch")
    assertEquals("Binary", BinaryType.name, "BinaryType.name mismatch")
    assertEquals("REF", RefType.name, "RefType.name mismatch")
    assertEquals("Blob", BlobType.name, "BlobType.name mismatch")
    assertEquals("Null", NullType.name, "NullType.name mismatch")

    // toString defaults to name
    assertEquals("String", StringType.toString, "StringType.toString mismatch")
  }

  // --- ValueType object (API) Tests ---

  @Test
  def testValueType_ValuesList(): Unit = {
    val values = ValueType.values

    assertEquals(8, values.length, "ValueType.values sequence should contain 8 elements")

    // Verify types present
    assertTrue(values.contains(IntType), "values sequence should contain IntType")
    assertTrue(values.contains(NullType), "values sequence should contain NullType")

    // Verify omitted types
    assertFalse(values.contains(RefType), "values sequence should NOT contain RefType")
    assertFalse(values.contains(BlobType), "values sequence should NOT contain BlobType")
  }

  @Test
  def testValueType_FromName_HappyPath(): Unit = {
    assertEquals(Some(IntType), ValueType.fromName("Int"), "fromName('Int') failed")
    assertEquals(Some(StringType), ValueType.fromName("String"), "fromName('String') failed")
    assertEquals(Some(NullType), ValueType.fromName("Null"), "fromName('Null') failed")
  }

  @Test
  def testValueType_FromName_SynonymsAndCase(): Unit = {
    // Synonyms
    assertEquals(Some(IntType), ValueType.fromName("integer"), "fromName('integer') failed")
    assertEquals(Some(BooleanType), ValueType.fromName("bool"), "fromName('bool') failed")
    assertEquals(Some(BinaryType), ValueType.fromName("bytes"), "fromName('bytes') failed")

    // Case insensitivity
    assertEquals(Some(IntType), ValueType.fromName("int"), "fromName('int') failed")
    assertEquals(Some(StringType), ValueType.fromName("STRING"), "fromName('STRING') failed")
    assertEquals(Some(BooleanType), ValueType.fromName("BoOlEaN"), "fromName('BoOlEaN') failed")
  }

  @Test
  def testValueType_FromName_Failure(): Unit = {
    // 1. Unknown type
    val exUnknown = assertThrows(classOf[Exception], () => {
      ValueType.fromName("UnknownType")
      ()
    }, "fromName('UnknownType') should throw exception")

    assertTrue(exUnknown.getMessage.contains("does not exist unknowntype"), "Exception message incorrect")

    // 2. Type not defined in fromName (e.g., REF)
    val exRef = assertThrows(classOf[Exception], () => {
      ValueType.fromName("REF")
      ()
    }, "fromName('REF') should throw exception")

    assertTrue(exRef.getMessage.contains("does not exist ref"), "Exception message incorrect")
  }

  @Test
  def testValueType_IsNumeric(): Unit = {
    assertTrue(ValueType.isNumeric(IntType), "isNumeric(IntType) should be true")
    assertTrue(ValueType.isNumeric(LongType), "isNumeric(LongType) should be true")
    assertTrue(ValueType.isNumeric(FloatType), "isNumeric(FloatType) should be true")
    assertTrue(ValueType.isNumeric(DoubleType), "isNumeric(DoubleType) should be true")

    assertFalse(ValueType.isNumeric(StringType), "isNumeric(StringType) should be false")
    assertFalse(ValueType.isNumeric(BooleanType), "isNumeric(BooleanType) should be false")
    assertFalse(ValueType.isNumeric(BinaryType), "isNumeric(BinaryType) should be false")
    assertFalse(ValueType.isNumeric(NullType), "isNumeric(NullType) should be false")
    assertFalse(ValueType.isNumeric(RefType), "isNumeric(RefType) should be false")
    assertFalse(ValueType.isNumeric(BlobType), "isNumeric(BlobType) should be false")
  }

  // --- ValueTypeHelper object Tests ---

  @Test
  def testHelper_GetAllTypes(): Unit = {
    assertEquals(ValueType.values, ValueTypeHelper.getAllTypes, "getAllTypes() should return ValueType.values")
  }

  @Test
  def testHelper_GetSpecificTypes(): Unit = {
    assertEquals(IntType, ValueTypeHelper.getIntType, "getIntType failed")
    assertEquals(LongType, ValueTypeHelper.getLongType, "getLongType failed")
    assertEquals(FloatType, ValueTypeHelper.getFloatType, "getFloatType failed")
    assertEquals(DoubleType, ValueTypeHelper.getDoubleType, "getDoubleType failed")
    assertEquals(StringType, ValueTypeHelper.getStringType, "getStringType failed")
    assertEquals(BooleanType, ValueTypeHelper.getBooleanType, "getBooleanType failed")
    assertEquals(BinaryType, ValueTypeHelper.getBinaryType, "getBinaryType failed")
    assertEquals(NullType, ValueTypeHelper.getNullType, "getNullType failed")
  }

  @Test
  def testHelper_IsNumeric(): Unit = {
    assertTrue(ValueTypeHelper.isNumeric(IntType), "Helper.isNumeric(IntType) should be true")
    assertFalse(ValueTypeHelper.isNumeric(StringType), "Helper.isNumeric(StringType) should be false")
  }

  @Test
  def testHelper_FromName_HappyPath(): Unit = {
    assertEquals(IntType, ValueTypeHelper.fromName("Int"), "Helper.fromName('Int') failed")
    assertEquals(IntType, ValueTypeHelper.fromName("integer"), "Helper.fromName('integer') failed")
    assertEquals(StringType, ValueTypeHelper.fromName("STRING"), "Helper.fromName('STRING') failed")
  }

  @Test
  def testHelper_FromName_Failure(): Unit = {
    val ex = assertThrows(classOf[Exception], () => {
      ValueTypeHelper.fromName("UnknownType")
      ()
    }, "Helper.fromName('UnknownType') should throw exception")

    assertTrue(ex.getMessage.contains("does not exist unknowntype"), "Exception message incorrect")
  }
}