/**
 * @Author Yomi
 * @Description:
 * @Data 2025/11/3 17:32
 * @Modified By:
 */
package link.rdcn.message

import com.fasterxml.jackson.core.JsonProcessingException
import org.junit.jupiter.api.Assertions.{assertEquals, assertNotNull, assertThrows, assertTrue}
import org.junit.jupiter.api.Test

class MapSerializerTest {

  @Test
  def testEncodeDecodeRoundTrip(): Unit = {
    // Prepare a complex Map with all supported types
    val originalMap: Map[String, Any] = Map(
      "stringValue" -> "Hello World",
      "intValue" -> 123,
      "doubleValue" -> 99.9,
      "booleanValue" -> true,
      "nullValue" -> null,
      "nestedMap" -> Map("subKey" -> "subValue"),
      "listValue" -> List(1, "a", false, 10.5)
    )

    // Encode
    val encodedBytes = MapSerializer.encodeMap(originalMap)
    assertNotNull(encodedBytes, "Encoded bytes should not be null")
    assertTrue(encodedBytes.length > 0, "Encoded bytes should not be empty")

    // Decode
    val decodedMap = MapSerializer.decodeMap(encodedBytes)
    assertNotNull(decodedMap, "Decoded Map should not be null")

    // Verify equality
    // Jackson deserializes List/Map to Java collections, so compare values
    assertEquals(originalMap("stringValue"), decodedMap("stringValue"), "String value mismatch")
    assertEquals(originalMap("intValue"), decodedMap("intValue"), "Int value mismatch")
    assertEquals(originalMap("doubleValue"), decodedMap("doubleValue"), "Double value mismatch")
    assertEquals(originalMap("booleanValue"), decodedMap("booleanValue"), "Boolean value mismatch")
    assertEquals(originalMap("nullValue"), decodedMap("nullValue"), "Null value mismatch")

    // Verify nested Map
    val expectedNestedMap = originalMap("nestedMap").asInstanceOf[Map[String, Any]]
    val actualNestedMap = decodedMap("nestedMap").asInstanceOf[Map[String, Any]]
    assertEquals(expectedNestedMap("subKey"), actualNestedMap("subKey"), "Nested Map value mismatch")

    // Verify nested List
    val expectedList = originalMap("listValue").asInstanceOf[List[Any]]
    val actualList = decodedMap("listValue").asInstanceOf[Seq[Any]]
    assertEquals(expectedList(0), actualList(0), "Nested List [0] mismatch")
    assertEquals(expectedList(1), actualList(1), "Nested List [1] mismatch")
    assertEquals(expectedList(2), actualList(2), "Nested List [2] mismatch")
    assertEquals(expectedList(3), actualList(3), "Nested List [3] mismatch")
  }

  /**
   * Test Edge Case: Empty Map
   */
  @Test
  def testEmptyMap(): Unit = {
    val emptyMap = Map.empty[String, Any]

    // Encode
    val encodedBytes = MapSerializer.encodeMap(emptyMap)

    assertEquals("{}", new String(encodedBytes, "UTF-8"), "Empty Map should encode to '{}'")

    // Decode
    val decodedMap = MapSerializer.decodeMap(encodedBytes)
    assertNotNull(decodedMap, "Decoded empty Map should not be null")
    assertTrue(decodedMap.isEmpty, "Decoded Map should be empty")
  }

  /**
   * Test Error Case: Decode Invalid Bytes
   */
  @Test
  def testDecodeInvalidBytes(): Unit = {
    val invalidBytes = "This is not JSON".getBytes("UTF-8")

    // Verify Jackson exception is thrown
    val exception = assertThrows(classOf[JsonProcessingException], () => {
      MapSerializer.decodeMap(invalidBytes)
      ()
    }, "Decoding invalid JSON should throw JsonProcessingException")

    assertNotNull(exception.getMessage, "Exception message should not be null")
  }

  /**
   * Test Error Case: Decode Empty Byte Array
   */
  @Test
  def testDecodeEmptyBytes(): Unit = {
    val emptyBytes = Array.empty[Byte]

    // Verify exception is thrown
    val exception = assertThrows(classOf[Exception], () => {
      MapSerializer.decodeMap(emptyBytes)
      ()
    }, "Decoding empty byte array should throw exception")

    assertNotNull(exception.getMessage, "Exception message should not be null")
  }
}