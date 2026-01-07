/**
 * @Author Yomi
 * @Description:
 * @Data 2025/11/3 17:32
 * @Modified By:
 */
package link.rdcn.message

import org.junit.jupiter.api.Assertions.{assertEquals, assertNotNull, assertThrows, assertTrue}
import org.junit.jupiter.api.Test

class MapSerializerTest {

  @Test
  def testEncodeDecodeRoundTrip(): Unit = {
    // Prepare a complex Map
    val originalMap: Map[String, Any] = Map(
      "key1" -> "value1",
      "key2" -> 123,
      "key3" -> 45.67,
      "key4" -> true,
      "nested" -> Map("subKey" -> "subValue")
    )

    // Encode
    val encodedBytes = MapSerializer.encodeMap(originalMap)
    assertNotNull(encodedBytes, "Encoded bytes should not be null")
    assertTrue(encodedBytes.length > 0, "Encoded bytes should not be empty")

    // Decode
    val decodedMap = MapSerializer.decodeMap(encodedBytes)
    assertNotNull(decodedMap, "Decoded Map should not be null")

    // Verify
    assertEquals("value1", decodedMap("key1"), "String value mismatch")
    assertEquals(123, decodedMap("key2"), "Int value mismatch")
    assertEquals(45.67, decodedMap("key3"), "Double value mismatch")
    assertEquals(true, decodedMap("key4"), "Boolean value mismatch")

    val nested = decodedMap("nested").asInstanceOf[Map[String, Any]]
    assertEquals("subValue", nested("subKey"), "Nested map value mismatch")
  }

  @Test
  def testEmptyMap(): Unit = {
    val encoded = MapSerializer.encodeMap(Map.empty)
    val decoded = MapSerializer.decodeMap(encoded)
    assertTrue(decoded.isEmpty, "Should handle empty map correctly")
  }

  @Test
  def testDecodeInvalidBytes(): Unit = {
    val invalidBytes = "Not JSON".getBytes
    assertThrows(classOf[Exception], () => {
      MapSerializer.decodeMap(invalidBytes)
      ()
    }, "Should throw exception when decoding invalid bytes")
  }
}