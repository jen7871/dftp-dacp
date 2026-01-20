package link.rdcn.util

import link.rdcn.user.{Credentials, TokenAuth, UsernamePassword}
import org.junit.jupiter.api.Assertions.{assertArrayEquals, assertEquals, assertThrows, assertTrue}
import org.junit.jupiter.api.Test

class CodecUtilsTest {

  @Test
  def testEncodeDecodeString(): Unit = {
    val input = "Hello Codec"
    val encoded = CodecUtils.encodeString(input)
    val decoded = CodecUtils.decodeString(encoded)

    assertEquals(input, decoded, "String encoding/decoding roundtrip failed")
  }

  @Test
  def testEncodeStringNull(): Unit = {
    val encoded = CodecUtils.encodeString(null)
    assertArrayEquals(Array.emptyByteArray, encoded, "Null string should encode to empty byte array")
  }

  @Test
  def testDecodeStringNullOrEmpty(): Unit = {
    assertEquals("", CodecUtils.decodeString(null), "Null bytes should decode to empty string")
    assertEquals("", CodecUtils.decodeString(Array.emptyByteArray), "Empty bytes should decode to empty string")
  }

  @Test
  def testCredentials_UsernamePassword(): Unit = {
    val creds = UsernamePassword("admin", "123456")
    val encoded = CodecUtils.encodeCredentials(creds)
    val decoded = CodecUtils.decodeCredentials(encoded)

    assertTrue(decoded.isInstanceOf[UsernamePassword], "Should decode to UsernamePassword")
    val decodedUp = decoded.asInstanceOf[UsernamePassword]
    assertEquals("admin", decodedUp.username, "Username mismatch")
    assertEquals("123456", decodedUp.password, "Password mismatch")
  }

  @Test
  def testCredentials_TokenAuth(): Unit = {
    val token = TokenAuth("secret-token-value")
    val encoded = CodecUtils.encodeCredentials(token)
    val decoded = CodecUtils.decodeCredentials(encoded)

    assertTrue(decoded.isInstanceOf[TokenAuth], "Should decode to TokenAuth")
    assertEquals("secret-token-value", decoded.asInstanceOf[TokenAuth].token, "Token mismatch")
  }

  @Test
  def testCredentials_Anonymous(): Unit = {
    val encoded = CodecUtils.encodeCredentials(Credentials.ANONYMOUS)
    val decoded = CodecUtils.decodeCredentials(encoded)

    assertEquals(Credentials.ANONYMOUS, decoded, "Should decode to Anonymous singleton")
  }

  @Test
  def testCredentials_Unsupported(): Unit = {
    val unknownBytes = Array[Byte](99, 0, 0, 0, 0, 0, 0, 0, 0) // Invalid type ID 99

    val ex = assertThrows(classOf[IllegalArgumentException], () => {
      CodecUtils.decodeCredentials(unknownBytes)
      ()
    }, "Should throw exception for unknown credential type ID")

    assertTrue(ex.getMessage.contains("99 not supported"), "Exception message should indicate unsupported type")
  }
}