package link.rdcn.util

import link.rdcn.client.UrlValidator
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

class DftpUrlValidatorTest {

  val urlValidator = UrlValidator("dftp")

  @Test
  def testValidateValidUrl(): Unit = {
    val result = urlValidator.validate("dftp://0.0.0.0:3101/listDataFrameNames/mydataset")
    result match {
      case Right((host, port, path)) =>
        assertEquals("0.0.0.0", host, "Host should match")
        assertEquals(Some(3101), port, "Port should match")
        assertEquals("/listDataFrameNames/mydataset", path, "Path should match")
      case Left(err) => fail(s"Validation failed: $err")
    }
  }

  @Test
  def testValidateWithPathPrefixSuccess(): Unit = {
    val result = urlValidator.validateWithPathPrefix(
      "dftp://example.com/getDataFrameSize/myframe",
      "/getDataFrameSize/"
    )
    assertTrue(result.isRight, "Validation should succeed with correct prefix")
  }

  @Test
  def testValidateWithPathPrefixFailure(): Unit = {
    val result = urlValidator.validateWithPathPrefix(
      "dftp://example.com/wrongPrefix/myframe",
      "/getDataFrameSize/"
    )
    assertTrue(result.isLeft, "Validation should fail with wrong prefix")
  }

  @Test
  def testValidateAndExtractParamSuccess(): Unit = {
    val result = urlValidator.validateAndExtractParam(
      "dftp://localhost:9090/listDataFrameNames/mydataset",
      "/listDataFrameNames/"
    )
    result match {
      case Right((host, port, param)) =>
        assertEquals("localhost", host, "Host match")
        assertEquals(Some(9090), port, "Port match")
        assertEquals("mydataset", param, "Param match")
      case Left(err) => fail(s"Parameter extraction failed: $err")
    }
  }

  @Test
  def testValidateAndExtractParamMissing(): Unit = {
    val result = urlValidator.validateAndExtractParam(
      "dftp://localhost:9090/listDataFrameNames/",
      "/listDataFrameNames/"
    )
    assertTrue(result.isLeft, "Should fail when param is missing")
  }

  @Test
  def testIsValidPositive(): Unit = {
    assertTrue(urlValidator.isValid("dftp://example.com:8080"), "Valid URL should return true")
  }

  @Test
  def testIsValidNegative(): Unit = {
    assertFalse(urlValidator.isValid("dftp://bad:port:abc/path"), "Invalid URL should return false")
  }

  @Test
  def testEmptyPath(): Unit = {
    val result = urlValidator.validate("dftp://example.com")
    result match {
      case Right((_, _, path)) => assertEquals("/", path, "Empty path should default to '/'")
      case Left(err) => fail(s"Empty path handling failed: $err")
    }
  }
}
