/**
 * @Author Yomi
 * @Description:
 * @Data 2025/9/26 10:55
 * @Modified By:
 */
package link.rdcn.util

import org.junit.jupiter.api.Assertions.{assertEquals, assertThrows}
import org.junit.jupiter.api.Test

import java.io.ByteArrayInputStream
import java.nio.charset.StandardCharsets

class MimeTypeFactoryTest {

  // --- Prepare "Magic Numbers" locally for testing ---

  // PNG: 89 50 4E 47 0D 0A 1A 0A
  val PNG_MAGIC: Array[Byte] = Array(
    0x89.toByte, 0x50.toByte, 0x4E.toByte, 0x47.toByte,
    0x0D.toByte, 0x0A.toByte, 0x1A.toByte, 0x0A.toByte
  )

  // PDF: %PDF-1.
  val PDF_MAGIC: Array[Byte] = "%PDF-1.4".getBytes(StandardCharsets.US_ASCII)

  // TEXT: MimeUtil usually identifies readable US-ASCII as text/plain
  val TEXT_DATA: Array[Byte] = "This is a plain text file.".getBytes(StandardCharsets.US_ASCII)

  // EMPTY
  val EMPTY_DATA: Array[Byte] = Array.emptyByteArray

  // --- MimeType (Case Class) Tests ---

  @Test
  def testMimeTypeMajorMinor(): Unit = {
    // Assuming 'application/pdf' is in properties
    val pdfType = MimeTypeFactory.fromText("application/pdf")

    assertEquals("application", pdfType.major, "Major type (application) should be correctly parsed")
    assertEquals("pdf", pdfType.minor, "Minor type (pdf) should be correctly parsed")
  }

  // --- MimeTypeFactory (Object) Tests ---

  @Test
  def testFromText_Success(): Unit = {
    // Assuming 100=image/png is in properties
    val mimeType = MimeTypeFactory.fromText("image/png")
    assertEquals(278L, mimeType.code, "Should find correct code based on properties file")
    assertEquals("image/png", mimeType.text, "Text should remain unchanged (lowercase)")
  }

  @Test
  def testFromText_CaseInsensitive(): Unit = {
    // Assuming 101=image/jpeg is in properties
    val mimeType = MimeTypeFactory.fromText("IMAGE/JPEG")
    assertEquals(273L, mimeType.code, "Should find code case-insensitively")
    assertEquals("image/jpeg", mimeType.text, "Text should be normalized to lowercase")
  }

  @Test
  def testFromText_Unknown(): Unit = {
    val invalidText = "application/x-non-existent"

    assertThrows(classOf[UnknownMimeTypeException], () => {
      MimeTypeFactory.fromText(invalidText)
      ()
    }, s"Unknown mime-type text ($invalidText) should throw UnknownMimeTypeException")
  }

  @Test
  def testGuessMimeType_Bytes(): Unit = {
    // Test PNG
    val pngType = MimeTypeFactory.guessMimeType(PNG_MAGIC)
    assertEquals(278L, pngType.code, "PNG bytes should be identified as code 100")
    assertEquals("image/png", pngType.text, "PNG bytes should be identified as 'image/png'")

    // Test PDF
    val pdfType = MimeTypeFactory.guessMimeType(PDF_MAGIC)
    assertEquals(46L, pdfType.code, "PDF bytes should be identified as code 200")
    assertEquals("application/pdf", pdfType.text, "PDF bytes should be identified as 'application/pdf'")

    // Test Text
    val textType = MimeTypeFactory.guessMimeType(TEXT_DATA)
    assertEquals(44L, textType.code, "Text bytes should be identified as code 300")
    assertEquals("application/octet-stream", textType.text, "Text bytes should be identified as 'application/octet-stream'")
  }

  @Test
  def testGuessMimeType_EmptyBytes(): Unit = {
    // Assuming -1=unknown/unknown is default
    val unknownType = MimeTypeFactory.guessMimeType(EMPTY_DATA)
    assertEquals(44L, unknownType.code, "Empty byte array should return default code -1")
    assertEquals("application/octet-stream", unknownType.text, "Empty byte array should return 'application/octet-stream'")
  }

  @Test
  def testGuessMimeType_InputStream(): Unit = {
    // (This method reads the full stream)
    val is = new ByteArrayInputStream(PNG_MAGIC)
    val pngType = MimeTypeFactory.guessMimeType(is)
    is.close()

    assertEquals(278L, pngType.code, "PNG InputStream should be identified as code 100")
    assertEquals("image/png", pngType.text, "PNG InputStream should be identified as 'image/png'")
  }

  @Test
  def testGuessMimeTypeWithPrefix_ShortStream(): Unit = {
    // (This method reads only prefix)
    val is = new ByteArrayInputStream(PDF_MAGIC)
    val pdfType = MimeTypeFactory.guessMimeTypeWithPrefix(is)
    is.close()

    assertEquals(46L, pdfType.code, "PDF InputStream (short) should be identified as code 200")
    assertEquals("application/pdf", pdfType.text, "PDF InputStream (short) should be identified as 'application/pdf'")
  }

  @Test
  def testGuessMimeTypeWithPrefix_LongStream(): Unit = {
    // (This method reads prefix 4KB)
    // Create stream larger than 4KB, with magic number at start
    val junk = new Array[Byte](5000)
    val longData = PDF_MAGIC ++ junk
    val is = new ByteArrayInputStream(longData)

    val pdfType = MimeTypeFactory.guessMimeTypeWithPrefix(is)
    is.close()

    assertEquals(46L, pdfType.code, "PDF header in long InputStream should be correctly identified")
    assertEquals("application/pdf", pdfType.text, "PDF header in long InputStream should be correctly identified")
  }

  @Test
  def testGuessMimeTypeWithPrefix_EmptyStream(): Unit = {
    val is = new ByteArrayInputStream(EMPTY_DATA)
    val unknownType = MimeTypeFactory.guessMimeTypeWithPrefix(is)
    is.close()

    assertEquals(44L, unknownType.code, "Empty InputStream should return default code -1")
    assertEquals("application/octet-stream", unknownType.text, "Empty InputStream should return 'application/octet-stream'")
  }
}