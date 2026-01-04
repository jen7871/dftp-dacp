/**
 * @Author Yomi
 * @Description:
 * @Data 2025/9/26 10:53
 * @Modified By:
 */
package link.rdcn.struct

import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

import java.util
import scala.collection.JavaConverters._

class RowTest {

  // Prepare standard Row for tests
  val testRow = new Row(Seq(1, "hello", true, null, Array(10.0, 20.0)))

  /**
   * Test basic accessors (get, length, isEmpty)
   */
  @Test
  def testCoreAccessors(): Unit = {
    assertEquals(5, testRow.length, "Row.length should be 5")
    assertEquals(1, testRow.get(0), "Row.get(0) should be 1")
    assertEquals("hello", testRow.get(1), "Row.get(1) should be 'hello'")
    assertFalse(testRow.isEmpty, "testRow.isEmpty should be false")
    assertTrue(Row.empty.isEmpty, "Row.empty.isEmpty should be true")

    // Test OOB
    assertThrows(classOf[IndexOutOfBoundsException], () => {
      testRow.get(99)
      ()
    }, "get(99) should throw IndexOutOfBoundsException")
  }

  /**
   * Test safe getOpt accessor
   */
  @Test
  def testGetOpt(): Unit = {
    assertEquals(Some(1), testRow.getOpt(0), "getOpt(0) should be Some(1)")
    assertEquals(Some(null), testRow.getOpt(3), "getOpt(3) should be Some(null)")
    assertEquals(None, testRow.getOpt(99), "getOpt(99) (OOB) should be None")
    assertEquals(None, testRow.getOpt(-1), "getOpt(-1) (OOB) should be None")
  }

  /**
   * Test typed accessor getAs[T]
   */
  @Test
  def testGetAs(): Unit = {
    assertEquals(1, testRow.getAs[Int](0), "getAs[Int](0) should be 1")
    assertEquals("hello", testRow.getAs[String](1), "getAs[String](1) should be 'hello'")
    assertTrue(testRow.getAs[Boolean](2), "getAs[Boolean](2) should be true")

    // Test getAs[T] for null
    assertNull(testRow.getAs[String](3), "getAs[String](3) (null value) should be null")
  }

  /**
   * Test getAs[T] failures
   */
  @Test
  def testGetAs_FailureCases(): Unit = {
    // Test index OOB
    assertThrows(classOf[NoSuchElementException], () => {
      testRow.getAs[String](99)
      ()
    }, "getAs[String](99) should throw NoSuchElementException")
  }

  /**
   * Test tuple-like accessors _1, _2 etc.
   */
  @Test
  def testTupleAccessors(): Unit = {
    val row = Row(10, "a", false)
    assertEquals(10, row._1, "row._1 should return 1st element")
    assertEquals("a", row._2, "row._2 should return 2nd element")
    assertEquals(false, row._3, "row._3 should return 3rd element")
  }

  /**
   * Test toSeq, toArray, iterator
   */
  @Test
  def testConversionMethods(): Unit = {
    assertEquals(testRow.values, testRow.toSeq, "toSeq should return original Seq")

    // Verify toArray
    val arr = testRow.toArray
    assertEquals(5, arr.length, "toArray length mismatch")
    assertEquals(1, arr(0), "toArray first element mismatch")
    assertTrue(arr(4).isInstanceOf[Array[Double]], "toArray fifth element type mismatch")

    // Verify iterator
    val iter = testRow.iterator
    assertTrue(iter.isInstanceOf[Iterator[Any]], "iterator should return an Iterator")
    assertEquals(1, iter.next(), "Iterator first element mismatch")
    assertEquals("hello", iter.next(), "Iterator second element mismatch")
  }

  /**
   * Test prepend and append (Immutability)
   */
  @Test
  def testPrependAndAppend(): Unit = {
    // 1. Test prepend
    val prependedRow = testRow.prepend("newHead")

    assertFalse(testRow.eq(prependedRow), "prepend should return a *new* Row instance")
    assertEquals(6, prependedRow.length, "New Row length incorrect after prepend")
    assertEquals("newHead", prependedRow.get(0), "New Row first element incorrect after prepend")
    assertEquals(1, prependedRow.get(1), "New Row second element incorrect after prepend")

    // 2. Test append
    val appendedRow = testRow.append("newTail")

    assertFalse(testRow.eq(appendedRow), "append should return a *new* Row instance")
    assertEquals(6, appendedRow.length, "New Row length incorrect after append")
    assertEquals(1, appendedRow.get(0), "New Row first element incorrect after append")
    assertEquals("newTail", appendedRow.get(5), "New Row last element incorrect after append")
  }

  /**
   * Test toString formatting for null and Array
   */
  @Test
  def testToStringFormatting(): Unit = {
    val expectedString = "Row(1, hello, true, null, Array(10.0, 20.0))"
    assertEquals(expectedString, testRow.toString, "toString() formatting incorrect")

    val simpleRow = Row("a", null)
    assertEquals("Row(a, null)", simpleRow.toString, "toString() formatting for null incorrect")
  }

  /**
   * Test toJsonString and toJsonObject
   */
  @Test
  def testJsonConversion(): Unit = {
    // Prepare StructType
    val schema = StructType.empty
      .add("id", ValueType.IntType)
      .add("name", ValueType.StringType)
      .add("active", ValueType.BooleanType)

    val row = Row(99, "Alice", true)

    // 1. Test toJsonObject
    val jsonObj = row.toJsonObject(schema)
    assertEquals(99, jsonObj.get("id"), "toJsonObject 'id' mismatch")
    assertEquals("Alice", jsonObj.get("name"), "toJsonObject 'name' mismatch")
    assertEquals(true, jsonObj.get("active"), "toJsonObject 'active' mismatch")

    // 2. Test toJsonString
    assertEquals(jsonObj.toString, row.toJsonString(schema), "toJsonString should match toJsonObject.toString")

    // 3. Test Schema length mismatch (zip behavior)
    val shortSchema = StructType.empty.add("id", ValueType.IntType)
    val shortJson = row.toJsonObject(shortSchema)

    assertEquals(99, shortJson.get("id"), "Field 'id' should match when schema is shorter")
    assertFalse(shortJson.has("name"), "Field 'name' should be absent when schema is shorter")
  }

  // --- Test object Row factory methods ---

  @Test
  def testFactory_Apply(): Unit = {
    val row = Row(1, "a", true)
    assertEquals(Seq(1, "a", true), row.toSeq, "Row.apply(varargs) failed")
  }

  @Test
  def testFactory_FromSeq(): Unit = {
    val seq = Seq(1, "a", true)
    val row = Row.fromSeq(seq)
    assertEquals(seq, row.toSeq, "Row.fromSeq failed")
  }

  @Test
  def testFactory_FromArray(): Unit = {
    val arr = Array[Any](1, "a", true)
    val row = Row.fromArray(arr)
    assertEquals(arr.toSeq, row.toSeq, "Row.fromArray failed")
  }

  @Test
  def testFactory_FromTuple(): Unit = {
    val tuple = (1, "a", true)
    val row = Row.fromTuple(tuple)
    assertEquals(Seq(1, "a", true), row.toSeq, "Row.fromTuple failed")
  }

  @Test
  def testFactory_FromJavaList(): Unit = {
    val javaList = new util.ArrayList[Object]()
    javaList.add(1.asInstanceOf[Object])
    javaList.add("a".asInstanceOf[Object])
    javaList.add(true.asInstanceOf[Object])

    val row = Row.fromJavaList(javaList)
    assertEquals(javaList.asScala.toSeq, row.toSeq, "Row.fromJavaList failed")
  }

  @Test
  def testFactory_FromJsonString(): Unit = {
    val jsonStr = """{"id": 123, "name": "Bob", "active": false}"""
    val row = Row.fromJsonString(jsonStr)

    // Note: JSONObject does not guarantee order
    val expectedValues = Set[Any](123, "Bob", false)
    assertEquals(3, row.length, "Length of Row parsed from json string incorrect")
    assertEquals(expectedValues, row.toSeq.toSet, "Content of Row parsed from json string incorrect")
  }

  @Test
  def testHelper_ZipWithIndex(): Unit = {
    val row = Row("a", "b", "c")
    val expected = Seq((0, "a"), (1, "b"), (2, "c"))
    val actual = Row.zipWithIndex(row)
    assertEquals(expected, actual, "Row.zipWithIndex result incorrect")
  }
}