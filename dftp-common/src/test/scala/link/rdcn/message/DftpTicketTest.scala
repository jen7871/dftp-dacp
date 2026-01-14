/**
 * @Author Yomi
 * @Description:
 * @Data 2025/9/26 10:51
 * @Modified By:
 */
package link.rdcn.message

import org.apache.arrow.flight.Ticket
import org.junit.jupiter.api.Assertions.{assertEquals, assertNotNull}
import org.junit.jupiter.api.Test

class DftpTicketTest {

  @Test
  def testGetTicketFromDftpTicketString(): Unit = {
    // DftpTicket 现在是 String 的类型别名
    val dftpTicketString = "token-12345-abc"

    // 执行：将 String 转换为 Arrow Ticket
    val arrowTicket: Ticket = DftpTicket.getTicket(dftpTicketString)

    assertNotNull(arrowTicket, "Generated Arrow Ticket should not be null")
    val bytes = arrowTicket.getBytes
    assertNotNull(bytes, "Ticket bytes should not be null")
  }

  @Test
  def testGetDftpTicketFromArrowTicket(): Unit = {
    val originalString = "access-token-xyz"
    // 通过工厂方法创建 Ticket
    val arrowTicket = DftpTicket.getTicket(originalString)

    // 执行：将 Arrow Ticket 转回 String
    val resultString = DftpTicket.getDftpTicket(arrowTicket)

    // 验证：往返转换应保持一致
    assertEquals(originalString, resultString, "Decoded DftpTicket string should match original")
  }

  @Test
  def testConstantsExistence(): Unit = {
    // 验证 ActionMethodType 常量是否存在 (用于编译检查)
    assertEquals("GET", ActionMethodType.GET)
    assertEquals("PUT_BLOB", ActionMethodType.PUT_BLOB)
    assertEquals("PUT_DATAFRAME", ActionMethodType.PUT_DATAFRAME)
  }
}