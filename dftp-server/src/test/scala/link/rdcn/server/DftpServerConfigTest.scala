package link.rdcn.server

import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

class DftpServerConfigTest {

  @Test
  def testConfigBuilderMethods(): Unit = {
    val initial = DftpServerConfig("localhost")

    // Test chaining builders
    val updated = initial
      .withPort(9999)
      .withUseTls(true)
      .withProtocolScheme("dftps")
      .withDftpHome("/tmp/dftp")
      .withDataSourcePath("/data")

    // Verify updates
    assertEquals("localhost", updated.host, "Host should stay same")
    assertEquals(9999, updated.port, "Port should be updated")
    assertTrue(updated.useTls, "TLS should be enabled")
    assertEquals("dftps", updated.protocolScheme, "Protocol scheme should be updated")
    assertEquals(Some("/tmp/dftp"), updated.dftpHome, "DFTP home should be updated")
    assertEquals(Some("/data"), updated.dftpDataSource, "Data source should be updated")

    // Verify immutability (original object not changed)
    assertEquals(3101, initial.port, "Original config should remain unchanged")
  }

  @Test
  def testConfigBeanConversion(): Unit = {
    val bean = new DftpServerConfigBean()
    bean.setHost("127.0.0.1")
    bean.setPort(8080)
    bean.setDftpHome("/var/dftp")
    bean.setUseTls(false)
    // Pointing to non-existent files to ensure code handles it (returns empty/None)
    // instead of crashing or requiring complex crypto setup
    bean.setPublicKeyMapPath("non_existent_pub.keys")
    bean.setPrivateKeyPath("non_existent_priv.key")

    val config = bean.toDftpServerConfig

    assertEquals("127.0.0.1", config.host, "Host mismatch")
    assertEquals(8080, config.port, "Port mismatch")
    assertEquals(Some("/var/dftp"), config.dftpHome, "DFTP Home mismatch")
    assertFalse(config.useTls, "TLS mismatch")

    // Verify file loading logic handles missing files gracefully
    assertTrue(config.pubKeyMap.isEmpty, "Public key map should be empty if file missing")
    assertTrue(config.privateKey.isEmpty, "Private key should be None if file missing")
  }
}