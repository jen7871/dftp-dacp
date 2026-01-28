package link.rdcn.user

import org.json.JSONObject

import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}

/**
 * @Author renhao
 * @Description:
 * @Data 2025/10/10 14:41
 * @Modified By:
 */
case class OdcAuthCredentials(url: String, basic: String, usernamePassword: UsernamePassword) extends Credentials {

  def requestAccessToken(usernamePassword: UsernamePassword): String = {
    val paramMap = new JSONObject().put("username", usernamePassword.username)
      .put("password", usernamePassword.password)
      .put("grantType", "password")

    val httpClient = HttpClient.newHttpClient()
    val request = HttpRequest.newBuilder()
      .uri(URI.create("https://api.opendatachain.cn/auth/oauth/token"))
      .header("Authorization", basic)
      .header("Content-Type", "application/json")
      .POST(HttpRequest.BodyPublishers.ofString(paramMap.toString()))
      .build()

    val response = httpClient.send(request, HttpResponse.BodyHandlers.ofString())
    new JSONObject(response.body()).getString("data")
  }

}