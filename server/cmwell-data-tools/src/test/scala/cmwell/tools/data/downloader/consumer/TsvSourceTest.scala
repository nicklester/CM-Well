/**
  * Copyright 2015 Thomson Reuters
  *
  * Licensed under the Apache License, Version 2.0 (the “License”); you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at
  *
  *   http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
  * an “AS IS” BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  *
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */

package cmwell.tools.data.downloader.consumer

import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes
import akka.stream.scaladsl.Source
import akka.stream.{ActorMaterializer, Materializer}
import cmwell.tools.data.helpers.BaseWiremockSpec
import cmwell.tools.data.utils.akka.HeaderOps.{CMWELL_N, CMWELL_POSITION}
import com.github.tomakehurst.wiremock.client.WireMock.{aResponse, get, stubFor, urlPathMatching}
import com.github.tomakehurst.wiremock.stubbing.Scenario

import scala.concurrent.Future

class TsvSourceTest extends BaseWiremockSpec {

  val scenario = "scenario"

  implicit val system: ActorSystem = ActorSystem.create("reactive-tools-system")
  implicit val mat: Materializer = ActorMaterializer()

  override protected def afterAll(): Unit = {
    system.terminate()
    super.afterAll()
  }


  val tsvs1 = List(
    "firsta\tlastModified1\tuuid1\tindexTime1\n",
    "firstb\tlastModified2\tuuid2\tindexTime2\n"
  )

  val tsvs2 = List(
    "seconda\tlastModified1\tuuid1\tindexTime1\n",
    "secondb\tlastModified2\tuuid2\tindexTime2\n"
  )

  val tsvs3 = List(
    "thirda\tlastModified1\tuuid1\tindexTime1\n",
    "thirdb\tlastModified2\tuuid2\tindexTime2\n"
  )


  it should "work" in {

    val downloadSuccess1 = "download-success-1"
    val downloadSuccess2 = "download-success-2"
    val downloadSuccess3 = "download-success-3"

    stubFor(get(urlPathMatching("/.*")).inScenario(scenario)
      .whenScenarioStateIs(Scenario.STARTED)
      .willReturn(aResponse()
        .withStatus(StatusCodes.OK.intValue)
        .withHeader(CMWELL_POSITION, "3AAAMHwv"))
      .willSetStateTo(downloadSuccess1)
    )

    stubFor(get(urlPathMatching("/_consume")).inScenario(scenario)
      .whenScenarioStateIs(downloadSuccess1)
      .willReturn(aResponse()
        .withBody(tsvs1.mkString)
        .withStatus(StatusCodes.OK.intValue)
        .withHeader(CMWELL_N, (tsvs1.size).toString)
        .withHeader(CMWELL_POSITION, "3AAAMHwvr"))
      .willSetStateTo(downloadSuccess2)
    )

    stubFor(get(urlPathMatching("/_consume")).inScenario(scenario)
      .whenScenarioStateIs(downloadSuccess2)
      .willReturn(aResponse()
        .withBody(tsvs2.mkString)
        .withStatus(StatusCodes.OK.intValue)
        .withHeader(CMWELL_N, (tsvs2.size).toString)
        .withHeader(CMWELL_POSITION, "3AAAMHwvs"))
      .willSetStateTo(downloadSuccess3)
    )

    stubFor(get(urlPathMatching("/_consume")).inScenario(scenario)
      .whenScenarioStateIs(downloadSuccess3)
      .willReturn(aResponse()
        .withBody(tsvs3.mkString)
        .withStatus(StatusCodes.OK.intValue)
        .withHeader(CMWELL_N, (tsvs3.size).toString)
        .withHeader(CMWELL_POSITION, "3AAAMHwvt"))
      .willSetStateTo(downloadSuccess1)
    )

    val initTokenFuture = Future{
      new Downloader.Token("sqwqweq")
    }

    val source = Source.fromFuture(initTokenFuture)
      .via(TsvSourceSideChannel(label=Some("df"),baseUrl = s"localhost:${wireMockServer.port}",threshold = 10))


    val result = source.take(6).map(_=>1).runFold(0)(_ + _)

    result.flatMap{_ => 1 should be (1)}


  }

}
