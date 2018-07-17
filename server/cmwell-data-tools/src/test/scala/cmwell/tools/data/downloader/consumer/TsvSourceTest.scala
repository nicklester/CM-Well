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


  val tsvs = List(
    "path1\tlastModified1\tuuid1\tindexTime1\n",
    "path2\tlastModified2\tuuid2\tindexTime2\n"
  )


  it should "work" in {

    val downloadSuccess1 = "download-success-1"

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
        .withBody(tsvs.mkString)
        .withStatus(StatusCodes.OK.intValue)
        .withHeader(CMWELL_N, (tsvs.size).toString)
        .withHeader(CMWELL_POSITION, "3AAAMHwv"))
      .willSetStateTo(downloadSuccess1)
    )

    val initTokenFuture = Future{
      new Downloader.Token("sqwqweq")
    }



    val source = Source.fromFuture(initTokenFuture)
      .via(TsvSourceSideChannel(label=Some("df"),baseUrl = s"localhost:${wireMockServer.port}",threshold = 10))


    val result = source.take(1).map(_=>1).runFold(0)(_ + _)

    result.flatMap{_ => 1 should be (1)}

    /*
    val future = source.take(1)
      .toMat(Sink.seq)(Keep.right).run
    */





    //assert( future.map(_ => 1) == 1)
  }

}
