/**
  * Copyright 2015 Thomson Reuters
  *
  * Licensed under the Apache License, Version 2.0 (the “License”); you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
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
import akka.stream.scaladsl.{Sink, Source}
import akka.stream.{ActorMaterializer, Materializer}
import cmwell.tools.data.utils.akka.HeaderOps.{CMWELL_N, CMWELL_POSITION}
import com.github.tomakehurst.wiremock.client.WireMock._
import com.github.tomakehurst.wiremock.stubbing.Scenario
import akka.stream.scaladsl.Keep
import akka.stream.testkit.scaladsl.{TestSink, TestSource}
import akka.testkit.TestProbe
import akka.util.ByteString
import cmwell.tools.data.downloader.consumer.Downloader.TsvData
import cmwell.tools.data.downloader.consumer.TsvSource.SensorOutput

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.concurrent._
import ExecutionContext.Implicits.global

class TsvSourceTest3 extends BaseWireMockSpecFLat {

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
    "secondb\tlastModified2\tuuid2\tindexTime2\n",
    "secondc\tlastModified2\tuuid2\tindexTime2\n",
    "secondd\tlastModified2\tuuid2\tindexTime2\n",
    "seconde\tlastModified2\tuuid2\tindexTime2\n",
    "secondf\tlastModified2\tuuid2\tindexTime2\n",
    "secondg\tlastModified2\tuuid2\tindexTime2\n",
    "secondh\tlastModified2\tuuid2\tindexTime2\n",
    "secondi\tlastModified2\tuuid2\tindexTime2\n",
    "secondj\tlastModified2\tuuid2\tindexTime2\n",
    "secondk\tlastModified2\tuuid2\tindexTime2\n",
    "secondl\tlastModified2\tuuid2\tindexTime2\n",
    "secondm\tlastModified2\tuuid2\tindexTime2\n",
    "secondn\tlastModified2\tuuid2\tindexTime2\n",
    "secondo\tlastModified2\tuuid2\tindexTime2\n"
  )

  val tsvs3 = List(
    "thirda\tlastModified1\tuuid1\tindexTime1\n",
    "thirdb\tlastModified2\tuuid2\tindexTime2\n"
  )

  val scenario = "scenario"

  val downloadSuccess1 = "download-success-1"
  val downloadSuccess2 = "download-success-2"
  val downloadSuccess3 = "download-success-3"

  val downloadNoContent = "download-no-content"



  lazy val createAndRunTestGraph = {

    val initTokenFuture = Future {  new Downloader.Token("A") }
    val testProbeSink = TestSink.probe[SensorOutput]

    val testSource = Source.fromGraph(TsvSource(initialToken = initTokenFuture, label = Some("df"),
      baseUrl = s"localhost:${wireMockServer.port}", retryTimeout = 10.seconds, threshold = 10, consumeLengthHint = Some(10)))

    testSource.toMat(testProbeSink)(Keep.right).run().ensureSubscription

  }


  ignore should "be resilient on server errors" in {

    val scenario = "scenario"

    val downloadSuccess1 = "download-success-1"
    val downloadFail1 = "download-fail1"
    val downloadFail2 = "download-fail2"
    val downloadFail3 = "download-fail3"
    val downloadFail4 = "download-fail4"
    val downloadSuccess2 = "download-success-2"



    stubFor(get(urlPathMatching("/_consume.*")).inScenario(scenario)
      .whenScenarioStateIs(Scenario.STARTED)
      .willReturn(aResponse()
        .withStatus(StatusCodes.GatewayTimeout.intValue))
      .willSetStateTo(downloadFail2)
    )

    stubFor(get(urlPathMatching("/_consume.*")).inScenario(scenario)
      .whenScenarioStateIs(downloadFail2)
      .willReturn(aResponse()
        .withStatus(StatusCodes.GatewayTimeout.intValue))
      .willSetStateTo(downloadSuccess1)
    )


    stubFor(get(urlPathMatching("/_consume.*")).inScenario(scenario)
      .whenScenarioStateIs(downloadSuccess1)
      .willReturn(aResponse()
        .withBody(tsvs1.mkString)
        .withStatus(StatusCodes.OK.intValue)
        .withHeader(CMWELL_N, (tsvs1.size).toString)
        .withHeader(CMWELL_POSITION, "B"))
      .willSetStateTo(downloadFail3)
    )

    stubFor(get(urlPathMatching("/_consume.*")).inScenario(scenario)
      .whenScenarioStateIs(downloadFail3)
      .willReturn(aResponse()
        .withStatus(StatusCodes.GatewayTimeout.intValue))
      .willSetStateTo(downloadSuccess2)
    )

    stubFor(get(urlPathMatching("/_consume.*")).inScenario(scenario)
      .whenScenarioStateIs(downloadSuccess2)
      .willReturn(aResponse()
        .withBody(tsvs2.mkString)
        .withStatus(StatusCodes.OK.intValue)
        .withHeader(CMWELL_N, (tsvs2.size).toString)
        .withHeader(CMWELL_POSITION, "C"))
      .willSetStateTo(downloadFail4)
    )

    stubFor(get(urlPathMatching("/_consume.*")).inScenario(scenario)
      .whenScenarioStateIs(downloadFail4)
      .willReturn(aResponse()
        .withStatus(StatusCodes.GatewayTimeout.intValue))
    )


    val initTokenFuture = Future {
      new Downloader.Token("A")
    }

    val testSource = Source.fromGraph(TsvSource(initialToken = initTokenFuture, label = Some("df"),
      baseUrl = s"localhost:${wireMockServer.port}", retryTimeout = 10.seconds, threshold = 10, consumeLengthHint = Some(10)))


    val probe = TestProbe()

    val c = testSource.toMat(Sink.actorRef(probe.ref, "completed"))(Keep.right).run()


//    probe.expectMsg(10.second, "hello")





    // val result = testSource.take(4).toMat(Sink.seq)(Keep.right).run()

    //val result2 = result.map(_=>1).runFold(0)(_ + _)

    //result.map {
    //  r=> r.size should be (4)
    //}

  }

  "TsvSource" should "shouldn't call consume until there is demand" in {

    val mat = createAndRunTestGraph

    // No demand, so no message received
    mat.expectNoMessage(5.seconds)

    val numberConsumeRequests = wireMockServer.findAll(getRequestedFor(urlPathMatching("/_consume"))).size

    assert(numberConsumeRequests==0)

  }

  "TsvSource" should "should call consume when there is demand and emit" in {

    stubFor(get(urlPathMatching("/_consume.*")).inScenario(scenario)
      .whenScenarioStateIs(Scenario.STARTED)
      .willReturn(aResponse()
        .withBody(tsvs1.mkString)
        .withStatus(StatusCodes.OK.intValue)
        .withHeader(CMWELL_N, (tsvs1.size).toString)
        .withHeader(CMWELL_POSITION, "B"))
      .willSetStateTo(downloadSuccess2)
    )

    stubFor(get(urlPathMatching("/_consume.*")).inScenario(scenario)
      .whenScenarioStateIs(downloadSuccess2)
      .willReturn(aResponse()
        .withBody(tsvs2.mkString)
        .withStatus(StatusCodes.OK.intValue)
        .withHeader(CMWELL_N, (tsvs2.size).toString)
        .withHeader(CMWELL_POSITION, "C"))
      .willSetStateTo(downloadSuccess3)
    )

    val mat = createAndRunTestGraph

    // Apply demand
    mat.request(1)

    val expect = ((new Downloader.Token("A"),
      TsvData(path = ByteString("firsta"),
        uuid = ByteString("uuid1"),
        lastModified = ByteString("lastModified1"),
        indexTime = ByteString("indexTime1"))), false, None)

    mat.expectNext(3.seconds,expect)

    /* _consume is called twice. Once to put some content in the buffer on the first pull on the source. The
     second pull will emit the expected element and will call _consume again as the buffer threshold will not have
     bean reached. */
    val numberConsumeRequests = wireMockServer.findAll(getRequestedFor(urlPathMatching("/_consume"))).size
    assert(numberConsumeRequests==2)

    mat.expectNoMessage(5.seconds)

  }

  "TsvSource" should "only call _consume when there is further demand on the source" in {
    val mat = createAndRunTestGraph
    mat.expectNoMessage(10.seconds)

    // The buffer is full, so during this time no further consumer were requested
    val numberConsumeRequests = wireMockServer.findAll(getRequestedFor(urlPathMatching("/_consume"))).size
    assert(numberConsumeRequests==0)
  }

  "TsvSource" should "call _consume safely when there is demand and _consume returns 204" in {

    stubFor(get(urlPathMatching("/_consume.*")).inScenario(scenario)
      .whenScenarioStateIs(downloadSuccess3)
      .willReturn(aResponse()
        .withStatus(StatusCodes.NoContent.intValue)
        .withHeader(CMWELL_N, (0).toString)
        .withHeader(CMWELL_POSITION, "C"))
      .willSetStateTo(downloadNoContent)
    )

    stubFor(get(urlPathMatching("/_consume.*")).inScenario(scenario)
      .whenScenarioStateIs(downloadNoContent)
      .willReturn(aResponse()
        .withStatus(StatusCodes.NoContent.intValue)
        .withHeader(CMWELL_N, (0).toString)
        .withHeader(CMWELL_POSITION, "C"))
      .willSetStateTo(downloadNoContent)
    )


      val mat = createAndRunTestGraph

    //mat.request(20)
    //mat.expectNextN(12)

    val numberConsumeRequests = wireMockServer.findAll(getRequestedFor(urlPathMatching("/_consume"))).size
    assert(numberConsumeRequests==3)

    mat.expectNoMessage(5.seconds)

  }









}
