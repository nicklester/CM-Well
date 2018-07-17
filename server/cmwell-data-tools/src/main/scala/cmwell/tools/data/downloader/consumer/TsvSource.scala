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

import akka.NotUsed
import akka.actor.ActorSystem
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.RawHeader
import akka.stream._
import akka.stream.scaladsl.{Broadcast, GraphDSL, Keep, Sink, Source}
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import cmwell.tools.data.downloader.consumer.Downloader._
import cmwell.tools.data.utils.ArgsManipulations
import cmwell.tools.data.utils.ArgsManipulations.{HttpAddress, formatHost}
import cmwell.tools.data.utils.akka.HeaderOps.{getHostnameValue, getNLeft, getPosition}
import cmwell.tools.data.utils.akka.{HttpConnections, lineSeparatorFrame}
import cmwell.tools.data.utils.logging.DataToolsLogging
import cmwell.tools.data.utils.text.Tokens
import cmwell.util.akka.http.HttpZipDecoder
import scala.concurrent.ExecutionContext

import scala.concurrent.duration.{FiniteDuration, _}
import scala.collection.mutable
import scala.concurrent.Future
import scala.util.{Failure, Success, Try}
import scala.concurrent.ExecutionContext.Implicits.global

/*
case class PushedTsv(token: Downloader.Token, tsv: Downloader.TsvData, horizon: Boolean, remaining: Option[Long])

object TsvSource {
  def apply(threshold: Long = 100,
            params: String = "",
            isBulk: Boolean = false,
            baseUrl: String,
            label: Option[String] = None)
             = new TsvSource(threshold,params,baseUrl,isBulk,label)
}

class TsvSource(threshold : Long,
                params: String = "",
                baseUrl: String,
                isBulk: Boolean = false,
                label: Option[String] = None) extends GraphStage[FlowShape[Downloader.Token, PushedTsv]] with DataToolsLogging {

  val in = Inlet[Downloader.Token]("Map.in")
  val out = Outlet[PushedTsv]("Map.out")
  override val shape = FlowShape.of(in, out)

  val consumeLengthHint = Some(100)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {

    private var initialToken : Token = _
    private var currToken: Token = _
    private var buf: mutable.Queue[Option[(Token, TsvData)]] = mutable.Queue()
    private var consumeComplete = false
    private var remainingInfotons : Option[Long] = None
    private var currConsumeState: ConsumeState = SuccessState(0)
    val uuidsFromCurrentToken = mutable.Set.empty[Uuid]
    val receivedUuids = mutable.Set.empty[Uuid]
    private var currKillSwitch: Option[KillSwitch] = None

    implicit val system: ActorSystem = ActorSystem.create("reactive-tools-system")
    implicit val mat: Materializer = ActorMaterializer()


    private val HttpAddress(protocol, host, port, _) =
      ArgsManipulations.extractBaseUrl(baseUrl)
    private val conn =
      HttpConnections.newHostConnectionPool[Option[_]](host, port, protocol)

    override def preStart(): Unit = {

    }

    override def postStop(): Unit = {
      logger.warn("stpop")
    }


    private def pushHead() = {
      if(buf.isEmpty)
        pullTsv()


      if (buf.nonEmpty)
        push(out,
          buf.dequeue.map({ tokenAndData =>
            PushedTsv(tokenAndData._1, tokenAndData._2, (buf.isEmpty && consumeComplete), remainingInfotons)
          }).get
        )



    }

    private def pullTsv() = {
      if(buf.size < threshold){
        logger.debug(
          s"status message: buffer-size=${buf.size}, will request for more data"
        )

        // Need to think about retry logic here. Do we need it, even?
        sendNextChunkRequest(currToken).map( { nextToken =>
          val decoded = Try(
            new org.joda.time.LocalDateTime(
              Tokens.decompress(currToken).takeWhile(_ != '|').toLong
            )
          )
          logger.debug(s"successfully consumed token: $currToken point in time: ${decoded
            .getOrElse("")} buffer-size: ${buf.size}")
          currConsumeState = ConsumeStateHandler.nextSuccess(currConsumeState)
        }).recover{
          case ex =>
            currConsumeState = ConsumeStateHandler.nextFailure(currConsumeState)
            throw ex
        }


      }
    }

    /**
      * Sends request of next data chunk and fills the given buffer
      * @param token cm-well position token to consume its data
      * @return optional next token value, otherwise None when there is no data left to be consumed
      */
    def sendNextChunkRequest(token: String): Future[Option[String]] = {

      /**
        * Creates http request for consuming data
        * @param token position token to be consumed
        * @param toHint to-hint field of cm-well consumer API
        * @return HTTP request for consuming data
        */
      def createRequestFromToken(token: String, toHint: Option[String] = None) = {
        // create HTTP request from token
        val paramsValue = if (params.isEmpty) "" else s"&$params"

        val (consumeHandler, slowBulk) = currConsumeState match {
          case SuccessState(_) =>
            val consumeHandler = if (isBulk) "_bulk-consume" else "_consume"
            (consumeHandler, "")
          case LightFailure(_, _) =>
            val consumeHandler = if (isBulk) "_bulk-consume" else "_consume"
            (consumeHandler, "&slow-bulk")
          case HeavyFailure(_) =>
            ("_consume", "&slow-bulk")
        }

        val lengthHintStr = consumeLengthHint.fold("") { chunkSize =>
          if (consumeHandler == "_consume") "&length-hint=" + chunkSize
          else ""
        }

        val to = toHint.map("&to-hint=" + _).getOrElse("")

        val uri =
          s"${formatHost(baseUrl)}/$consumeHandler?position=$token&format=tsv$paramsValue$slowBulk$to$lengthHintStr"

        logger.debug("send HTTP request: {}", uri)
        HttpRequest(uri = uri).addHeader(RawHeader("Accept-Encoding", "gzip"))
      }

      uuidsFromCurrentToken.clear()

      val source: Source[Token, (Future[Option[Token]], UniqueKillSwitch)] = {
        val src: Source[(Option[String], Source[(Token, Tsv), Any]), NotUsed] =
          Source
            .single(createRequestFromToken(token, None))
            .map(_ -> None)
            .via(conn)
            .map {
              case (tryResponse, state) =>
                tryResponse.map(HttpZipDecoder.decodeResponse) -> state
            }
            .map {
              case (Success(HttpResponse(s, h, e, _)), _) if s == StatusCodes.TooManyRequests =>
                e.discardBytes()

                logger.error(s"HTTP 429: too many requests token=$token")
                None -> Source.failed(new Exception("too many requests"))

              case (Success(HttpResponse(s, h, e, _)), _) if s == StatusCodes.NoContent =>
                e.discardBytes()
                None -> Source.empty
              case (Success(HttpResponse(s, h, e, _)), _) if s == StatusCodes.OK || s == StatusCodes.PartialContent =>

                remainingInfotons = getNLeft(h) match {
                  case Some(HttpHeader(_, nLeft)) => Some(nLeft.toInt)
                  case _ => None
                }

                val nextToken = getPosition(h) match {
                  case Some(HttpHeader(_, pos)) => pos
                  case None                     => throw new RuntimeException("no position supplied")
                }

                logger.debug(
                  s"received consume answer from host=${getHostnameValue(h)}"
                )

                val dataSource: Source[(Token, Tsv), Any] = e
                  .withoutSizeLimit()
                  .dataBytes
                  .via(lineSeparatorFrame)
                  .map(extractTsv)
                  .map(token -> _)

                Some(nextToken) -> dataSource

              case (Success(HttpResponse(s, h, e, _)), _) =>
                e.toStrict(1.minute).onComplete {
                  case Success(res: HttpEntity.Strict) =>
                    logger
                      .info(
                        s"received consume answer from host=${getHostnameValue(
                          h
                        )} status=$s token=$token entity=${res.data.utf8String}"
                      )
                  case Failure(err) =>
                    logger.error(
                      s"received consume answer from host=${getHostnameValue(h)} status=$s token=$token cannot extract entity",
                      err
                    )
                }

                Some(token) -> Source.failed(new Exception(s"Status is $s"))

              case x =>
                logger.error(s"unexpected message: $x")
                Some(token) -> Source.failed(
                  new UnsupportedOperationException(x.toString)
                )
            }

        val tokenSink = Sink.last[(Option[String], Source[(Token, Tsv), Any])]
        //The below is actually alsoToMat but with eagerCancel = true
        val srcWithSink = Source
          .fromGraph(GraphDSL.create(tokenSink) { implicit builder => sink =>
            import GraphDSL.Implicits._
            val tokenSource = builder.add(src)
            val bcast = builder.add(
              Broadcast[(Option[String], Source[(Token, Tsv), Any])](2, eagerCancel = true)
            )
            tokenSource ~> bcast.in
            bcast.out(1) ~> sink
            SourceShape(bcast.out(0))
          })
          .mapMaterializedValue(_.map { case (nextToken, _) => nextToken })
        srcWithSink
          .map { case (_, dataSource) => dataSource }
          .flatMapConcat(identity)
          .collect {
            case (token, tsv: TsvData) =>
              // if uuid was not emitted before, write it to buffer
              if (receivedUuids.add(tsv.uuid)) {
                buf += Some((token, tsv))
              }

              uuidsFromCurrentToken.add(tsv.uuid)
              token
          }
          .viaMat(KillSwitches.single)(Keep.both)
      }

      val (result, killSwitch) = source
        .toMat(Sink.ignore) {
          case ((token, killSwitch), done) =>
            done.flatMap(_ => token) -> killSwitch
        }
        .run()

      currKillSwitch = Some(killSwitch)

      result
    }


    setHandler(in, new InHandler {
      override def onPush(): Unit = {
        initialToken = grab(in)

        // Start buffer



        println(initialToken)
        pull(in)
      }
    })

/*
    def getNext = {
      buf += Some((new Downloader.Token("aaa"),
        new consumer.Downloader.TsvData(ByteString.empty,ByteString.empty,ByteString.empty,ByteString.empty))
    }
*/





    setHandler(out, new OutHandler {
      override def onPull(): Unit = {
        //pullTsv()
        pushHead()


        pull(in)
      }
    })








  }




}
*/