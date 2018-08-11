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
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.RawHeader
import akka.stream._
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.stream.stage._
import cmwell.tools.data.downloader.consumer.Downloader._
import cmwell.tools.data.downloader.consumer.BufferedTsvSource.{InfotonSource, ConsumeComplete, SensorOutput}
import cmwell.tools.data.utils.ArgsManipulations
import cmwell.tools.data.utils.ArgsManipulations.{HttpAddress, formatHost}
import cmwell.tools.data.utils.akka.HeaderOps.{getHostnameValue, getNLeft, getPosition}
import cmwell.tools.data.utils.akka.{DataToolsConfig, HttpConnections, lineSeparatorFrame}
import cmwell.tools.data.utils.logging.DataToolsLogging
import cmwell.tools.data.utils.text.Tokens
import cmwell.util.akka.http.HttpZipDecoder

import scala.concurrent.duration._
import scala.collection.mutable
import scala.concurrent.Future
import scala.util.{Failure, Success, Try}
import scala.concurrent.ExecutionContext.Implicits.global

case class ConsumeResponse(token: Option[String], consumeComplete: ConsumeComplete, infotonSource: InfotonSource)

object BufferedTsvSource {

  type InfotonSource = Source[(Token, Tsv), Any]
  type ConsumeComplete = Boolean
  type RemainingInfotons = Option[Long]
  type SensorOutput = ((Token,TsvData), ConsumeComplete, RemainingInfotons)

  val bufferLowWaterMarkDefault : Long = 100
  val consumeLengthHintDefault : Option[Int] = Some(100)
  val retryTimeoutDefault : FiniteDuration = 10.seconds
  val horizonRetryTimeoutDefault : FiniteDuration = 1.minute

  def apply(initialToken: Future[String],
            threshold: Long = bufferLowWaterMarkDefault,
            params: String = "",
            baseUrl: String,
            consumeLengthHint: Option[Int] = consumeLengthHintDefault,
            retryTimeout: FiniteDuration = retryTimeoutDefault,
            horizonRetryTimeout: FiniteDuration = horizonRetryTimeoutDefault,
            isBulk: Boolean = false,
            label: Option[String] = None)
  = new BufferedTsvSource(initialToken,threshold,params,baseUrl,consumeLengthHint,retryTimeout, horizonRetryTimeout, isBulk, label)
}

class BufferedTsvSource(initialToken: Future[String],
                        threshold : Long,
                        params: String = "",
                        baseUrl: String,
                        consumeLengthHint: Option[Int],
                        retryTimeout: FiniteDuration,
                        horizonRetryTimeout: FiniteDuration,
                        isBulk: Boolean,
                        label: Option[String] = None) extends GraphStage[SourceShape[SensorOutput]]
  with DataToolsLogging with DataToolsConfig {

  val out = Outlet[SensorOutput]("sensor.out")
  override val shape = SourceShape(out)

  def isHorizon(consumeComplete: Boolean, buf: mutable.Queue[Option[(Token, TsvData)]]) = consumeComplete && buf.isEmpty

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {

    var callback : AsyncCallback[ConsumeResponse] = _
    var asyncCallInProgress = false

    private var currentConsumeToken: Token = _
    private var buf: mutable.Queue[Option[(Token, TsvData)]] = mutable.Queue()
    private var consumeComplete = false
    private var remainingInfotons : Option[Long] = None
    private var currConsumeState: ConsumeState = SuccessState(0)

    implicit val system: ActorSystem = ActorSystem.create("data-tools")
    implicit val mat: Materializer = ActorMaterializer()

    private val HttpAddress(protocol, host, port, _) =
      ArgsManipulations.extractBaseUrl(baseUrl)
    private val conn =
      HttpConnections.newHostConnectionPool[Option[_]](host, port, protocol)

    override def preStart(): Unit = {

      def bufferFillerCallback(tokenAndTsv : ConsumeResponse) : Unit = {

        consumeComplete = tokenAndTsv.consumeComplete

        tokenAndTsv match {
          case ConsumeResponse(_, true, _) =>
            // We are at consume complete, so we can periodically retry
            logger.info(s"${label} is at horizon. Will retry at consume position ${currentConsumeToken} " +
              s"in ${horizonRetryTimeout}")

            materializer.scheduleOnce(horizonRetryTimeout, () =>
              invokeBufferFillerCallback(sendNextChunkRequest(currentConsumeToken)))

          case ConsumeResponse(token, false, dataSource) =>

            dataSource.toMat(Sink.seq)(Keep.right).run().onComplete {

              case Success(tokensWithTsv) =>

                logger.debug(s"Consuming _consume response for ${label}. Adding ${tokensWithTsv.size} to the buffer")

                tokensWithTsv.foreach {
                  case (token, tsvData: TsvData) => buf += (Some(token, tsvData))
                }

                token.collect{
                  case token =>
                    currentConsumeToken = token

                    val decoded = Try(
                      new org.joda.time.LocalDateTime(
                        Tokens.decompress(currentConsumeToken).takeWhile(_ != '|').toLong
                      )
                    )

                    logger.debug(s"successfully consumed token: $currentConsumeToken point in time: ${
                      decoded
                        .getOrElse("")
                    } buffer-size: ${buf.size}")
                }


                asyncCallInProgress = false

                getHandler(out).onPull()

              case Failure(_)=>
                case e =>
                  logger.error(s"error consuming token: ${e.toString}, buffer-size: ${buf.size}. " +
                    s"Scheduling retry in ${retryTimeout}")

                  materializer.scheduleOnce(retryTimeout, () => new Runnable {
                    override def run(): Unit = invokeBufferFillerCallback(sendNextChunkRequest(currentConsumeToken))
                  })
            }
        }

      }

      initialToken.onComplete({
        case Success(token) => currentConsumeToken = token
        case Failure(e) => logger.error(
          s"failed to obtain token for=${label} ${e.getMessage}",
          throw new RuntimeException(e)
        )
      })

      callback = getAsyncCallback[ConsumeResponse](bufferFillerCallback)

    }

    override def postStop(): Unit = {
      logger.info(s"Buffered TSV Source stopped: buffer-size: ${buf.size}, label: ${label}")
    }

    /**
      * Sends request of next data chunk and fills the given buffer
      * @param token cm-well position token to consume its data
      * @return optional next token value, otherwise None when there is no data left to be consumed
      */
    def sendNextChunkRequest(token: String): Future[ConsumeResponse] = {

      /**
        * Creates http request for consuming data
        * @param token position token to be consumed
        * @param toHint to-hint field of cm-well consumer API
        * @return HTTP request for consuming data
        */
      def createRequestFromToken(token: String, toHint: Option[String] = None) = {
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

        val lengthHintStr = consumeLengthHint.fold("") { chunkSize => "&length-hint=" + chunkSize }

        val uri =
          s"${formatHost(baseUrl)}/$consumeHandler?position=$token&format=tsv$paramsValue$slowBulk$lengthHintStr"

        logger.debug("send HTTP request: {}", uri)
        HttpRequest(uri = uri).addHeader(RawHeader("Accept-Encoding", "gzip"))
      }

      val src: Source[ConsumeResponse,Any] =
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

              ConsumeResponse(token = None, consumeComplete = false, infotonSource =
                Source.failed(new Exception("too many requests")))

            case (Success(HttpResponse(s, h, e, _)), _) if s == StatusCodes.NoContent =>
              e.discardBytes()

              logger.info(s"No more data available, setting consume complete to true")

              ConsumeResponse(token=None, consumeComplete = true, infotonSource = Source.empty)

            case (Success(HttpResponse(s, h, e, _)), _) if s == StatusCodes.OK || s == StatusCodes.PartialContent =>

              remainingInfotons = getNLeft(h) match {
                case Some(HttpHeader(_, nLeft)) => Some(nLeft.toInt)
                case _ => None
              }

              val nextToken = getPosition(h) match {
                case Some(HttpHeader(_, pos)) => pos
                case None                     => throw new RuntimeException("no position supplied")
              }

              logger.debug(s"received consume answer from host=${getHostnameValue(h)}")

              val infotonSource: Source[(Token, Tsv), Any] = e
                .withoutSizeLimit()
                .dataBytes
                .via(lineSeparatorFrame)
                .map(extractTsv)
                .map(token -> _)

              ConsumeResponse(token=Some(nextToken), consumeComplete = false, infotonSource = infotonSource)

            case (Success(HttpResponse(s, h, e, _)), _) =>
              e.toStrict(1.minute).onComplete {
                case Success(res: HttpEntity.Strict) =>
                  currConsumeState = ConsumeStateHandler.nextSuccess(currConsumeState)

                  logger
                    .info(
                      s"received consume answer from host=${getHostnameValue(
                        h
                      )} status=$s token=$token entity=${res.data.utf8String}"
                    )
                case Failure(err) =>
                  currConsumeState = ConsumeStateHandler.nextFailure(currConsumeState)

                  logger.error(
                    s"received consume answer from host=${getHostnameValue(h)} status=$s token=$token cannot extract entity",
                    err
                  )
              }

              e.discardBytes()

              ConsumeResponse(token=Some(token), consumeComplete = false, infotonSource =
                Source.failed(new Exception(s"Status is $s")))

            case x =>
              logger.error(s"unexpected response: $x")

              ConsumeResponse(token=Some(token), consumeComplete = false, infotonSource =
                Source.failed(
                  new UnsupportedOperationException(x.toString)
                ))
          }

      src.toMat(Sink.head)(Keep.right).run

    }

    setHandler(out, new OutHandler {

      override def onDownstreamFinish(): Unit = {
        logger.info(s"demand ended. items in buffer: " +
          s"${buf.size}, token: $currentConsumeToken")
        super.onDownstreamFinish
      }

      override def onPull(): Unit = {

          if (buf.nonEmpty && isAvailable(out)){
          buf.dequeue().foreach(tokenAndData=>{
            val sensorOutput =  ((tokenAndData._1, tokenAndData._2), isHorizon(consumeComplete,buf), remainingInfotons)
            logger.debug(s"successfully de-queued tsv: $currentConsumeToken remaining buffer-size: ${buf.size}")
            push(out,sensorOutput)
          })
        }

        if(buf.size < threshold && !asyncCallInProgress && currentConsumeToken !=null ){
          logger.debug(s"buffer size: ${buf.size} is less than threshold of $threshold. Requesting more tsvs")
          invokeBufferFillerCallback(sendNextChunkRequest(currentConsumeToken))
        }
      }
    })

    private def invokeBufferFillerCallback(future: Future[ConsumeResponse]): Unit = {
      asyncCallInProgress = true
      future.onComplete{
        case Success(consumeResponse) => callback.invoke(consumeResponse)
        case Failure(ex) => {
          logger.error(s"TSV future failed: ${ex.toString}")
        }
      }(materializer.executionContext)
    }

  }

}
