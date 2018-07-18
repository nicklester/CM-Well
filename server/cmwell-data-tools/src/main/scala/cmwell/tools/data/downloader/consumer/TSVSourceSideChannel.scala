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
import akka.pattern.after
import akka.stream._
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.stream.stage._
import cmwell.tools.data.downloader.consumer.BufferFillerActor.FinishedToken
import cmwell.tools.data.downloader.consumer.Downloader._
import cmwell.tools.data.downloader.consumer.TsvSourceSideChannel.DataSource
import cmwell.tools.data.utils.ArgsManipulations
import cmwell.tools.data.utils.ArgsManipulations.{HttpAddress, formatHost}
import cmwell.tools.data.utils.akka.HeaderOps.{getHostnameValue, getNLeft, getPosition}
import cmwell.tools.data.utils.akka.{HttpConnections, lineSeparatorFrame}
import cmwell.tools.data.utils.logging.DataToolsLogging
import cmwell.tools.data.utils.text.Tokens
import cmwell.util.akka.http.HttpZipDecoder

import scala.concurrent.duration._
import scala.collection.mutable
import scala.concurrent.Future
import scala.util.{Failure, Success, Try}
import scala.concurrent.ExecutionContext.Implicits.global

case class PushedTsv(token: Downloader.Token, tsv: Downloader.TsvData, horizon: Boolean, remaining: Option[Long])




object TsvSourceSideChannel {

  type DataSource = Source[(Token, Tsv), Any]

  def apply(threshold: Long = 100,
            params: String = "",
            isBulk: Boolean = false,
            baseUrl: String,
            consumeLengthHint: Option[Int] = Some(100),
            retryTimeout: FiniteDuration,
            label: Option[String] = None)
  = new TsvSourceSideChannel(threshold,params,baseUrl,consumeLengthHint,retryTimeout,label)
}

class TsvSourceSideChannel(threshold : Long,
                params: String = "",
                baseUrl: String,
                consumeLengthHint: Option[Int],
                retryTimeout: FiniteDuration,
                label: Option[String] = None) extends GraphStage[FlowShape[Downloader.Token, ((Token,TsvData),Boolean,Option[Long])]] with DataToolsLogging {

  val in = Inlet[Downloader.Token]("Map.in")
  val out = Outlet[((Token,TsvData),Boolean,Option[Long])]("Map.out")
  override val shape = FlowShape.of(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {

    var callback: AsyncCallback[(Option[String],DataSource)] = _
    var asyncCallInProgress = false

    private var initialToken : Token = _
    private var currToken: Token = _
    private var buf: mutable.Queue[Option[(Token, TsvData)]] = mutable.Queue()
    private var consumeComplete = false
    private var remainingInfotons : Option[Long] = None

    implicit val system: ActorSystem = ActorSystem.create("reactive-tools-system")
    implicit val mat: Materializer = ActorMaterializer()

    private val HttpAddress(protocol, host, port, _) =
      ArgsManipulations.extractBaseUrl(baseUrl)
    private val conn =
      HttpConnections.newHostConnectionPool[Option[_]](host, port, protocol)

    override def preStart(): Unit = {

      def bufferFillerCallback(tokenAndTsv : (Option[String],DataSource)): Unit = {

        tokenAndTsv._2.runForeach {
          case (a:Token,b:TsvData) => buf+=(Some(a,b))
        }.andThen{
          case Success(_)=> tokenAndTsv._1.foreach{currToken=_}
        }.andThen{
          case Success(_) => {
            val decoded = Try(
              new org.joda.time.LocalDateTime(
                Tokens.decompress(currToken).takeWhile(_ != '|').toLong
              )
            )
            logger.debug(s"successfully consumed token: $currToken point in time: ${decoded
              .getOrElse("")} buffer-size: ${buf.size}")
          }
        }.recover {
          case e =>
            logger.error(e.toString)

            materializer.scheduleOnce(retryTimeout, () =>
              invokeBufferFillerCallback(sendNextChunkRequest(initialToken)))

        }

        asyncCallInProgress = false

        this.getHandler(out).onPull()
      }

      callback = getAsyncCallback[(Option[String],DataSource)](bufferFillerCallback)

      invokeBufferFillerCallback(sendNextChunkRequest(initialToken))

    }

    override def postStop(): Unit = {
      logger.warn("stpop")
    }

    /**
      * Sends request of next data chunk and fills the given buffer
      * @param token cm-well position token to consume its data
      * @return optional next token value, otherwise None when there is no data left to be consumed
      */
    def sendNextChunkRequest(token: String): Future[(Option[String], DataSource)] = {

      /**
        * Creates http request for consuming data
        * @param token position token to be consumed
        * @param toHint to-hint field of cm-well consumer API
        * @return HTTP request for consuming data
        */
      def createRequestFromToken(token: String, toHint: Option[String] = None) = {
        val paramsValue = if (params.isEmpty) "" else s"&$params"

        val lengthHintStr = consumeLengthHint.fold("") { chunkSize => "&length-hint=" + chunkSize }

        val uri =
          s"${formatHost(baseUrl)}/_consume?position=$token&format=tsv$paramsValue$lengthHintStr"

        logger.debug("send HTTP request: {}", uri)
        HttpRequest(uri = uri).addHeader(RawHeader("Accept-Encoding", "gzip"))
      }

      val src: Source[(Option[String], DataSource),Any] =
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

              //None -> Future.failed(new Exception("too many requests"))
              None -> Source.failed(new Exception("too many requests"))

            case (Success(HttpResponse(s, h, e, _)), _) if s == StatusCodes.NoContent =>
              e.discardBytes()
              consumeComplete = true
             //None -> Future.failed(new Exception("empty"))
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

             // Some(nextToken) -> dataSource.toMat(Sink.seq)(Keep.right).run()

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

             // Some(token) -> Future.failed(new Exception(s"Status is $s"))
              Some(token) -> Source.failed(new Exception(s"Status is $s"))


            case x =>
              logger.error(s"unexpected message: $x")
              Some(token) -> Source.failed(
                new UnsupportedOperationException(x.toString)
              )
          }

      src.toMat(Sink.head)(Keep.right).run

    }

    setHandler(in, new InHandler {
      override def onPush(): Unit = {
        initialToken = grab(in)
        println(initialToken)
        pull(in)
      }
    })

    setHandler(out, new OutHandler {
      override def onPull(): Unit = {
        if (buf.nonEmpty){
          buf.dequeue().foreach(f=>{
            val d =  ((f._1,f._2),false,remainingInfotons)
            push(out,d)
          })
        }

        if(buf.size < threshold && !asyncCallInProgress){
          invokeBufferFillerCallback(sendNextChunkRequest(currToken))
        }
      }
    })

    private def invokeBufferFillerCallback(future: Future[(Option[String], DataSource)]): Unit = {
      asyncCallInProgress = true
      future.onComplete {
        case Success(tokenWithData) => {
          callback.invoke(tokenWithData._1,tokenWithData._2)
        }
        case Failure(ex) => {
          logger.error(ex.toString)
        }
      }(materializer.executionContext)

    }

  }

}
