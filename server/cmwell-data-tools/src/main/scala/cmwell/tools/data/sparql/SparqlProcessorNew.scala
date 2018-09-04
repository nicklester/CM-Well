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
package cmwell.tools.data.sparql

import akka.actor.ActorSystem
import akka.stream.Materializer
import akka.stream.scaladsl.{Flow, Source}
import akka.util.ByteString
import cmwell.tools.data.downloader.DataPostProcessor
import cmwell.tools.data.sparql.SparqlProcessorNew.Paths
import cmwell.tools.data.utils.akka.{balancer, blank, lineSeparatorFrame}
import cmwell.tools.data.utils.logging.DataToolsLogging
import controllers._
import k.grid.Grid
import logic.CRUDServiceFS

import scala.annotation.tailrec
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}


object SparqlProcessorNew {

  type Paths = Seq[ByteString]

  implicit class ByteStringUtils(b: ByteString) {
    def trim() = {
      @tailrec
      def iterateRight(b: ByteString): ByteString = {
        if (b.isEmpty) b
        else if (b.last <= ' ') iterateRight(b.init)
        else b
      }

      iterateRight(b.dropWhile(_ <= ' '))
    }

    def split(delimiter: Byte) = {
      @tailrec
      def splitByteString(bytes: ByteString, delimiter: Byte, acc: Seq[ByteString]): Seq[ByteString] = {
        bytes.splitAt(bytes.indexOf(delimiter)) match {
          case (split, rest) if split.isEmpty => acc :+ rest
          case (split, rest)                  => splitByteString(rest.tail, delimiter, acc :+ split)
        }
      }

      splitByteString(b, delimiter, Seq.empty[ByteString])
    }
  }

  def createSparqlSourceFromPaths[T](
      baseUrl: String,
      format: Option[String] = None,
      parallelism: Int = 4,
      isNeedWrapping: Boolean = true,
      sparqlQuery: String,
      source: Source[(ByteString, Option[T]), _],
      label: Option[String] = None)(implicit system: ActorSystem, mat: Materializer, ec: ExecutionContext) = {

    new SparqlProcessorNew(
      baseUrl = baseUrl,
      parallelism = parallelism,
      sparqlQuery = sparqlQuery,
      isNeedWrapping = isNeedWrapping,
      format = format,
      source = source,
      label = label
    ).createSource()
  }

}

class SparqlProcessorNew[T](baseUrl: String,
                         parallelism: Int = 4,
                         isNeedWrapping: Boolean = true,
                         sparqlQuery: String,
                         format: Option[String] = None,
                         source: Source[(ByteString, Option[T]), _],
                         override val label: Option[String] = None) extends DataToolsLogging {

  val endl = ByteString("\n", "UTF-8")

  val parser = new SPParser

  def getRequestParameters (paths: Paths, spQueryParamsBuilder: Seq[String] => Map[String, String]) = {
    RequestParameters(format = "nquads",
      quads=true,
      verbose=false,
      showGraph=false,
      forceUsingFile = false,
      disableImportsCache = true,
      execLogging=false,
      doNotOptimize = false,
      intermediateLimit = 0L,
      resultsLimit = 0L,
      explainOnly = false,
      bypassCache = true,
      customParams = spQueryParamsBuilder(paths.map(_.utf8String))
    )
  }

  def createSource()(implicit system: ActorSystem, mat: Materializer, ec: ExecutionContext) = {

    val crudServiceFS = new CRUDServiceFS()(implicitly,Grid.system)
    val jarsImporter = new JarsImporter(crudServiceFS)
    val queriesImporter = new QueriesImporter(crudServiceFS)
    val sourcesImporter = new SourcesImporter(crudServiceFS)

    def sparqlFlow() = {

      Flow[(Seq[ByteString], Option[T])]
        .map {
          case (data: Paths, context) =>
            val startTime = System.currentTimeMillis
            data -> Some(context -> startTime)
          }
          .map {
            case (paths, contextAndStartTime) =>
                Try(parser.parseQuery(sparqlQuery.filterNot(_ == '\r'))) match {

                case Success(fun) =>
                  Try(fun(getRequestParameters(paths = paths,
                         spQueryParamsBuilder = (p: Seq[String]) => {
                            Map("sp.pid" -> p.head.substring(p.head.lastIndexOf('-') + 1),
                              "sp.path" -> p.head.substring(p.head.lastIndexOf('/') + 1))
                         }))
                  ) match {
                    case Success(paq) =>
                      val evaluated = paq.evaluate(jarsImporter, queriesImporter, sourcesImporter)

                      evaluated.map { queryResult =>

                        val processedData = DataPostProcessor
                          .postProcessByFormat(SparqlProcessor.format, Source.single(ByteString(queryResult._1)))
                          .via(lineSeparatorFrame)
                          .filter(_.nonEmpty)

                        contextAndStartTime -> processedData
                      }

                    case Failure(ex) =>
                      logger.warn("Failure " + ex)
                      Future.successful(contextAndStartTime -> Source.empty)

                  }

                case Failure(ex) =>
                  logger.warn("Failure " + ex)
                  Future.successful(contextAndStartTime -> Source.empty)
              }
        }.mapAsyncUnordered(parallelism)( future =>
          future.map {
            case (Some((context,startTime)), dataLines) =>
              logger.debug("received response from sparql and startTime={}", startTime)
              dataLines -> context
            case x =>
              logger.info(s"unexpected message: $x")
              Source.empty -> None
          }
        )
        .mapAsyncUnordered(parallelism){
          case (dataLines, context) =>
            import SparqlProcessorNew.ByteStringUtils
            dataLines.runFold(blank)(_ ++ _ ++ endl).map(_.trim() -> context)
        }
        .filter { case (data, _) => data.nonEmpty }
    }

    source
      .map { case (path, context) => Seq(path) -> context }
      .via(balancer(sparqlFlow(), parallelism))
  }

}













