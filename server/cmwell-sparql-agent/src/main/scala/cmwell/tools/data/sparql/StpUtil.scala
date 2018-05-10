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


import cmwell.tools.data.utils.akka.stats.DownloaderStats.DownloadStats
import cmwell.zstore.ZStore
import io.circe._
import io.circe.parser._
import cmwell.util.concurrent.{RetryParams, ShouldRetry, retryUntil}
import cmwell.util.http.SimpleResponse

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.Duration
import scala.util.{Success, Try}

object StpUtil {
  def headerString(header: (String, String)): String = header._1 + ":" + header._2

  def headersString(headers: Seq[(String, String)]): String = headers.map(headerString).mkString("[", ",", "]")

  def extractLastPart(path: String) = {
    val p =
      if (path.endsWith("/")) path.init
      else path
    val (_, name) = p.splitAt(p.lastIndexOf("/"))
    name.tail
  }


  def readPreviousTokens(baseUrl: String, path: String, format: String, zStore: ZStore)
                        (implicit context: ExecutionContext) = {




    //retryUntil(initialRetryState)(shouldRetry(s"Getting token and statstics state from zCache for agent $path")) {

      zStore.getStringOpt(s"stp-agent-${extractLastPart(path)}", dontRetry = true).map {
        case None => {
          // No such key - start STP from scratch
          Map.newBuilder[String, TokenAndStatistics].result()
        }
        case Some(tokenPayload) => {
          // Key exists and has returned
          tokenPayload.lines.map({
            row =>
              parse(row) match {
                case Left(parseFailure@ParsingFailure(_, _)) => throw parseFailure
                case Right(json) => {

                  val token = json.hcursor.downField("token").as[String].getOrElse("")
                  val sensor = json.hcursor.downField("sensor").as[String].getOrElse("")
                  val receivedInfotons = json.hcursor.downField("receivedInfotons").downArray.as[Long].toOption.map {
                    value => DownloadStats(receivedInfotons = value)
                  }

                  sensor -> (token, receivedInfotons)
                }
              }
          })
          .foldLeft(Map.newBuilder[String, TokenAndStatistics])(_.+=(_))
          .result()
        }
      }

  }

}



