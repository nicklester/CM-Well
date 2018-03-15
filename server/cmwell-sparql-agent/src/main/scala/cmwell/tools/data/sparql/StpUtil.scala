package cmwell.tools.data.sparql

import cmwell.tools.data.utils.akka.stats.DownloaderStats.DownloadStats
import cmwell.tools.data.utils.akka.stats.IngesterStats.IngestStats
import cmwell.util.http.SimpleResponse.Implicits.UTF8StringHandler
import io.circe._, io.circe.parser._
import scala.concurrent.ExecutionContext

object StpUtil {
  def headerString(header: (String, String)): String = header._1 + ":" + header._2

  def headersString(headers: Seq[(String, String)]): String = headers.map(headerString).mkString("[", ",", "]")

  def extractLastPart(path: String) = {
    val p = if (path.endsWith("/")) path.init
    else path
    val (_, name) = p.splitAt(p.lastIndexOf("/"))
    name.tail
  }

  def readIngestStatistics(baseUrl: String, path: String)(implicit context : ExecutionContext) = {
    cmwell.util.http.SimpleHttpClient.get(s"http://$baseUrl$path/stats?format=json")
      .map(response => {
        response.payload.lines.map({ row =>
          parse(row) match {
            case Left(parseFailure@ParsingFailure(_, _)) => throw parseFailure
            case Right(json) => {

              val ingestedInfotons = json.hcursor.downField("fields").downField("ingestedInfotons").downArray.as[Long].toOption
              val failedInfotons = json.hcursor.downField("fields").downField("failedInfotons").downArray.as[Long].toOption

              (ingestedInfotons, failedInfotons) match {
                case (Some(ingested), Some(failed)) => IngestStats(ingestedInfotons=ingested, failedInfotons=failed)
                case _ => None
              }
            }
          }
        })
      })
  }


  def readPreviousTokens(baseUrl: String, path: String, format: String)(implicit context : ExecutionContext) = {
    cmwell.util.http.SimpleHttpClient.get(s"http://$baseUrl$path/tokens?op=stream&recursive&format=json")
      .map(response =>{
        response.payload.lines.map( {row =>
          parse(row) match {
            case Left(parseFailure@ParsingFailure(_, _)) => throw parseFailure
            case Right(json) => {

              val token = json.hcursor.downField("fields").downField("token").values.get(0).asString.get

              val receivedInfotons : Option[DownloadStats] = json.hcursor.downField("fields").downField("receivedInfotons").downArray.as[Long].toOption.map{ value =>
                DownloadStats(receivedInfotons=value)
              }

              val sensor = extractLastPart(json.hcursor.downField("system").get[String]("path").toOption.get)
              sensor -> (token,receivedInfotons,None)
            }
          }
        }).foldLeft(Map.empty[String,TokenAndStatistics])(_ + _)
      })
  }


  def loadIngestStatsFromZStore() = {

  }



}
