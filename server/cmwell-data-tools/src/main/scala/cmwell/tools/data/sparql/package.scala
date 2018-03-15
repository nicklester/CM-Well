package cmwell.tools.data

import cmwell.tools.data.downloader.consumer.Downloader.Token
import cmwell.tools.data.utils.akka.stats.DownloaderStats.DownloadStats
import cmwell.tools.data.utils.akka.stats.IngesterStats.IngestStats

package object sparql {
  type TokenAndStatisticsMap = Map[String, TokenAndStatistics]
  type TokenAndStatistics = (Token,Option[DownloadStats],Option[IngestStats])
}
