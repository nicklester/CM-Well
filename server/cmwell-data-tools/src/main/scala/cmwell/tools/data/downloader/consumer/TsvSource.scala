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

import akka.stream._
import akka.stream.stage.{GraphStage, GraphStageLogic, OutHandler}
import akka.util.ByteString
import cmwell.tools.data.downloader.consumer.Downloader.{Token, TsvData}

import scala.collection.mutable
import scala.concurrent.Future

object TsvSource {
  def apply(label: Option[String] = None) = new TsvSource(label)
}


class TsvSource(label: Option[String] = None) extends GraphStage[FlowShape[(ByteString,((Token, TsvData), Boolean, Option[Long])),(ByteString, String)]] {

  val in = Inlet[Downloader.Token]("Map.in")
  val out = Outlet[((Token, TsvData), Boolean, Option[Long])]("Map.out")
  override val shape = FlowShape.of(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {

    private var currToken: Token = _
    private val buf: mutable.Queue[Option[(Token, TsvData)]] = mutable.Queue()
    private var consumeComplete = false
    private var remainingInfotons : Option[Long] = None

    override def preStart(): Unit = ???

    setHandler(out, new OutHandler {
      override def onPull(): Unit = {
        pull(in)
      }
    })




  }




}
