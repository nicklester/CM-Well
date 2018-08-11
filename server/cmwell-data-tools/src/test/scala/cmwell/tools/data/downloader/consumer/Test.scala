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
import akka.stream.{ActorMaterializer, Materializer}
import akka.stream.scaladsl.{Keep, Source}
import akka.stream.scaladsl.Sink

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

import scala.concurrent.ExecutionContext.Implicits.global

object Test {

  implicit val system: ActorSystem = ActorSystem.create("reactive-tools-system")
  implicit val mat: Materializer = ActorMaterializer()


  def main(args: Array[String]) : Unit = {
    val initTokenFuture = Future {  new Downloader.Token("jgCkeJxtjbEKwjAUAH_GsSYvCda" +
      "SQRBUcNDJSXEINqaFJK80L0pLP97qLBzcdJxYKbWWoqxkWamJ14YMowZDwtjbTLZP7IFhutHQWbbw41kctlo3RJ3mHCOhRz" +
      "f8KXi078SPZEORhjSLeZPohHX7bG29kSCqJXy5AOgfDACuhTfRZePm1_7VjTun-3z_ACwgN6Y") }
    val testSource = Source.fromGraph(BufferedTsvSource(initialToken = initTokenFuture, label = Some("df"),
      baseUrl = s"http://cm-well-ppe.int.thomsonreuters.com", retryTimeout = 10.seconds, threshold = 400, consumeLengthHint = Some(500)))

    //val result = testSource.toMat(Sink.seq)(Keep.right).run()
    val result = testSource.take(3).toMat(Sink.seq)(Keep.right).run()
   // val result = testSource.to(Sink.seq).run()


    result.foreach {
      r => {
        r.foreach {
          d=> println("f") // scalastyle:ignore
        }
      }
    }

  }

}
