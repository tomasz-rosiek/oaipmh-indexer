package pl.tzr.oaimph.client.akka

import akka.actor.{ActorRef, Props}
import akka.stream.actor.ActorPublisher
import akka.stream.actor.ActorPublisherMessage.{Cancel, Request}
import akka.util.Timeout
import pl.tzr.oaimph.client.Record
import scala.concurrent.duration._

class OaiPmhStreamActor(url : String, setSpec : String) extends ActorPublisher[Record] {

  implicit val timeout = Timeout(5 seconds)

  var oaiPmhClient: ActorRef = context.actorOf(Props(new OaiPmhActor(url)))
  oaiPmhClient ! ListRecordsRequest(setSpec)

  var iteratorClient : ActorRef = null

  var ongoingRequest = false

  override def receive: Receive = {
    case Request(elements) =>
      requestNewRecord()
    case Cancel =>
      if (iteratorClient != null) iteratorClient ! CancelRequest
    case ListRecordsResponse(ref) =>
      iteratorClient = ref
      requestNewRecord()
    case NextRecordResponse(record) =>
      ongoingRequest = false
      onNext(record)
      requestNewRecord()
    case NoMoreRecordsResponse =>
      onComplete()
  }

  private def requestNewRecord(): Unit = {
    if (iteratorClient != null && isActive && totalDemand > 0 && !ongoingRequest) {
      ongoingRequest = true
      iteratorClient ! NextRecordRequest
    }
  }
}
