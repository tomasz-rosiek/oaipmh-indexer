package pl.tzr.oaimph.client.actors

import akka.actor.{ActorRef, Props}
import akka.stream.actor.ActorPublisher
import akka.stream.actor.ActorPublisherMessage.{Cancel, Request}
import pl.tzr.oaimph.client.Record
import pl.tzr.oaimph.client.actors.OaiPmhActor.{ListRecordsResponse, ListRecordsRequest}
import pl.tzr.oaimph.client.actors.OaiPmhIteratorActor._

class OaiPmhStreamActor(url: String, setSpec: String) extends ActorPublisher[Record] {

  var iteratorClient: ActorRef = null
  var ongoingRequest = false

  val oaiPmhClient: ActorRef = context.actorOf(Props(new OaiPmhActor(url)))

  oaiPmhClient ! ListRecordsRequest(setSpec)

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
      onCompleteThenStop()
    case NextRecordFailureResponse(cause) =>
      onErrorThenStop(new Exception(cause))
  }

  private def requestNewRecord(): Unit = {
    if (iteratorClient != null && isActive && totalDemand > 0 && !ongoingRequest) {
      ongoingRequest = true
      iteratorClient ! NextRecordRequest
    }
  }
}
