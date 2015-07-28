package pl.tzr.oaimph.client.actors

import akka.actor.{ActorLogging, Actor, ActorRef, Props}
import _root_.akka.event.Logging
import pl.tzr.oaimph.client._
import pl.tzr.oaimph.client.actors.OaiPmhActor._

import scala.concurrent.ExecutionContext.Implicits.global
import scalaz.{-\/, \/-}

object OaiPmhActor {

  case object ListSetsRequest
  case class ListSetsResponse(items: Seq[MetadataSet])
  case class ListSetsFailureResponse(message: String)
  case class ListRecordsRequest(setSpec: String)
  case class ListRecordsResponse(iteratorRef: ActorRef)

}

class OaiPmhActor(serverUrl: String) extends Actor with ActorLogging {

  implicit val actorSystem = context.system

  val oaiPmhClient = new OaiPmhClient(serverUrl)

  def receive = {
    case ListSetsRequest => listSets(sender())
    case ListRecordsRequest(setSpec) => listRecords(setSpec, sender())
  }

  private def listSets(sender: ActorRef) = {
    oaiPmhClient.listSets().onSuccess {
      case \/-(items: Seq[MetadataSet]) => sender ! ListSetsResponse(items)
      case -\/(message: String) => sender ! ListSetsFailureResponse(message)
    }
  }

  private def listRecords(setSpec: String, sender: ActorRef) = {
    val iteratorRef = context.actorOf(Props(new OaiPmhIteratorActor(serverUrl, setSpec)))
    sender ! ListRecordsResponse(iteratorRef)
  }

}
