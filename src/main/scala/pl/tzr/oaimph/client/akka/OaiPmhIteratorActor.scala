package pl.tzr.oaimph.client.akka

import _root_.akka.actor.{Actor, ActorRef}
import _root_.akka.event.Logging
import _root_.akka.pattern.pipe
import pl.tzr.oaimph.client._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}
import scalaz.{\/, -\/, \/-}

case object NextRecordRequest
case object CancelRequest
case class NextRecordResponse(record : Record)
case object NoMoreRecordsResponse
case class NextRecordFailureResponse(message : String)
case object CancelResponse

case class NextPage(sender : ActorRef, page : (String \/ RecordPage))

class OaiPmhIteratorActor(serverUrl : String, setSpec : String) extends Actor {

  sealed trait IterationState
  case object FirstStep extends IterationState
  case class NextStep(token : ResumptionToken) extends IterationState
  case object LastStep extends IterationState

   val oaiPmhClient = new OaiPmhClient(serverUrl)

   val log = Logging(context.system, this)

   var lastToken : IterationState = FirstStep
   var lastResults : List[Record] = Nil


  override def receive: Receive = {
     case NextRecordRequest => next(sender())
     case CancelRequest => cancel(sender())
     case NextPage(sender, x) => handleNextPage(sender, x)
     case _ => log.info("received unknown message")
   }

   def next(sender: ActorRef): Unit = {
     if (lastResults.isEmpty) askForNextPage(sender)
     else sendNextResult(sender)
   }

   def askForNextPage(sender : ActorRef) = {
     (lastToken match {
       case NextStep(token) => oaiPmhClient.iterateOverSet(token)
       case FirstStep => oaiPmhClient.iterateOverSet(setSpec)
       case LastStep => Future.successful(\/-(RecordPage(Nil, Option.empty)))
     }).map(NextPage(sender, _)).pipeTo(self)
   }

  def handleNextPage(sender : ActorRef, page: String \/ RecordPage) : Unit = {
    page match {
      case \/-(page) =>
        lastResults = page.items
        lastToken = page.resumptionToken.map(NextStep).getOrElse(LastStep)
        if (lastResults.isEmpty) replyNoMoreRecords(sender) else sendNextResult(sender)
      case -\/(message) =>
        replyFailure(sender, message)
    }
  }

  def replyNoMoreRecords(sender: ActorRef) = {
    sender ! NoMoreRecordsResponse
    context.stop(self)
  }

  def replyFailure(sender : ActorRef, message : String) = {
    sender ! NextRecordFailureResponse(message)
    context.stop(self)
  }

  def sendNextResult(sender: ActorRef) = {
     sender ! NextRecordResponse(lastResults.head)
     lastResults = lastResults.tail
   }

   def cancel(ref: ActorRef): Unit = {
     //TODO dispose the execution
   }
 }