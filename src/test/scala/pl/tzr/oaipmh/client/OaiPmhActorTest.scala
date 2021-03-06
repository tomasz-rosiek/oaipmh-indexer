package pl.tzr.oaipmh.client

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import pl.tzr.oaimph.client.actors.OaiPmhActor.{ListRecordsRequest, ListRecordsResponse}
import pl.tzr.oaimph.client.actors.OaiPmhIteratorActor.{NoMoreRecordsResponse, NextRecordRequest, NextRecordResponse}
import pl.tzr.oaimph.client.actors._

import scala.concurrent.duration._

case class Harvest(url : String, setSpec : String)

class OaiPmhHarvester extends Actor {

  implicit val timeout = Timeout(5 seconds)

  var oaiPmhClient: ActorRef = null
  var iteratorClient : ActorRef = null

  override def receive: Receive = {
    case Harvest(url, setSpec) =>
      oaiPmhClient = context.actorOf(Props(new OaiPmhActor(url)))
      harvestSet(oaiPmhClient, setSpec)
    case ListRecordsResponse(ref) => {
      iteratorClient = ref
      iteratorClient ! NextRecordRequest
    }
    case NextRecordResponse(record) => {
      println(s"Record $record")
      iteratorClient ! NextRecordRequest
    }
    case NoMoreRecordsResponse => {
      println("No more records!")
      context.system.shutdown()
    }
  }

  def harvestSet(oaiPmhClient : ActorRef, setSpec : String): Unit = {
    println("Harvesting set")
    oaiPmhClient ! ListRecordsRequest(setSpec)
  }

}

object OaiPmhActorTest extends App {

  val config = ConfigFactory.load()
  val repositoryUrl = config.getString("oaipmh.url")
  val setSpec = config.getString("oaipmh.set")

  implicit val system = ActorSystem()
  val ref = system.actorOf(Props[OaiPmhHarvester])
  ref ! Harvest(repositoryUrl, setSpec)
  system.awaitTermination()

}
