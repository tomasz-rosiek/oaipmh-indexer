package pl.tzr.oaipmh.client

import akka.actor.{ActorSystem, Props}
import akka.stream.ActorMaterializer
import akka.stream.actor.ActorPublisher

import akka.stream.scaladsl.{Sink, Source}
import com.sksamuel.elastic4s.{BulkCompatibleDefinition, ElasticDsl, ElasticClient}
import com.sksamuel.elastic4s.streams.RequestBuilder
import com.typesafe.config.ConfigFactory
import pl.tzr.oaimph.client.Record
import pl.tzr.oaimph.client.akka.OaiPmhStreamActor
import com.sksamuel.elastic4s.streams.ReactiveElastic._

object RecordIndexDocumentBuilder extends RequestBuilder[Record] {
  import ElasticDsl._
  def request(t: Record): BulkCompatibleDefinition =  index into "journals" fields t.attributes
}

object OaiPmhStreamingTest extends App {

  val config = ConfigFactory.load()
  val repositoryUrl = config.getString("oaipmh.url")
  val setSpec = config.getString("oaipmh.set")

  implicit val system: ActorSystem = ActorSystem()
  implicit val materializer = ActorMaterializer()
  implicit val builder = RecordIndexDocumentBuilder

  val client: ElasticClient = ElasticClient.local

  val src = Source(ActorPublisher(system.actorOf(Props(new OaiPmhStreamActor(repositoryUrl, setSpec)))))
  val sink = Sink(client.subscriber[Record]())
  val runnableFlow = src.to(sink)

  runnableFlow.run
 }
