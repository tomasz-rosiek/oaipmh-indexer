package pl.tzr.oaipmh.client

import akka.actor.{ActorSystem, Props}
import akka.stream.ActorMaterializer
import akka.stream.actor.ActorPublisher

import akka.stream.scaladsl.{Sink, Source, FlowGraph, Flow}
import pl.tzr.oaimph.client.Record
import pl.tzr.oaimph.client.akka.OaiPmhStreamActor


object OaiPmhStreamingTest extends App {

  implicit val system = ActorSystem()
  implicit val materializer = ActorMaterializer()
  val ref = system.actorOf(Props(new OaiPmhStreamActor("REPO", "SET SPEC")))

  val src = Source(ActorPublisher(ref))

  val converter = Flow[Record].map(_.toString)
  val sink = Sink.foreach[String](println)
  val runnableFlow = src.via(converter).to(sink)

  runnableFlow.run
 }
