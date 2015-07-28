package pl.tzr.oaipmh.client

import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory
import pl.tzr.oaimph.client.{MetadataSet, OaiPmhClient, Record, RecordPage}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scalaz.EitherT._
import scalaz.Scalaz._
import scalaz._

object OaiPmhClientTest extends App {

  implicit val system: ActorSystem = ActorSystem()

  val config = ConfigFactory.load()
  val repositoryUrl = config.getString("oaipmh.url")
  val oaiPmhClient = new OaiPmhClient(repositoryUrl)

  val result: Future[String \/ List[Record]] = (for (
    sets: Seq[MetadataSet] <- eitherT(oaiPmhClient.listSets());
    firstSet: MetadataSet = sets.head;
    recordPage: RecordPage <- eitherT(oaiPmhClient.iterateOverSet(firstSet.spec));
    recordPage2: RecordPage <- eitherT(oaiPmhClient.iterateOverSet(recordPage.resumptionToken.get))
  ) yield { recordPage.items ++ recordPage2.items }).run

  println(Await.result(result, 30 seconds))

  system.shutdown()
}
