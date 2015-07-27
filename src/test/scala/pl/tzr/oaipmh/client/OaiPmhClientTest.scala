package pl.tzr.oaipmh.client

import pl.tzr.oaimph.client.{MetadataSet, OaiPmhClient, Record, RecordPage}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scalaz.EitherT._
import scalaz.Scalaz._
import scalaz._

object OaiPmhClientTest extends App {

  val oaiPmhClient = new OaiPmhClient("http://localhost:8080")

  val result: Future[String \/ List[Record]] = (for (
    sets: Seq[MetadataSet] <- eitherT(oaiPmhClient.listSets());
    firstSet: MetadataSet = sets.head;
    recordPage: RecordPage <- eitherT(oaiPmhClient.iterateOverSet(firstSet.spec));
    recordPage2: RecordPage <- eitherT(oaiPmhClient.iterateOverSet(recordPage.resumptionToken.get))
  ) yield { recordPage.items ++ recordPage2.items }).run

  println(Await.result(result, 30 seconds))

}
