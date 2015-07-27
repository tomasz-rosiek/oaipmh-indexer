package pl.tzr.oaimph.client

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scalaz.OptionT._
import scalaz.Scalaz._

object OaiPmhClientTest extends App {

  val oaiPmhClient = new OaiPmhClient("http://localhost:8080")

  val result: Future[Option[List[Record]]] = (for (
    firstSet: MetadataSet <- optionT(oaiPmhClient.listSets()).map(_.head);
    recordPage: RecordPage <- optionT(oaiPmhClient.iterateOverSet(firstSet.spec));
    recordPage2: RecordPage <- optionT(oaiPmhClient.iterateOverSet(recordPage.resumptionToken.get))
  ) yield {
      recordPage.items ++ recordPage2.items
    }).run

  println(Await.result(result, 30 seconds))

}
