package pl.tzr.oaimph.client

import java.io.{ByteArrayInputStream, InputStreamReader}

import akka.actor.ActorSystem
import pl.tzr.oaipmh.generated._
import spray.client.pipelining._
import spray.http.MediaTypes._
import spray.http.Uri.Query
import spray.http._
import spray.httpx.unmarshalling.Unmarshaller

import scala.concurrent.Future
import scala.xml.{Elem, XML}
import scalaz.OptionT._
import scalaz.Scalaz._

class OaiPmhClient(baseUrl : String) {

  implicit val system = ActorSystem() // execution context for futures

  implicit val NodeSeqUnmarshaller =
    Unmarshaller[Option[OAIu45PMHtype]](`text/xml`, `application/xml`, `application/xhtml+xml`) {
      case HttpEntity.NonEmpty(contentType, data) =>

        val parser = XML.parser
        try {
          parser.setProperty("http://apache.org/xml/properties/locale", java.util.Locale.ROOT)
        } catch {
          case e: org.xml.sax.SAXNotRecognizedException ⇒ // property is not needed
        }
        val elem = XML.withSAXParser(parser).load(new InputStreamReader(new ByteArrayInputStream(data.toByteArray), contentType.charset.nioCharset))
        Some(scalaxb.fromXML[OAIu45PMHtype](elem))
      case HttpEntity.Empty =>
        Option.empty[OAIu45PMHtype]
    }

  val pipeline: HttpRequest => Future[Option[OAIu45PMHtype]] = sendReceive ~> unmarshal[Option[OAIu45PMHtype]]

  def identify(): Future[Option[OAIu45PMHtype]] =
    performOperation(Query("verb" -> "Identify"), x => x)

  def listSets(): Future[Option[Seq[MetadataSet]]] =
    performOperation(Query("verb" -> "ListSets"), parseListSetsResponse)

  def iterateOverSet(setSpec : String): Future[Option[RecordPage]] =
    performOperation(Query("verb" -> "ListRecords", "set" -> setSpec, "metadataPrefix" -> "oai_dc"), parseSetResponse(_).get)

  def iterateOverSet(resumptionToken : ResumptionToken):Future[Option[RecordPage]] =
    performOperation(Query("verb" -> "ListRecords", "resumptionToken" -> resumptionToken.value), parseSetResponse(_).get)

  private def parseSetDetails(content : SetType) : MetadataSet = MetadataSet(content.setSpec, content.setName)

  private def parseListSetsResponse(response : OAIu45PMHtype) : Seq[MetadataSet] = {
    for (
      response <- extractResultByType[ListSetsType](response);
      set <- response.set
    ) yield { parseSetDetails(set) }
  }

  private def performOperation[R](query : Query, resultHandler : (OAIu45PMHtype => R)): Future[Option[R]] =
    optionT(pipeline(Get(Uri(baseUrl).copy(query = query)))).map(resultHandler(_)).run

  private def extractResultByType[T](response : OAIu45PMHtype): Seq[T] = {
    response.oaiu45pmhtypeoption.map(_.value.asInstanceOf[T])
  }

  private def parseSetResponse(response: OAIu45PMHtype): Option[RecordPage] = {
    for (page <- extractResultByType[ListRecordsType](response).headOption) yield {
      val resumptionToken = page.resumptionToken.map(token => ResumptionToken(token.value))
      val records = page.record.toList.map(buildRecord)
      RecordPage(records, resumptionToken)
    }
  }

  private def buildRecord(recordType: RecordType) : Record = {
    val record = scalaxb.fromXML[Oai_dcType](recordType.metadata.get.any.as[Elem])
    val attributes = record.oai_dctypeoption.map(r => r.key.get -> r.value.asInstanceOf[ElementType].value)
    val attributeMap = attributes.groupBy(_._1).mapValues(_.map(_._2).toList)
    Record(recordType.header.identifier, attributeMap)
  }

}