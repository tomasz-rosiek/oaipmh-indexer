package pl.tzr.oaimph.client

import java.io.{ByteArrayInputStream, InputStreamReader}

import _root_.akka.actor.ActorSystem
import pl.tzr.oaipmh.generated._
import spray.client.pipelining._
import spray.http.MediaTypes._
import spray.http.Uri.Query
import spray.http._
import spray.httpx.unmarshalling.Unmarshaller

import scala.concurrent.Future
import scala.xml.{Elem, XML}

import scalaz._
import scalaz.Scalaz._

class OaiPmhClient(baseUrl : String) {

  implicit val system = ActorSystem() // execution context for futures

  implicit val NodeSeqUnmarshaller =
    Unmarshaller[OAIu45PMHtype](`text/xml`, `application/xml`, `application/xhtml+xml`) {
      case HttpEntity.NonEmpty(contentType, data) =>

        val parser = XML.parser
        try {
          parser.setProperty("http://apache.org/xml/properties/locale", java.util.Locale.ROOT)
        } catch {
          case e: org.xml.sax.SAXNotRecognizedException ⇒ // property is not needed
        }
        val elem = XML.withSAXParser(parser).load(new InputStreamReader(new ByteArrayInputStream(data.toByteArray), contentType.charset.nioCharset))
        scalaxb.fromXML[OAIu45PMHtype](elem)
    }

  val pipeline: HttpRequest => Future[OAIu45PMHtype] = sendReceive ~> unmarshal[OAIu45PMHtype]

  def identify(): Future[String \/ OAIu45PMHtype] =
    performOperation(Query("verb" -> "Identify"), x => \/-(x))

  def listSets(): Future[String \/ Seq[MetadataSet]] =
    performOperation(Query("verb" -> "ListSets"), parseListSetsResponse)

  def iterateOverSet(setSpec : String): Future[String \/ RecordPage] =
    performOperation(Query("verb" -> "ListRecords", "set" -> setSpec, "metadataPrefix" -> "oai_dc"), parseSetResponse)

  def iterateOverSet(resumptionToken : ResumptionToken):Future[String \/ RecordPage] =
    performOperation(Query("verb" -> "ListRecords", "resumptionToken" -> resumptionToken.value), parseSetResponse)

  private def parseSetDetails(content : SetType) : MetadataSet = MetadataSet(content.setSpec, content.setName)

  private def parseListSetsResponse(response : OAIu45PMHtype) : String \/ Seq[MetadataSet] = {
    extractResultByType[ListSetsType](response).map(_.head.set.map(parseSetDetails))
  }

  private def performOperation[R](query : Query, resultHandler : (OAIu45PMHtype => String \/ R)): Future[String \/ R] =
    pipeline(Get(Uri(baseUrl).copy(query = query))).map(resultHandler)


  private def extractResultByType[T](response : OAIu45PMHtype): String \/ List[T] = {
    val result: List[String \/ T] = response.oaiu45pmhtypeoption.map(_.value).toList.collect {
      case error: OAIu45PMHerrorType => -\/(error.toString)
      case x if x.isInstanceOf[T] => \/-(x.asInstanceOf[T])
      case unknown => -\/(s"Unknown content $unknown")
    }
    result.partition(_.isLeft) match {
      case (Nil,  list) => \/-(for (\/-(x) <- list) yield x)
      case (errors, _) => -\/((for (-\/(x) <- errors) yield x).mkString(","))
    }
  }

  private def parseSetResponse(response: OAIu45PMHtype): String \/ RecordPage = {
    for (page <- extractResultByType[ListRecordsType](response)) yield {
      val resumptionToken = page.head.resumptionToken.filter(!_.value.isEmpty).map(token => ResumptionToken(token.value))
      val records = page.head.record.toList.map(buildRecord)
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