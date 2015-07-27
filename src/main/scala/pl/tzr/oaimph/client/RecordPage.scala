package pl.tzr.oaimph.client

case class RecordPage(items : List[Record], resumptionToken : Option[ResumptionToken])
