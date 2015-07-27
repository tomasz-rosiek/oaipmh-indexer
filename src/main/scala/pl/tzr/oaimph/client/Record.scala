package pl.tzr.oaimph.client

import java.net.URI

case class Record(identifier : URI, attributes : Map[String, List[String]])
