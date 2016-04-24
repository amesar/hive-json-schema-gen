package org.amm.util

import scala.collection.JavaConverters._
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.core.`type`.TypeReference

object JacksonUtils {
  private val mapper = new ObjectMapper()
  private val typeReferenceMap = new TypeReference[java.util.Map[String, Object]] {}

  def toMap(msg: String) : java.util.Map[String, Object] = {
    mapper.readValue(msg.getBytes, typeReferenceMap)
  }

  def toMap(bytes: Array[Byte]) : java.util.Map[String, Object] = {
    mapper.readValue(bytes, typeReferenceMap)
  }

  def toString(map: java.util.Map[String, Object]) : String = {
    mapper.writeValueAsString(map)
  }

  def getAsMap(map: java.util.Map[String, Object], field: String) : java.util.Map[String, Object] = {
    map.get(field).asInstanceOf[java.util.Map[String, Object]]
  }

  def getAsString(map: java.util.Map[String, Object], oname: String, name: String) : String = {
    val omap = map.get(oname).asInstanceOf[java.util.Map[String, Object]]
    if (omap==null) return null
    omap.get(name).asInstanceOf[String]
  }

}
