package org.amm.hiveschema

import scala.collection.JavaConverters._

class JsonParser extends Parser {
  def parse(map: java.util.Map[String, Object], schema: Schema) {
    parseMap("", map, schema.root)
  }

  def parseMap(k: String, map: java.util.Map[String, Object], odef: ObjectDef, level: Int=0) {
    val name = if (k=="") "" else ""+k+": "
    for ((k,v) <- map.asScala) {
       parseObject(k,v,odef,level)
    }
  }

  def parseObject(k: String, obj: Object, pdef: CompoundDef, level: Int=0) {
    obj match {
      case x: java.util.Map[String, Object] => {
        val odef2 = new ObjectDef(k)
        pdef.addField(k, odef2)
        parseMap(k,x,odef2,level+1)
      }
      case x: java.util.ArrayList[Object] => {
        val adef = new ArrayDef(k)
        pdef.addField(k, adef)
        parseList(k,x,adef,level+1)
      }
      case _ => {
        pdef.addField(k, Schema.toDataType(obj))
      }
    }
  }

  def parseList(k: String, list: java.util.ArrayList[Object], adef: ArrayDef, level: Int=0) {
    for (e <- list.asScala) {
      val dtype = Schema.toDataType(e)
      adef.addElementDataType(dtype)
      parseObject("",e,adef,level+1)
    }
  }
}
