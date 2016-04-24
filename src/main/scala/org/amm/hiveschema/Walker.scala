package org.amm.hiveschema

import scala.collection.JavaConverters._

class Walker {
  def walk(map: java.util.Map[String, Object], schema: Schema) {
    walkMap("", map, schema.root)
  }

  def walk(map: java.util.Map[String, Object]) : Schema = {
    val schema = new Schema() 
    walkMap("", map, schema.root)
    schema 
  }

  def walkMap(k: String, map: java.util.Map[String, Object], odef: ObjectDef, level: Int=0) {
    val name = if (k=="") "" else ""+k+": "
    for ((k,v) <- map.asScala) {
       walkObject(k,v,odef,level)
    }
  }

  def walkObject(k: String, obj: Object, pdef: CompoundDef, level: Int=0) {
    obj match {
      case x: java.util.Map[String, Object] => {
        val odef2 = new ObjectDef(k)
        pdef.addField(k, odef2)
        walkMap(k,x,odef2,level+1)
      }
      case x: java.util.ArrayList[Object] => {
        val adef = new ArrayDef(k)
        pdef.addField(k, adef)
        walkList(k,x,adef,level+1)
      }
      case _ => {
        pdef.addField(k, Schema.toDataType(obj))
      }
    }
  }

  def walkList(k: String, list: java.util.ArrayList[Object], adef: ArrayDef, level: Int=0) {
    for (e <- list.asScala) {
      val dtype = Schema.toDataType(e)
      adef.addElementDataType(dtype)
      walkObject("",e,adef,level+1)
    }
  }
}
