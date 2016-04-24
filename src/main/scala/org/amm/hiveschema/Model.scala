package org.amm.hiveschema

import org.amm.hiveschema.DataType._
import scala.collection.mutable.{HashSet,LinkedHashMap}

class FieldDef(val name: String, val dataType: DataType) {
  val _dataTypes = new HashSet[DataType]() // to handle polymorphism, a field can have more than one type
  _dataTypes.add(dataType)

  def dataTypes : Set[DataType] = {
   val set = _dataTypes.toSet.filter(x => x != DataType.NULL)

   // TODO: make type promotion pluggable
   val nset = if (set.contains(DataType.INT) && set.contains(DataType.LONG)) {
     val tset = HashSet(set.toSeq: _*)
     tset.remove(DataType.INT) 
     tset.toSet
   } else set
   nset
  }

  def addDataType(dtype: DataType) = _dataTypes.add(dtype)

  override def toString = "name="+name+" dataTypes="+dataTypes
}

class CompoundDef(name: String, dataType: DataType) extends FieldDef(name,dataType) {
  val fieldDefs = new LinkedHashMap[String,FieldDef]()

  def isEmpty : Boolean = {
    if (fieldDefs.size==0) return true;
    for ((key, fdef) <- fieldDefs) {
      if (fdef.dataType==DataType.ARRAY) {
        val adef = fdef.asInstanceOf[ArrayDef]
        if (adef.elementDataTypes.size > 0) return false
      } else {
        if (fdef.dataType!=DataType.NULL) return false
      }
    }
    return true
  }

  def addField(name: String, dtype: DataType) {
    val vopt = fieldDefs.get(name)
    vopt match {
       case Some(fdef) => fdef.addDataType(dtype) // add to existing field
       case None => fieldDefs.put(name,new FieldDef(name,dtype)) // new field
    }
  }

  def addField(name: String, fdef: CompoundDef) = fieldDefs.put(name,fdef)

  override def toString = "[class="+this.getClass.getSimpleName+" name="+name+" #fieldDefs="+fieldDefs.size+"]"
}

class ObjectDef(name: String) extends CompoundDef(name,DataType.OBJECT) 

class ArrayDef(name: String) extends CompoundDef(name,DataType.ARRAY) {
  val elementDataTypes = new HashSet[DataType]()

  def addElementDataType(dtype: DataType) = elementDataTypes.add(dtype)

  override def toString = "[class="+this.getClass.getSimpleName+" name="+name+" #fieldDefs="+fieldDefs.size+" elementDataTypes"+elementDataTypes+"]"
}
