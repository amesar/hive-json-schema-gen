package org.amm.hiveschema

object DataType extends Enumeration {
  type DataType = Value
  val STRING, INT, LONG, DOUBLE, FLOAT, BOOLEAN, OBJECT, ARRAY, NULL = Value
}

class Schema(name: String) {
  def this() = this("root")
  val _root = new ObjectDef(name)
  def root = _root
  override def toString = " root: "+root
}

object Schema {
  import org.amm.hiveschema.DataType._

  val typeMappings = Map(
    "java.lang.String" -> DataType.STRING, 
    "java.lang.Boolean" -> DataType.BOOLEAN, 
    "java.lang.Integer" -> DataType.INT, 
    "java.lang.Long" -> DataType.LONG,
    "java.lang.Double" -> DataType.DOUBLE,
    "java.lang.Float" -> DataType.DOUBLE,
    "java.util.LinkedHashMap" -> DataType.OBJECT,
    "java.util.ArrayList" -> DataType.ARRAY
 )
  def toDataType(obj: Object) : DataType = {
     if (obj==null) return DataType.NULL
     typeMappings(obj.getClass.getName) // TODO: make class instead of string
  }

  def isCompound(dataType: DataType) = dataType==DataType.OBJECT || dataType==DataType.ARRAY
}
