package org.amm.hiveschema

import java.io.PrintWriter
import org.amm.hiveschema.DataType._

class HiveSchemaGenerator(table: String, location: String, serde: String, outputFile: String, isExternalTable: Boolean, escapeReservedKeywords: Boolean, buildInfo: String, reservedKeywords: Set[String]) {
  val TAB = "  "
  val outputFilename = if (outputFile==null) table+".ddl" else outputFile
  val pw = new PrintWriter(outputFilename)
 
  if (buildInfo != null) pw.println("-- "+buildInfo)

  val typeMappings = Map(
    DataType.STRING -> "string",
    DataType.BOOLEAN -> "boolean",
    DataType.INT -> "int",
    DataType.LONG -> "bigint",
    DataType.DOUBLE -> "double",
    DataType.FLOAT -> "float",
    DataType.OBJECT -> "struct",
    DataType.ARRAY -> "array"
 )

  def buildHiveSchema(schema: Schema) {
    val etable = if (isExternalTable) "EXTERNAL " else ""
    pw.println("CREATE "+etable+"TABLE "+table+" (")
    buildObjectDef(schema.root)
    pw.println(")")
    pw.println("ROW FORMAT SERDE '"+serde+"'")
    pw.println("LOCATION '"+location+"'")
    pw.close()
  }

  def buildObjectDef(pdef: CompoundDef, indent: String="") {

    for (((fname,fdef),j) <- pdef.fieldDefs.zipWithIndex) {
      val sep = if (j < (pdef.fieldDefs.size-1)) "," else ""

      if (fdef.dataType==OBJECT) {
        val odef = fdef.asInstanceOf[ObjectDef]
        if (odef.isEmpty) {
          pw.println(indent+"  -- struct "+norm(fname,indent)+" is empty")
        } else { 
          pw.println(indent+"  "+norm(fname,indent)+typeMappings(fdef.dataType)+" <")
          buildObjectDef(odef,indent+TAB)
          pw.println(indent+"  >"+sep)
        }

      } else if (fdef.dataType==ARRAY) {
        buildArray(fname, fdef, sep, indent)

      } else { // scalar
        buildScalar(fname, fdef, sep, indent)
      }
    }
  }

  def buildScalar(fname: String, fdef: FieldDef, sep: String, indent: String) {
    val dtypes = fdef.dataTypes
    if (dtypes.size == 0) {
      val htype = "null"
      pw.println(indent+"  -- "+fname+" "+htype+sep) 
    } else if (dtypes.size == 1) {
      val dtype = dtypes.toList(0)
      if (dtype == DataType.NULL) {
        val htype = typeMappings(dtype)
        pw.println(indent+"  "+norm(fname,indent)+htype+sep+" -- WARN: NULL")
      } else {
        val htype = typeMappings(dtype)
        pw.println(indent+"  "+norm(fname,indent)+htype+sep) // AMM: TODO
      }
    } else {
      val htype = fmtUnion(dtypes.toSet)
      pw.println(indent+"  "+norm(fname,indent)+htype+sep)
    }
  }

  def buildArray(fname: String, fdef: FieldDef, sep: String, indent: String) {
    val adef = fdef.asInstanceOf[ArrayDef]

    val dtypes = adef.elementDataTypes
    dtypes.size match {
      case 0 => { 
        pw.println(indent+"  -- "+fname+" "+  " array <>"+sep) // EMTPY ARRAY
      } 
      case 1 => {
        val dtype = dtypes.toList(0)
        if (Schema.isCompound(dtype)) {
          pw.println(indent+"  "+norm(fname,indent)+  " array <" )
          buildObjectDef(adef, indent+TAB)
          pw.println(indent+"  >"+sep)
        } else {
          pw.println(indent+"  "+norm(fname,indent)+  " array<") // AMM: TODO
          buildObjectDef(adef, indent)
          pw.println(indent+"  >"+sep)
        }
      }
      case _ => {
        val htype = fmtUnion(dtypes.toSet)
        pw.println(indent+"  "+norm(fname,indent)+ "array <"+htype+">"+sep)
      }
    }
  }

  def fmtUnion(dataTypes: Set[DataType]) : String = {
    "uniontype<"+ (dataTypes.map(x=>typeMappings(x)).foldLeft("") ((b,a) => b+","+a)+"").drop(1)+ ">"
  }

  def norm(name: String, indent: String) : String = {
    if (name.length==0) return " "
    normalize(name)+mksep(indent)
  }
  def mksep(indent: String) = if (indent=="") " " else ": "

  def normalize(name: String) : String = {
    val n = name.toLowerCase();
    if (escapeReservedKeywords && (reservedKeywords.contains(n) || name.startsWith("_") || isNumeric(n))) "`"+name+"`" else name
  }

  def isNumeric(s: String) : Boolean = {
    s.matches("[+-]?\\d*(\\.\\d+)?") 
  }
}
