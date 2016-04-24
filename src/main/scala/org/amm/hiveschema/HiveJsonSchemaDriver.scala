package org.amm.hiveschema

import java.io._
import java.util.zip.GZIPInputStream
import scala.io.Source
import scala.collection.JavaConverters._
import org.amm.util.JacksonUtils
import com.beust.jcommander.{JCommander, Parameter}

object HiveJsonSchemaDriver {

  def main(args: Array[String]) {
    new JCommander(opts, args.toArray: _*)
    println("Options:")
    println("  table: "+opts.table)
    println("  location: "+opts.location)
    println("  serde: "+opts.serde)
    println("  outputFile: "+opts.outputFile)
    println("  isExternalTable: "+opts.outputFile)
    println("  escapeReservedKeywords: "+opts.escapeReservedKeywords)
    println("  reservedKeywordsFile: "+opts.reservedKeywordsFile)
    println("  files: "+opts.files.asScala.mkString(","))
    process()
  }

  def process() {
    val schema = new Schema()
    val walker = new Walker()

    var totalLines = 0
    for (file <- opts.files.asScala) {
      println("Processing file "+file)
      val bistream = new BufferedInputStream(new FileInputStream(file))
      val istream = if (file.endsWith(".gz")) new GZIPInputStream(bistream) else bistream
      for ((line,j) <- Source.fromInputStream(istream).getLines().zipWithIndex) {
        if (j> 0 && j%opts.logmod==0) println("  Processing line "+j+" of "+file);
        walker.walk(JacksonUtils.toMap(line),schema)
        totalLines += 1
      }
    }
    println("Lines: "+totalLines+" Files: "+opts.files.size)

    val buildInfo = if (opts.generateBuildInfo) "Generated on "+new java.util.Date()+" by "+getClass.getName().replace("$","") else null

    val hgen = new HiveSchemaGenerator(opts.table, opts.location, opts.serde, opts.outputFile, opts.isExternalTable, opts.escapeReservedKeywords, buildInfo, readReservedKeywordsFile())
    hgen.buildHiveSchema(schema)
    println("Hive DDL file: "+hgen.outputFilename)
  }

  def readReservedKeywordsFile() : Set[String] = {
    if (!(new File(opts.reservedKeywordsFile)).exists()) {
      println("WARNING: Reserved keywords file does not exist: "+opts.reservedKeywordsFile)
      return Set[String]()
    }
    Source.fromFile(opts.reservedKeywordsFile).getLines().map(x => x.toLowerCase()).toSet
  }

  object opts {
    @Parameter(names = Array( "-t", "--table" ), description = "Table", required=true )
    var table: String = null

    @Parameter(names = Array( "-l", "--location" ), description = "HDFS location", required=false )
    var location: String = null

    @Parameter(names = Array( "-s", "--serde" ), description = "SerDe", required=false )
    var serde: String = null

    @Parameter(names = Array( "-o", "--output" ), description = "Output schema file", required=false )
    var outputFile: String = null

    @Parameter(names = Array(  "--isExternalTable" ), description = "Is external table?", required=false )
    var isExternalTable: Boolean = false

    @Parameter(names = Array(  "--escapeReservedKeywords" ), description = "Escape Reserved Keywords?", required=false )
    var escapeReservedKeywords: Boolean = false

    @Parameter(names = Array( "--reservedKeywordsFile" ), description = "Reserved Keywords File", required=false )
    var reservedKeywordsFile: String = "src/main/resources/reservedKeywords.txt"

    @Parameter(names = Array(  "--generateBuildInfo" ), description = "Generate Build Info?", required=false )
    var generateBuildInfo: Boolean = false

    @Parameter(names = Array(  "--logmod" ), description = "Print processing line every N rows", required=false )
    var logmod = 10000

    @Parameter(description = "JSON files", required=true )
    var files: java.util.ArrayList[String] = null
  }
}
