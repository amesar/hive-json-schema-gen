package org.amm.hiveschema

import scala.collection.JavaConverters._

trait Parser {
  def parse(map: java.util.Map[String, Object], schema: Schema) 

/*
  def walk(map: java.util.Map[String, Object]) : Schema = {
    val schema = new Schema() 
    walkMap("", map, schema.root)
    schema 
  }
*/

}
