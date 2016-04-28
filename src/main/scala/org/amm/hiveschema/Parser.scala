package org.amm.hiveschema

trait Parser {
  def parse(content: String) : java.util.Map[String, Object]
}
