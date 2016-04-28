package org.amm.hiveschema

import org.amm.util.JacksonUtils

class JsonParser extends Parser {
  def parse(content: String) : java.util.Map[String, Object] = JacksonUtils.toMap(content)
}
