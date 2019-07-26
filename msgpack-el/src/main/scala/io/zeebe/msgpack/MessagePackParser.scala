package io.zeebe.msgpack

import fastparse.NoWhitespace._
import fastparse._

object MessagePackParser {

  def parseQuery(expression: String): MessagePackQuery = {
    parse(expression, queryExpression(_)) match {
      case Parsed.Success(query, index) => query
      case f: Parsed.Failure => {
        val message = f.trace().aggregateMsg
        throw new RuntimeException(s"Failed to parse query: $message")
      }
    }
  }

  private def queryExpression[_: P]: P[MessagePackQuery] = P(query ~ End)

  private def query[_: P]: P[MessagePackQuery] = P(name.! ~ ("." ~ name.!).rep).map {
    case (varName, path) => MessagePackQuery(
      expression = (varName +: path).mkString("."),
      variableName = varName,
      path = path
    )
  }

  private def name[_: P]: P[String] = P((nameStart ~ namePart.rep).!)

  private def nameStart[_: P]: P[String] = P(CharIn("a-zA-Z").!)

  private def namePart[_: P]: P[String] = P(CharIn("a-zA-Z0-9_").!)

}
