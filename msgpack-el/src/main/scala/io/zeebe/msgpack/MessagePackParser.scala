package io.zeebe.msgpack

import fastparse.NoWhitespace._
import fastparse._
import io.zeebe.msgpack.spec.{MsgPackToken, MsgPackType}
import io.zeebe.util.buffer.BufferUtil

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

  private def name[_: P]: P[String] = P((nameStart ~ namePart.rep)).!

  private def nameStart[_: P]: P[String] = P(CharIn("a-zA-Z")).!

  private def namePart[_: P]: P[String] = P(CharIn("a-zA-Z0-9_")).!

  private def condition[_: P] = ???

  private def comparison[_: P] = P((literal | query) ~ ("==" | "!=" | "<" | "<=" | ">" | ">=").! ~ (literal | query)).map {
    case (x: MsgPackToken, "==", y: MessagePackQuery) => ???
  }

  private def literal[_: P]: P[MsgPackToken] = P(nullLiteral | booleanLiteral | stringLiteral | numberLiteral)

  private def nullLiteral[_: P]: P[MsgPackToken] = P("null").map(_ => MsgPackToken.NIL)

  private def booleanLiteral[_: P]: P[MsgPackToken] = P("true" | "false").!.map { b =>
    val token = new MsgPackToken
    token.setType(MsgPackType.BOOLEAN)
    token.setValue(b.toBoolean)
    token
  }

  private def stringLiteral[_: P]: P[MsgPackToken] = P(
    ("\"" ~ CharsWhile(_ != '\"').! ~ "\"")
      | ("\'" ~ CharsWhile(_ != '\'').! ~ "\'")).map { s =>

    val buffer = BufferUtil.wrapString(s)

    val token = new MsgPackToken()
    token.setType(MsgPackType.STRING)
    token.setValue(buffer, 0, buffer.capacity())
    token
  }

  private def numberLiteral[_: P]: P[MsgPackToken] = P(CharIn("+\\-").? ~ integral ~ fractional.?).!.map { n =>
    val x = n.toDouble

    val token = new MsgPackToken();
    if (x.isWhole) {
      token.setType(MsgPackType.INTEGER)
      token.setValue(x.toLong)
    } else {
      token.setType(MsgPackType.FLOAT)
      token.setValue(x)
    }
    token
  }

  private def fractional[_: P] = P("." ~ digits)

  private def integral[_: P] = P("0" | CharIn("1-9") ~ digits.?)

  private def digits[_: P] = P(CharsWhileIn("0-9"))


}
