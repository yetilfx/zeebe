package io.zeebe.msgpack

import io.zeebe.msgpack.spec.{MsgPackReader, MsgPackType}
import io.zeebe.util.buffer.BufferUtil
import org.agrona.DirectBuffer
import org.agrona.concurrent.UnsafeBuffer

case class MessagePackQuery(val expression: String,
                            variableName: String,
                            path: Iterable[String]) {

  private lazy val reader = new MsgPackReader
  private lazy val resultBuffer = new UnsafeBuffer(0, 0)

  val variableNameAsBuffer: DirectBuffer = BufferUtil.wrapString(variableName)
  val pathAsArray: Array[DirectBuffer] = path.map(BufferUtil.wrapString).toArray

  def apply(document: DirectBuffer): DirectBuffer = apply(document, 0, document.capacity())

  def apply(document: DirectBuffer, offset: Int, length: Int): DirectBuffer = {

    if (path.isEmpty) {
      resultBuffer.wrap(document, 0, length)
      return resultBuffer;
    }

    reader.wrap(document, offset, length)

    search(0)
  }

  private def search(pathIndex: Int): DirectBuffer = {

    if (!reader.hasNext) {
      resultBuffer.wrap(0, 0)
      return resultBuffer;
    }

    val token = reader.readToken()

    if (token.getType != MsgPackType.MAP) {
      throw new RuntimeException(s"Failed to apply query '${path.mkString(".")}': Expected MAP but found '${token.getType}'.")
    }

    val size = token.getSize

    for (_ <- 0 until size) {

      val key = reader.readToken().getValueBuffer

      if (key.equals(pathAsArray(pathIndex))) {

        if (pathIndex == (path.size - 1)) {

          val offset = reader.getOffset
          reader.skipValue()
          val length = reader.getOffset - offset

          resultBuffer.wrap(reader.getBuffer, offset, length)

          return BufferUtil.cloneBuffer(resultBuffer)

        } else {
          return search(pathIndex + 1)
        }

      } else {
        reader.skipValue()
      }
    }

    // throw new RuntimeException(s"Failed to apply query '${path.mkString(".")}': '${path.toList(pathIndex)}' not found.")

    resultBuffer.wrap(0, 0)
    return resultBuffer;

  }

}
