package io.zeebe.msgpack;

import static io.zeebe.util.buffer.BufferUtil.wrapString;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.zeebe.test.util.MsgPackUtil;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Test;

public class MessagePackTest {

  @Test
  public void shouldParseQueryWithoutPath() {
    final MessagePackQuery query = MessagePackParser.parseQuery("x");

    assertThat(query.expression()).isEqualTo("x");
    assertThat(query.variableNameAsBuffer()).isEqualTo(wrapString("x"));
    assertThat(query.pathAsArray()).isEmpty();
  }

  @Test
  public void shouldParseQueryWithPath() {
    final MessagePackQuery query = MessagePackParser.parseQuery("x.y.z");

    assertThat(query.expression()).isEqualTo("x.y.z");
    assertThat(query.variableNameAsBuffer()).isEqualTo(wrapString("x"));
    assertThat(query.pathAsArray()).containsExactly(wrapString("y"), wrapString("z"));
  }

  @Test
  public void shouldNotParseEmptyQuery() {
    assertThatThrownBy(() -> MessagePackParser.parseQuery(""))
        .isInstanceOf(RuntimeException.class)
        .hasMessage("Failed to parse query: Expected [a-zA-Z]:1:1, found \"\"");
  }

  @Test
  public void shouldNotParseInvalidQuery() {
    assertThatThrownBy(() -> MessagePackParser.parseQuery("123"))
        .isInstanceOf(RuntimeException.class)
        .hasMessage("Failed to parse query: Expected [a-zA-Z]:1:1, found \"123\"");
  }

  @Test
  public void shouldApplyQuery() {
    final DirectBuffer document =
        MsgPackUtil.asMsgPack(b -> b.put("a", "foo").put("b", "bar").put("c", "baz"));

    assertThat(MessagePackParser.parseQuery("x.a").apply(document))
        .isEqualTo(MsgPackUtil.encodeMsgPack(b -> b.packString("foo")));
    assertThat(MessagePackParser.parseQuery("x.b").apply(document))
        .isEqualTo(MsgPackUtil.encodeMsgPack(b -> b.packString("bar")));
    assertThat(MessagePackParser.parseQuery("x.c").apply(document))
        .isEqualTo(MsgPackUtil.encodeMsgPack(b -> b.packString("baz")));
  }

  @Test
  public void shouldApplyNestedQuery() {
    final MessagePackQuery query = MessagePackParser.parseQuery("x.y.z");

    final DirectBuffer result =
        query.apply(
            MsgPackUtil.encodeMsgPack(
                b ->
                    b.packMapHeader(1)
                        .packString("y")
                        .packMapHeader(1)
                        .packString("z")
                        .packString("foo")));

    assertThat(result).isEqualTo(MsgPackUtil.encodeMsgPack(b -> b.packString("foo")));
  }

  @Test
  public void shouldApplyQueryNotExistingPath() {
    final UnsafeBuffer emptyDocument = new UnsafeBuffer(0, 0);

    assertThat(MessagePackParser.parseQuery("x.y").apply(emptyDocument).capacity()).isEqualTo(0);
  }

  @Test
  public void shouldApplyQueryEmptyPath() {
    final DirectBuffer document = MsgPackUtil.encodeMsgPack(p -> p.packString("foo"));

    assertThat(MessagePackParser.parseQuery("x").apply(document)).isEqualTo(document);
  }
}
